"""Natural-language time windows → Snowflake date filters.

One place for every phrasing the tools understand, so "last quarter", "May 2026", "past 2
weeks" and "this month" mean the same thing everywhere. Calendar periods use Snowflake's
DATE_TRUNC so they are evaluated at query time; explicit months/quarters/years are computed
here from today's date (month names without a year mean the current year).
"""
import re
from dataclasses import dataclass
from datetime import date

_MONTHS = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3, "apr": 4, "april": 4,
    "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7, "aug": 8, "august": 8, "sep": 9, "sept": 9,
    "september": 9, "oct": 10, "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12,
}
_MONTH_NAMES = "|".join(sorted(_MONTHS, key=len, reverse=True))
# "may" is also a verb; count it as a month only in date context.
_MONTH_RE = re.compile(
    rf"(?<![\w])(?:(?P<m1>{_MONTH_NAMES})(?!\w))(?:\s+(?P<y1>20\d\d))?", re.IGNORECASE
)
_MAY_CONTEXT_RE = re.compile(
    r"\b(?:in|for|of|during|since|until|from|through|to|and|till|between)\s+may\b|\bmay\s+(?:20\d\d|and|to|through)\b",
    re.IGNORECASE,
)
_UNIT_DAYS = {"day": 1, "days": 1, "week": 7, "weeks": 7}
_LABEL_MONTH = ["", "January", "February", "March", "April", "May", "June", "July", "August",
                "September", "October", "November", "December"]


@dataclass(frozen=True)
class Period:
    sql: str    # "AND <col> ... " fragment, safe to interpolate (no user text inside)
    label: str  # human-readable window, echoed back in the answer


def _between(col: str, start: date, end_exclusive: date) -> str:
    return f"AND {col} >= '{start.isoformat()}' AND {col} < '{end_exclusive.isoformat()}'"


def _month_start(year: int, month: int) -> date:
    return date(year, month, 1)


def _next_month(year: int, month: int) -> date:
    return date(year + (month == 12), (month % 12) + 1, 1)


def _month_mentions(q: str, today: date):
    """Return [(year, month), ...] for every month name used in date context."""
    out = []
    for m in _MONTH_RE.finditer(q):
        name = m.group("m1").lower()
        if name == "may" and not _MAY_CONTEXT_RE.search(q):
            continue
        year = int(m.group("y1")) if m.group("y1") else None
        out.append((year, _MONTHS[name]))
    if not out:
        return []
    default_year = next((y for y, _ in out if y), None) or today.year
    return [(y or default_year, mo) for y, mo in out]


def parse_period(question: str, col: str = "o.order_placed_at", today: date | None = None) -> Period:
    q = question.lower()
    today = today or date.today()

    def word(*ws):
        return any(re.search(rf"\b{re.escape(w)}\b", q) for w in ws)

    if word("today"):
        return Period(f"AND {col}::DATE = CURRENT_DATE()", "today")
    if word("yesterday"):
        return Period(f"AND {col}::DATE = CURRENT_DATE() - 1", "yesterday")

    m = re.search(r"\b(?:last|past|previous)\s+(\d{1,3})\s+(days?|weeks?|months?|years?)\b", q)
    if m:
        n, unit = int(m.group(1)), m.group(2)
        if unit in _UNIT_DAYS:
            days = n * _UNIT_DAYS[unit]
            return Period(f"AND {col}::DATE >= DATEADD(day, -{days}, CURRENT_DATE())", f"last {n} {unit}")
        part = "month" if unit.startswith("month") else "year"
        return Period(f"AND {col}::DATE >= DATEADD({part}, -{n}, CURRENT_DATE())", f"last {n} {unit}")

    # Quarters: "q3", "q3 2026", "quarter 3", "this/last/previous quarter"
    if re.search(r"\b(?:last|previous)\s+quarter\b", q):
        return Period(
            f"AND {col} >= DATE_TRUNC(quarter, DATEADD(quarter, -1, CURRENT_DATE())) "
            f"AND {col} < DATE_TRUNC(quarter, CURRENT_DATE())",
            "last quarter",
        )
    if re.search(r"\bthis\s+quarter\b", q):
        return Period(f"AND {col} >= DATE_TRUNC(quarter, CURRENT_DATE())", "this quarter to date")
    m = re.search(r"\b(?:q([1-4])|quarter\s*([1-4]))(?:\s+(?:of\s+)?(20\d\d))?\b", q)
    if m:
        qn = int(m.group(1) or m.group(2))
        year = int(m.group(3)) if m.group(3) else today.year
        start = _month_start(year, 3 * qn - 2)
        end = _next_month(year, 3 * qn)
        return Period(_between(col, start, end), f"Q{qn} {year}")

    # Years
    if re.search(r"\b(?:last|previous)\s+year\b", q):
        return Period(
            f"AND {col} >= DATE_TRUNC(year, DATEADD(year, -1, CURRENT_DATE())) "
            f"AND {col} < DATE_TRUNC(year, CURRENT_DATE())",
            "last year",
        )
    if word("this year", "year to date", "ytd", "annual", "yearly", "year"):
        return Period(f"AND {col} >= DATE_TRUNC(year, CURRENT_DATE())", "year to date")

    # Named months (one or a span: "april and may", "jan to mar 2026")
    months = _month_mentions(q, today)
    if months:
        (y0, m0), (y1, m1) = min(months), max(months)
        label = _LABEL_MONTH[m0] + (f"–{_LABEL_MONTH[m1]}" if (y0, m0) != (y1, m1) else "") + f" {y1}"
        return Period(_between(col, _month_start(y0, m0), _next_month(y1, m1)), label)

    # Weeks / months (calendar for "this", rolling otherwise)
    if re.search(r"\bthis\s+week\b", q):
        return Period(f"AND {col} >= DATE_TRUNC(week, CURRENT_DATE())", "this week to date")
    if word("week", "weekly", "last week", "past week"):
        return Period(f"AND {col}::DATE >= DATEADD(day, -7, CURRENT_DATE())", "last 7 days")
    if re.search(r"\bthis\s+month\b", q):
        return Period(f"AND {col} >= DATE_TRUNC(month, CURRENT_DATE())", "this month to date")
    if re.search(r"\b(?:last|previous)\s+month\b", q):
        return Period(
            f"AND {col} >= DATE_TRUNC(month, DATEADD(month, -1, CURRENT_DATE())) "
            f"AND {col} < DATE_TRUNC(month, CURRENT_DATE())",
            "last month",
        )
    if word("month", "monthly"):
        return Period(f"AND {col}::DATE >= DATEADD(day, -30, CURRENT_DATE())", "last 30 days")

    return Period(f"AND {col}::DATE >= DATEADD(day, -30, CURRENT_DATE())", "last 30 days")
