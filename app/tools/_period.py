"""Natural-language time windows → Snowflake date filters.

One place for every phrasing the tools understand, so "last quarter", "May 2026", "past 2
weeks" and "this month" mean the same thing everywhere. Calendar periods use Snowflake's
DATE_TRUNC so they are evaluated at query time in the session timezone (the client sets it
to Asia/Kolkata); explicit months/quarters/years are computed here from today's date. Month
names without a year mean the current year, except that a span such as "nov to feb" wraps
into the previous year.

The SQL fragment only ever contains regex-captured integers and ISO dates computed here;
no user text is interpolated.
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
_MONTH_RE = re.compile(rf"(?<![\w])(?P<m>{_MONTH_NAMES})(?!\w)(?:\s+(?:(?P<y>20\d\d)|'(?P<y2>\d\d)))?", re.IGNORECASE)
# "may" is also a verb; count it as a month only in date context.
_MAY_CONTEXT_RE = re.compile(
    r"\b(?:in|for|of|during|since|until|from|through|to|and|till|between)\s+may\b|\bmay\s+(?:20\d\d|and|to|through)\b",
    re.IGNORECASE,
)
_UNIT_DAYS = {"day": 1, "days": 1, "week": 7, "weeks": 7}
_LABEL_MONTH = ["", "January", "February", "March", "April", "May", "June", "July", "August",
                "September", "October", "November", "December"]
DEFAULT_LABEL = "the last 30 days (default window)"


@dataclass(frozen=True)
class Period:
    sql: str          # "AND <col> ..." fragment, safe to interpolate
    label: str        # human-readable window, echoed back in the answer
    recognised: bool = True  # False when the question named no window we understand


def _between(col, start: date, end_exclusive: date | None) -> str:
    sql = f"AND {col} >= '{start.isoformat()}'"
    if end_exclusive is not None:
        sql += f" AND {col} < '{end_exclusive.isoformat()}'"
    return sql


def _next_month(year: int, month: int) -> date:
    return date(year + (month == 12), (month % 12) + 1, 1)


def _month_label(y0, m0, y1, m1) -> str:
    if (y0, m0) == (y1, m1):
        return f"{_LABEL_MONTH[m0]} {y0}"
    if y0 == y1:
        return f"{_LABEL_MONTH[m0]}–{_LABEL_MONTH[m1]} {y1}"
    return f"{_LABEL_MONTH[m0]} {y0}–{_LABEL_MONTH[m1]} {y1}"


def _named_months(q: str, today: date) -> Period | None:
    mentions = []
    for m in _MONTH_RE.finditer(q):
        name = m.group("m").lower()
        if name == "may" and not _MAY_CONTEXT_RE.search(q):
            continue
        year = int(m.group("y")) if m.group("y") else (2000 + int(m.group("y2")) if m.group("y2") else None)
        mentions.append((year, _MONTHS[name], m.start()))
    if not mentions:
        return None
    # Year qualifier: an explicit year, or "this/last year" applied to the month.
    explicit = next((y for y, _, _ in mentions if y), None)
    if explicit is None and re.search(r"\b(?:last|previous)\s+year\b", q):
        explicit = today.year - 1
    default_year = explicit or today.year
    first = mentions[0]
    last = mentions[-1]
    y0, m0 = first[0] or default_year, first[1]
    y1, m1 = last[0] or default_year, last[1]
    if (y0, m0) > (y1, m1):
        if first[0] is None and last[0] is None:
            y0 -= 1  # "nov to feb" wraps into the previous year
        elif first[0] is not None and last[0] is None:
            y1 = y0 + 1  # "nov 2025 to feb" rolls forward
        elif first[0] is None and last[0] is not None:
            y0 = y1 - 1  # "nov to feb 2026" rolls back
        else:
            (y0, m0), (y1, m1) = (y1, m1), (y0, m0)
    col_placeholder = "{col}"
    start = date(y0, m0, 1)
    if re.search(rf"\bsince\s+(?:{_MONTH_NAMES})\b", q[: first[2] + 12]):
        return Period(_between(col_placeholder, start, None), f"since {_LABEL_MONTH[m0]} {y0}")
    return Period(_between(col_placeholder, start, _next_month(y1, m1)), _month_label(y0, m0, y1, m1))


def parse_period(question: str, col: str = "o.order_placed_at", today: date | None = None) -> Period:
    q = question.lower()
    today = today or date.today()

    def word(*ws):
        return any(re.search(rf"\b{re.escape(w)}\b", q) for w in ws)

    def done(sql, label, recognised=True):
        return Period(sql.replace("{col}", col), label, recognised)

    if word("today"):
        return done("AND {col}::DATE = CURRENT_DATE()", "today")
    if word("yesterday"):
        return done("AND {col}::DATE = CURRENT_DATE() - 1", "yesterday")

    _WORDS = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 8,
              "nine": 9, "ten": 10, "twelve": 12}
    if re.search(r"\b(?:last|past)\s+fortnight\b", q):
        return done("AND {col}::DATE >= DATEADD(day, -14, CURRENT_DATE())", "the last 14 days")
    m = re.search(r"\b(?:last|past|previous)\s+(\d{1,3}|one|two|three|four|five|six|seven|eight|nine|ten|twelve)\s+(days?|weeks?|months?|quarters?|years?)\b", q)
    if m:
        n, unit = (int(m.group(1)) if m.group(1).isdigit() else _WORDS[m.group(1)]), m.group(2)
        if unit in _UNIT_DAYS:
            return done(f"AND {{col}}::DATE >= DATEADD(day, -{n * _UNIT_DAYS[unit]}, CURRENT_DATE())", f"the last {n} {unit}")
        part = "month" if unit.startswith("month") else "quarter" if unit.startswith("quarter") else "year"
        return done(f"AND {{col}}::DATE >= DATEADD({part}, -{n}, CURRENT_DATE())", f"the last {n} {unit}")

    if re.search(r"\b(?:past\s+year|last\s+twelve\s+months|past\s+twelve\s+months)\b", q):
        # Rolling 12 months; the calendar "last year" is handled further down.
        return done("AND {col}::DATE >= DATEADD(year, -1, CURRENT_DATE())", "the last 12 months")

    # Half years: "H1", "h2 2025"
    m = re.search(r"\bh([12])(?:\s+(?:of\s+)?(20\d\d))?\b", q)
    if m:
        half, year = int(m.group(1)), int(m.group(2)) if m.group(2) else today.year
        start = date(year, 1 if half == 1 else 7, 1)
        end = date(year, 7, 1) if half == 1 else date(year + 1, 1, 1)
        return done(_between("{col}", start, end), f"H{half} {year}")

    # Quarters
    if re.search(r"\b(?:last|previous)\s+quarter\b", q):
        return done("AND {col} >= DATE_TRUNC(quarter, DATEADD(quarter, -1, CURRENT_DATE())) "
                    "AND {col} < DATE_TRUNC(quarter, CURRENT_DATE())", "last quarter")
    if re.search(r"\bthis\s+quarter\b", q):
        return done("AND {col} >= DATE_TRUNC(quarter, CURRENT_DATE())", "this quarter to date")
    m = re.search(r"\b(?:(20\d\d)\s+)?(?:q([1-4])|quarter\s*([1-4])|(first|second|third|fourth|1st|2nd|3rd|4th)\s+quarter)(?:\s+(?:of\s+)?(20\d\d))?\b", q)
    if m:
        ordinal = {"first": 1, "1st": 1, "second": 2, "2nd": 2, "third": 3, "3rd": 3, "fourth": 4, "4th": 4}
        qn = int(m.group(2) or m.group(3) or 0) or ordinal[m.group(4)]
        year = int(m.group(1) or m.group(5) or 0)
        if not year:
            year = today.year - 1 if re.search(r"\b(?:last|previous)\s+year\b", q) else today.year
        return done(_between("{col}", date(year, 3 * qn - 2, 1), _next_month(year, 3 * qn)), f"Q{qn} {year}")

    # Named months (checked before year phrases so "march last year" is March, not the whole year)
    named = _named_months(q, today)
    if named:
        return done(named.sql, named.label)

    # Indian financial year: "FY26", "FY 2025-26", "fy2025" → April 2025 – March 2026
    m = re.search(r"\bfy\s*(?:(20)?(\d\d))(?:\s*[-/]\s*(?:20)?(\d\d))?\b", q)
    if m:
        start_year = 2000 + int(m.group(2)) - (0 if m.group(3) else 1)  # "FY26" = 2025-26
        return done(_between("{col}", date(start_year, 4, 1), date(start_year + 1, 4, 1)), f"FY {start_year}-{str(start_year + 1)[2:]}")

    # Year span: "between 2025 and 2026"
    m = re.search(r"\b(20\d\d)\s*(?:-|to|and|through|until)\s*(20\d\d)\b", q)
    if m:
        y0, y1 = sorted((int(m.group(1)), int(m.group(2))))
        return done(_between("{col}", date(y0, 1, 1), date(y1 + 1, 1, 1)), f"{y0}–{y1}")

    # Bare calendar year: "revenue for 2025"
    m = re.search(r"\b(?:in|for|of|during|year|since|from|until|till|to)\s+(20\d\d)\b|(?<![#\w])(?<!ticket )(?<!order )(?<!orders )(?<!sku )(?<!id )(?<!no\. )(?<!number )(20\d\d)\s*$", q)
    if m:
        year = int(m.group(1) or m.group(2))
        return done(_between("{col}", date(year, 1, 1), date(year + 1, 1, 1)), str(year))

    # Years
    if re.search(r"\b(?:last|previous)\s+year\b", q):
        return done("AND {col} >= DATE_TRUNC(year, DATEADD(year, -1, CURRENT_DATE())) "
                    "AND {col} < DATE_TRUNC(year, CURRENT_DATE())", "last year")
    if word("this year", "year to date", "ytd", "annual", "yearly", "year"):
        return done("AND {col} >= DATE_TRUNC(year, CURRENT_DATE())", "year to date")

    # Weeks / months (calendar for "this", rolling otherwise)
    if re.search(r"\bthis\s+week\b", q):
        return done("AND {col} >= DATE_TRUNC(week, CURRENT_DATE())", "this week to date")
    if word("week", "weekly"):
        return done("AND {col}::DATE >= DATEADD(day, -7, CURRENT_DATE())", "the last 7 days")
    if re.search(r"\bthis\s+month\b", q):
        return done("AND {col} >= DATE_TRUNC(month, CURRENT_DATE())", "this month to date")
    if re.search(r"\b(?:last|previous)\s+month\b", q):
        return done("AND {col} >= DATE_TRUNC(month, DATEADD(month, -1, CURRENT_DATE())) "
                    "AND {col} < DATE_TRUNC(month, CURRENT_DATE())", "last month")
    if word("month", "monthly"):
        return done("AND {col}::DATE >= DATEADD(day, -30, CURRENT_DATE())", "the last 30 days")

    return done("AND {col}::DATE >= DATEADD(day, -30, CURRENT_DATE())", DEFAULT_LABEL, recognised=False)


def combined_window(question: str, col: str = "o.order_placed_at", today: date | None = None):
    """For "A vs B" questions, the window spanning both named periods (earliest start to latest end).

    Returns (sql, label) or (None, None) when fewer than two periods are named. Only explicit
    calendar periods (quarters, months, years) and this/last quarter are combined.
    """
    q = question.lower()
    parts = re.split(r"\b(?:vs\.?|versus|compared (?:to|with)|against|and)\b", q)
    if len(parts) < 2:
        return None, None
    windows = [parse_period(p, col=col, today=today) for p in parts]
    windows = [w for w in windows if w.recognised]
    if len(windows) < 2:
        return None, None
    starts, ends, labels = [], [], []
    for w in windows:
        m = re.search(r">= '(\d{4}-\d{2}-\d{2})'(?: AND \S+ < '(\d{4}-\d{2}-\d{2})')?", w.sql)
        if m:
            starts.append(m.group(1)); ends.append(m.group(2))
        elif "quarter" in w.label:
            # this/last quarter: use Snowflake expressions; fall back to spanning the two quarters
            starts.append(None); ends.append(None)
        labels.append(w.label)
    if any(s is None for s in starts):
        return (f"AND {col} >= DATE_TRUNC(quarter, DATEADD(quarter, -1, CURRENT_DATE()))",
                " vs ".join(labels))
    start = min(starts)
    end = max(e for e in ends if e) if any(ends) else None
    sql = f"AND {col} >= '{start}'" + (f" AND {col} < '{end}'" if end else "")
    return sql, " vs ".join(labels)
