"""Natural-language time windows → concrete date ranges → Snowflake filters.

Every phrasing the tools understand resolves to a half-open [start, end) date range computed
in Python from "today" (the IST calendar date, matching the seeded data), so windows can be
compared and combined without SQL expressions. The SQL fragment then contains only ISO dates
produced here; no user text is ever interpolated.

Unrecognised phrasings fall back to the last 30 days with ``recognised=False`` and an explicit
label, so the answer never presents a guessed window as the asked one.
"""
import re
from dataclasses import dataclass
from datetime import date, timedelta
from zoneinfo import ZoneInfo

from app.config import settings

_MONTHS = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3, "apr": 4, "april": 4,
    "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7, "aug": 8, "august": 8, "sep": 9, "sept": 9,
    "september": 9, "oct": 10, "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12,
}
_MONTH_NAMES = "|".join(sorted(_MONTHS, key=len, reverse=True))
_MONTH_RE = re.compile(
    rf"(?:(?P<ybefore>20\d\d)\s+)?(?<![\w])(?P<m>{_MONTH_NAMES})(?!\w)(?:\s*(?:(?P<y>20\d\d)|'(?P<y2>\d\d)))?",
    re.IGNORECASE,
)
# "may" is also a verb; count it as a month only in date context.
_MAY_CONTEXT_RE = re.compile(
    r"\b(?:in|for|of|during|since|until|from|through|to|and|till|between|vs|versus)\s+may\b|\bmay\s+(?:20\d\d|'\d\d|and|to|through|vs)\b",
    re.IGNORECASE,
)
_WORDS = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 8,
          "nine": 9, "ten": 10, "twelve": 12}
_ORDINAL = {"first": 1, "1st": 1, "second": 2, "2nd": 2, "third": 3, "3rd": 3, "fourth": 4, "4th": 4}
_LABEL_MONTH = ["", "January", "February", "March", "April", "May", "June", "July", "August",
                "September", "October", "November", "December"]
DEFAULT_LABEL = "the last 30 days (default window)"


def today_local() -> date:
    """Calendar date in the data's timezone (the Snowflake session timezone)."""
    from datetime import datetime
    return datetime.now(ZoneInfo(settings.SNOWFLAKE_TIMEZONE)).date()


@dataclass(frozen=True)
class Period:
    start: date | None          # inclusive; None = open
    end: date | None            # exclusive; None = open ("up to now")
    label: str
    col: str = "o.order_placed_at"
    recognised: bool = True

    @property
    def sql(self) -> str:
        """``AND col >= 'YYYY-MM-DD' AND col < 'YYYY-MM-DD'`` with only computed ISO dates."""
        parts = []
        if self.start is not None:
            parts.append(f"{self.col} >= '{self.start.isoformat()}'")
        if self.end is not None:
            parts.append(f"{self.col} < '{self.end.isoformat()}'")
        return ("AND " + " AND ".join(parts)) if parts else ""


# ── date helpers ─────────────────────────────────────────────────────────────
def _add_months(d: date, n: int) -> date:
    m = d.month - 1 + n
    return date(d.year + m // 12, m % 12 + 1, 1)


def _month_start(y: int, m: int) -> date:
    return date(y, m, 1)


def _next_month(y: int, m: int) -> date:
    return _add_months(date(y, m, 1), 1)


def _quarter(y: int, qn: int) -> tuple[date, date]:
    return date(y, 3 * qn - 2, 1), _next_month(y, 3 * qn)


def _fy(start_year: int) -> tuple[date, date, str]:
    return date(start_year, 4, 1), date(start_year + 1, 4, 1), f"FY {start_year}-{str(start_year + 1)[2:]}"


def _month_label(y0, m0, y1, m1) -> str:
    if (y0, m0) == (y1, m1):
        return f"{_LABEL_MONTH[m0]} {y0}"
    if y0 == y1:
        return f"{_LABEL_MONTH[m0]}–{_LABEL_MONTH[m1]} {y1}"
    return f"{_LABEL_MONTH[m0]} {y0}–{_LABEL_MONTH[m1]} {y1}"


def _year_qualifier(q: str, today: date, default_year: int | None) -> int | None:
    """An explicit year to apply to months/quarters that carry none."""
    if re.search(r"\b(?:last|previous)\s+year\b", q):
        return today.year - 1
    return default_year


# ── named months ─────────────────────────────────────────────────────────────
def _named_months(q: str, today: date, default_year: int | None, may_is_month: bool):
    mentions = []
    for m in _MONTH_RE.finditer(q):
        name = m.group("m").lower()
        if name == "may" and not may_is_month:
            continue
        year = None
        if m.group("ybefore"):
            year = int(m.group("ybefore"))
        elif m.group("y"):
            year = int(m.group("y"))
        elif m.group("y2"):
            year = 2000 + int(m.group("y2"))
        mentions.append((year, _MONTHS[name], m.start()))
    if not mentions:
        return None
    explicit = next((y for y, _, _ in mentions if y), None)
    bare_year = re.search(r"\b(20\d\d)\b", q)
    fallback = explicit or (int(bare_year.group(1)) if bare_year else None) or _year_qualifier(q, today, default_year)
    assumed = fallback is None
    fallback = fallback or today.year
    first, last = mentions[0], mentions[-1]
    y0, m0 = first[0] or fallback, first[1]
    y1, m1 = last[0] or fallback, last[1]
    if assumed and first[0] is None and last[0] is None and m0 <= m1 and m0 > today.month:
        y0 -= 1  # a yearless month still ahead of us means the most recent one, i.e. last year
        y1 -= 1
    if (y0, m0) > (y1, m1):
        if first[0] is None and last[0] is None:
            y0 -= 1                      # "nov to feb" wraps into the previous year
        elif first[0] is not None and last[0] is None:
            y1 = y0 + 1                  # "nov 2025 to feb" rolls forward
        elif first[0] is None and last[0] is not None:
            y0 = y1 - 1                  # "nov to feb 2026" rolls back
        else:
            (y0, m0), (y1, m1) = (y1, m1), (y0, m0)
    start = _month_start(y0, m0)
    if len(mentions) == 1:
        if re.search(rf"\b(?:since|from)\s+(?:{_MONTH_NAMES})\b", q) and not re.search(r"\b(?:to|until|till|through|and)\b", q):
            return Period(start, None, f"since {_LABEL_MONTH[m0]} {y0}")
        if re.search(rf"\b(?:until|till|up to|through|to the end of)\s+(?:{_MONTH_NAMES})\b", q):
            return Period(None, _next_month(y0, m0), f"up to the end of {_LABEL_MONTH[m0]} {y0}")
        if re.search(rf"\bbefore\s+(?:{_MONTH_NAMES})\b", q):
            return Period(None, start, f"before {_LABEL_MONTH[m0]} {y0}")
    return Period(start, _next_month(y1, m1), _month_label(y0, m0, y1, m1))


# ── main parser ──────────────────────────────────────────────────────────────
def parse_period(question: str, col: str = "o.order_placed_at", today: date | None = None,
                 default_year: int | None = None, may_is_month: bool | None = None) -> Period:
    q = question.lower()
    today = today or today_local()
    if may_is_month is None:
        may_is_month = _MAY_CONTEXT_RE.search(q) is not None

    def word(*ws):
        return any(re.search(rf"\b{re.escape(w)}\b", q) for w in ws)

    def P(start, end, label, recognised=True):
        return Period(start, end, label, col, recognised)

    tomorrow = today + timedelta(days=1)

    if word("today"):
        return P(today, tomorrow, "today")
    if word("yesterday"):
        return P(today - timedelta(days=1), today, "yesterday")

    if re.search(r"\b(?:last|past)\s+fortnight\b", q):
        return P(today - timedelta(days=14), None, "the last 14 days")
    m = re.search(r"\b(?:last|past|previous)\s+(\d{1,3}|one|two|three|four|five|six|seven|eight|nine|ten|twelve)\s+(days?|weeks?|months?|quarters?|years?)\b", q)
    if m:
        n = int(m.group(1)) if m.group(1).isdigit() else _WORDS[m.group(1)]
        unit = m.group(2)
        label = f"the last {n} {unit}"
        if unit.startswith("day"):
            return P(today - timedelta(days=n), None, label)
        if unit.startswith("week"):
            return P(today - timedelta(days=7 * n), None, label)
        if unit.startswith("month"):
            return P(_shift_months(today, -n), None, label)
        if unit.startswith("quarter"):
            return P(_shift_months(today, -3 * n), None, label)
        return P(_shift_years(today, -n), None, label)
    if re.search(r"\b(?:past\s+year|(?:last|past)\s+twelve\s+months)\b", q):
        return P(_shift_years(today, -1), None, "the last 12 months")

    # Half years: "H1", "h2 2025", "first half 2025", "second half of last year"
    m = re.search(r"\b(?:h([12])|(first|second|1st|2nd)\s+half)(?:\s+(?:of\s+)?(?:the\s+)?(20\d\d|last year|this year))?\b", q)
    if m:
        half = int(m.group(1) or _ORDINAL[m.group(2)])
        yr = m.group(3)
        year = int(yr) if yr and yr.isdigit() else today.year - 1 if yr == "last year" else (_year_qualifier(q, today, default_year) or today.year)
        start, end = (date(year, 1, 1), date(year, 7, 1)) if half == 1 else (date(year, 7, 1), date(year + 1, 1, 1))
        return P(start, end, f"H{half} {year}")

    # Indian financial year: "FY26", "FY 2025-26", "fy2025", "2025-26"
    m = re.search(r"\bfy\s*(?:(?:20)?(\d\d))(?:\s*[-/]\s*(?:20)?(\d\d))?\b|\b(20\d\d)\s*[-/]\s*(\d\d)\b(?![-/]\d)", q)
    if m:
        if m.group(3):
            start_year, second = int(m.group(3)), int(m.group(4))
            m = m if (start_year + 1) % 100 == second else None  # "2026-09" is a date, not a FY
        else:
            start_year = 2000 + int(m.group(1)) - (0 if m.group(2) else 1)   # "FY26" = 2025-26
        if m:
            s, e, label = _fy(start_year)
            return P(s, e, label)

    # Quarters
    m = re.search(r"\blast\s+quarter\s+of\s+(?:(20\d\d)|(last|previous)\s+year)\b", q)
    if m:
        year = int(m.group(1)) if m.group(1) else today.year - 1
        s, e = _quarter(year, 4)
        return P(s, e, f"Q4 {year}")
    if re.search(r"\b(?:last|previous)\s+quarter\b", q):
        cur_q = (today.month - 1) // 3 + 1
        y, qn = (today.year, cur_q - 1) if cur_q > 1 else (today.year - 1, 4)
        s, e = _quarter(y, qn)
        return P(s, e, "last quarter")
    if re.search(r"\bthis\s+quarter\b", q):
        s, _ = _quarter(today.year, (today.month - 1) // 3 + 1)
        return P(s, tomorrow, "this quarter to date")
    m = re.search(r"\b(?:(20\d\d)\s+)?(?:q([1-4])|quarter\s*([1-4])|(first|second|third|fourth|1st|2nd|3rd|4th)\s+quarter)(?:\s+(?:of\s+)?(20\d\d))?\b", q)
    if m:
        qn = int(m.group(2) or m.group(3) or 0) or _ORDINAL[m.group(4)]
        year = int(m.group(1) or m.group(5) or 0) or _year_qualifier(q, today, default_year) or today.year
        s, e = _quarter(year, qn)
        return P(s, e, f"Q{qn} {year}")

    # Named months (before year phrases so "march last year" is March, not the whole year)
    named = _named_months(q, today, default_year, may_is_month)
    if named:
        return P(named.start, named.end, named.label)

    # Year spans and open-ended years
    m = re.search(r"\b(20\d\d)\s*(?:-|to|and|through|until)\s*(20\d\d)\b", q)
    if m:
        y0, y1 = sorted((int(m.group(1)), int(m.group(2))))
        return P(date(y0, 1, 1), date(y1 + 1, 1, 1), f"{y0}–{y1}")
    m = re.search(r"\b(?:since|from)\s+(20\d\d)\b", q)
    if m:
        return P(date(int(m.group(1)), 1, 1), None, f"since {m.group(1)}")
    m = re.search(r"\b(?:until|till|up to|through)\s+(20\d\d)\b", q)
    if m:
        return P(None, date(int(m.group(1)) + 1, 1, 1), f"up to the end of {m.group(1)}")
    m = re.search(r"\bbefore\s+(20\d\d)\b", q)
    if m:
        return P(None, date(int(m.group(1)), 1, 1), f"before {m.group(1)}")
    m = re.search(r"\b(?:in|for|of|during|year)\s+(20\d\d)\b|(?<![#\w])(?<!ticket )(?<!order )(?<!orders )(?<!sku )(?<!id )(?<!no\. )(?<!number )(20\d\d)\s*$", q)
    if m:
        y = int(m.group(1) or m.group(2))
        return P(date(y, 1, 1), date(y + 1, 1, 1), str(y))

    if re.search(r"\b(?:last|previous)\s+year\b", q):
        return P(date(today.year - 1, 1, 1), date(today.year, 1, 1), "last year")
    if word("this year", "year to date", "ytd", "annual", "yearly", "year"):
        return P(date(today.year, 1, 1), tomorrow, "year to date")

    # Weeks / months (calendar for "this", rolling otherwise)
    if re.search(r"\bthis\s+week\b", q):
        return P(today - timedelta(days=today.weekday()), tomorrow, "this week to date")
    if word("week", "weekly"):
        return P(today - timedelta(days=7), None, "the last 7 days")
    if re.search(r"\bthis\s+month\b", q):
        return P(today.replace(day=1), tomorrow, "this month to date")
    if re.search(r"\b(?:last|previous)\s+month\b", q):
        return P(_add_months(today, -1), today.replace(day=1), "last month")
    if word("month", "monthly"):
        return P(today - timedelta(days=30), None, "the last 30 days")

    return P(today - timedelta(days=30), None, DEFAULT_LABEL, recognised=False)


def _shift_months(d: date, n: int) -> date:
    """d moved by n months, clamped to the month length."""
    first = _add_months(d.replace(day=1), n)
    last_day = (_add_months(first, 1) - timedelta(days=1)).day
    return first.replace(day=min(d.day, last_day))


def _shift_years(d: date, n: int) -> date:
    return _shift_months(d, 12 * n)


# ── comparisons ──────────────────────────────────────────────────────────────
_VS_RE = re.compile(r"\b(?:vs\.?|versus|compared\s+(?:to|with)|against)\b")


def comparison_periods(question: str, col: str = "o.order_placed_at", today: date | None = None) -> list[Period]:
    """The periods named on each side of an "A vs B" question (empty unless at least two).

    A year (or "last year") written on one side also applies to a side that names none of
    its own ("q1 vs q2 2025" is Q1 2025 vs Q2 2025; "march vs april last year" is both 2025),
    unless that would make both sides the same window, in which case the yearless side keeps
    the current year ("q1 vs q1 last year" is Q1 2026 vs Q1 2025).
    """
    q = question.lower()
    today = today or today_local()
    parts = [p for p in _VS_RE.split(q) if p.strip()]
    if len(parts) < 2:
        return []
    ym = re.search(r"\b(20\d\d)\b", q)
    shared_year = int(ym.group(1)) if ym else (today.year - 1 if re.search(r"\b(?:last|previous)\s+year\b", q) else None)
    may_is_month = _MAY_CONTEXT_RE.search(q) is not None

    def own_year(part: str) -> bool:
        return re.search(r"\b20\d\d\b|'\d\d\b|\b(?:last|previous|this)\s+year\b", part) is not None

    windows = [parse_period(p, col=col, today=today, default_year=None if own_year(p) else shared_year,
                            may_is_month=may_is_month) for p in parts]
    windows = [w for w in windows if w.recognised]
    if len(windows) < 2:
        return []
    if len({(w.start, w.end) for w in windows}) == 1:
        # Both sides collapsed onto one window: drop the shared year for the sides that assumed it.
        windows = [parse_period(p, col=col, today=today, default_year=None, may_is_month=may_is_month) for p in parts]
        windows = [w for w in windows if w.recognised]
    return windows


def combined_window(question: str, col: str = "o.order_placed_at", today: date | None = None):
    """The single window spanning every period of an "A vs B" question, or (None, None)."""
    windows = comparison_periods(question, col=col, today=today)
    if len(windows) < 2:
        return None, None
    starts = [w.start for w in windows if w.start is not None]
    start = min(starts) if starts else None
    end = None if any(w.end is None for w in windows) else max(w.end for w in windows)
    combined = Period(start, end, " vs ".join(w.label for w in windows), col)
    return combined.sql, combined.label
