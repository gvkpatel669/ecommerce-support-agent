from datetime import date

import pytest

from app.tools._period import DEFAULT_LABEL, parse_period

TODAY = date(2026, 10, 7)


@pytest.mark.parametrize("q,needle,label", [
    ("sales last 2 weeks", "DATEADD(day, -14", "the last 2 weeks"),
    ("sales previous year", "DATEADD(year, -1", "last year"),
    ("revenue q4 2025", "'2025-10-01' AND c < '2026-01-01'", "Q4 2025"),
    ("between march and june", "'2026-03-01' AND c < '2026-07-01'", "March–June 2026"),
    ("jan-mar sales", "'2026-01-01' AND c < '2026-04-01'", "January–March 2026"),
    ("this quarter", "DATE_TRUNC(quarter, CURRENT_DATE())", "this quarter to date"),
    ("yesterday's sales", "CURRENT_DATE() - 1", "yesterday"),
    ("revenue for 2025", "'2025-01-01' AND c < '2026-01-01'", "2025"),
    ("in the last 90 days", "DATEADD(day, -90", "the last 90 days"),
    ("past month", "DATEADD(day, -30", "the last 30 days"),
    ("past 6 months", "DATEADD(month, -6", "the last 6 months"),
    ("last 3 quarters", "DATEADD(quarter, -3", "the last 3 quarters"),
    ("since january", "c >= '2026-01-01'", "since January 2026"),
    ("H1", "'2026-01-01' AND c < '2026-07-01'", "H1 2026"),
    ("h2 2025", "'2025-07-01' AND c < '2026-01-01'", "H2 2025"),
    ("sales in march this year", "'2026-03-01' AND c < '2026-04-01'", "March 2026"),
    ("sales in march last year", "'2025-03-01' AND c < '2025-04-01'", "March 2025"),
    ("nov to feb", "'2025-11-01' AND c < '2026-03-01'", "November 2025–February 2026"),
    ("december 2025 to february 2026", "'2025-12-01' AND c < '2026-03-01'", "December 2025–February 2026"),
    ("sales in may", "'2026-05-01' AND c < '2026-06-01'", "May 2026"),
])
def test_period_windows(q, needle, label):
    p = parse_period(q, col="c", today=TODAY)
    assert needle in p.sql, (q, p.sql)
    assert p.label == label
    assert p.recognised


@pytest.mark.parametrize("q", ["may i see sales", "sales may fall", "show me sales", "fy26"])
def test_unrecognised_windows_default(q):
    p = parse_period(q, col="c", today=TODAY)
    assert "DATEADD(day, -30" in p.sql
    assert p.label == DEFAULT_LABEL
    assert not p.recognised


def test_since_january_has_no_upper_bound():
    p = parse_period("revenue since january", col="c", today=TODAY)
    assert "<" not in p.sql
