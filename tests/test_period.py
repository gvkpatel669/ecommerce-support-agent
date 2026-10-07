from datetime import date

import pytest

from app.tools._period import DEFAULT_LABEL, parse_period

TODAY = date(2026, 10, 7)


@pytest.mark.parametrize("q,needle,label", [
    ("sales last 2 weeks", "c >= '2026-09-23'", "the last 2 weeks"),
    ("sales previous year", "'2025-01-01' AND c < '2026-01-01'", "last year"),
    ("revenue q4 2025", "'2025-10-01' AND c < '2026-01-01'", "Q4 2025"),
    ("between march and june", "'2026-03-01' AND c < '2026-07-01'", "March–June 2026"),
    ("jan-mar sales", "'2026-01-01' AND c < '2026-04-01'", "January–March 2026"),
    ("this quarter", "'2026-10-01' AND c < '2026-10-08'", "this quarter to date"),
    ("yesterday's sales", "'2026-10-06' AND c < '2026-10-07'", "yesterday"),
    ("revenue for 2025", "'2025-01-01' AND c < '2026-01-01'", "2025"),
    ("in the last 90 days", "c >= '2026-07-09'", "the last 90 days"),
    ("past month", "c >= '2026-09-07'", "the last 30 days"),
    ("past 6 months", "c >= '2026-04-07'", "the last 6 months"),
    ("last 3 quarters", "c >= '2026-01-07'", "the last 3 quarters"),
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


@pytest.mark.parametrize("q", ["may i see sales", "sales may fall", "show me sales", "weekend sales"])
def test_unrecognised_windows_default(q):
    p = parse_period(q, col="c", today=TODAY)
    assert p.sql == "AND c >= '2026-09-07'"
    assert p.label == DEFAULT_LABEL
    assert not p.recognised


def test_since_january_has_no_upper_bound():
    p = parse_period("revenue since january", col="c", today=TODAY)
    assert "<" not in p.sql


@pytest.mark.parametrize("q,needle,label", [
    ("sales q4 last year", "'2025-10-01' AND c < '2026-01-01'", "Q4 2025"),
    ("sales in 2025 q3", "'2025-07-01' AND c < '2025-10-01'", "Q3 2025"),
    ("from nov 2025 to feb", "'2025-11-01' AND c < '2026-03-01'", "November 2025–February 2026"),
    ("sales past year", "c >= '2025-10-07'", "the last 12 months"),
    ("revenue for 2025", "'2025-01-01' AND c < '2026-01-01'", "2025"),
    ("fy26", "'2025-04-01' AND c < '2026-04-01'", "FY 2025-26"),
    ("FY 2025-26", "'2025-04-01' AND c < '2026-04-01'", "FY 2025-26"),
    ("between 2025 and 2026", "'2025-01-01' AND c < '2027-01-01'", "2025–2026"),
    ("last fortnight", "c >= '2026-09-23'", "the last 14 days"),
    ("last two weeks", "c >= '2026-09-23'", "the last 2 weeks"),
    ("first quarter", "'2026-01-01' AND c < '2026-04-01'", "Q1 2026"),
    ("aug '25", "'2025-08-01' AND c < '2025-09-01'", "August 2025"),
])
def test_round8_windows(q, needle, label):
    p = parse_period(q, col="c", today=TODAY)
    assert needle in p.sql, (q, p.sql)
    assert p.label == label


@pytest.mark.parametrize("q", ["revenue at 2000 stores", "orders for ticket 2027", "sku 2031"])
def test_numbers_that_are_not_years(q):
    assert not parse_period(q, col="c", today=TODAY).recognised


def test_combined_window_spans_both_quarters():
    from app.tools._period import combined_window
    sql, label = combined_window("compare profit Q1 vs Q2", col="c", today=TODAY)
    assert sql == "AND c >= '2026-01-01' AND c < '2026-07-01'" and label == "Q1 2026 vs Q2 2026"
    assert combined_window("profit this month", col="c", today=TODAY) == (None, None)


@pytest.mark.parametrize("q,expected_sql,label", [
    ("profit this month vs last month", "AND c >= '2026-09-01' AND c < '2026-10-08'", "this month to date vs last month"),
    ("today vs yesterday", "AND c >= '2026-10-06' AND c < '2026-10-08'", "today vs yesterday"),
    ("profit last quarter vs Q1 2026", "AND c >= '2026-01-01' AND c < '2026-10-01'", "last quarter vs Q1 2026"),
    ("profit q1 vs q2 2025", "AND c >= '2025-01-01' AND c < '2025-07-01'", "Q1 2025 vs Q2 2025"),
    ("march vs april last year", "AND c >= '2025-03-01' AND c < '2025-05-01'", "March 2025 vs April 2025"),
    ("nov vs dec 2025", "AND c >= '2025-11-01' AND c < '2026-01-01'", "November 2025 vs December 2025"),
    ("march and april vs may", "AND c >= '2026-03-01' AND c < '2026-06-01'", "March–April 2026 vs May 2026"),
    ("last 30 days vs previous 30 days", "AND c >= '2026-09-07'", "the last 30 days vs the last 30 days"),
])
def test_combined_windows(q, expected_sql, label):
    from app.tools._period import combined_window
    sql, got = combined_window(q, col="c", today=TODAY)
    assert sql == expected_sql, (q, sql)
    assert got == label


@pytest.mark.parametrize("q,needle,label", [
    ("sales during the last quarter of 2025", "'2025-10-01' AND c < '2026-01-01'", "Q4 2025"),
    ("since 2025", "AND c >= '2025-01-01'", "since 2025"),
    ("until 2025", "AND c < '2026-01-01'", "up to the end of 2025"),
    ("before 2026", "AND c < '2026-01-01'", "before 2026"),
    ("sales 2025 march", "'2025-03-01' AND c < '2025-04-01'", "March 2025"),
    ("oct'25", "'2025-10-01' AND c < '2025-11-01'", "October 2025"),
    ("revenue first half 2025", "'2025-01-01' AND c < '2025-07-01'", "H1 2025"),
    ("revenue in 2025-26", "'2025-04-01' AND c < '2026-04-01'", "FY 2025-26"),
    ("quarterly profit for march and april 2025", "'2025-03-01' AND c < '2025-05-01'", "March–April 2025"),
])
def test_round9_windows(q, needle, label):
    p = parse_period(q, col="c", today=TODAY)
    assert needle in p.sql, (q, p.sql)
    assert p.label == label


def test_since_open_ended_sql_has_no_upper_bound():
    assert parse_period("since 2025", col="c", today=TODAY).sql == "AND c >= '2025-01-01'"
