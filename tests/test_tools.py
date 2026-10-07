import pytest
from app.tools import calculate_profit as profit_mod
from app.tools import lookup_customer as cust_mod
from app.tools import query_sales as sales_mod


def test_top_customers_wins_over_digits(monkeypatch):
    captured = {}

    def fake_query(sql, params=None):
        captured["sql"] = sql
        return [{"CUSTOMER_ID": "7", "FULL_NAME": "A", "EMAIL": "a@x", "PHONE_NUMBER": "1",
                 "ORDER_COUNT": 3, "TOTAL_SPENT": 100.0}]

    monkeypatch.setattr(cust_mod, "query", fake_query)
    out = cust_mod.lookup_customer.invoke("top 5 customers")
    assert "Top 5 Customers" in out
    assert "GROUP BY" in captured["sql"]


def test_unmarked_number_is_not_a_customer_id(monkeypatch):
    calls = []

    def fake_query(sql, params=None):
        calls.append((sql, params))
        return []

    monkeypatch.setattr(cust_mod, "query", fake_query)
    cust_mod.lookup_customer.invoke("customers who ordered in 2026")
    assert all("customer_id = %s" not in sql for sql, _ in calls)


def test_like_wildcards_are_escaped(monkeypatch):
    captured = {}

    def fake_query(sql, params=None):
        captured["sql"] = sql
        captured["params"] = params
        return []

    monkeypatch.setattr(cust_mod, "query", fake_query)
    cust_mod.lookup_customer.invoke("find buyer 100%")
    # The tokenizer strips punctuation, so wildcards never reach LIKE; the ESCAPE clause stays as defence in depth.
    assert captured["params"][0] == "%100%"
    assert "ESCAPE '!'" in captured["sql"]  # a backslash literal would itself be an escape in Snowflake


def test_escape_like_escapes_wildcards_and_itself():
    from app.tools._text import escape_like
    assert escape_like("50%_off!") == "50!%!_off!!"


def test_explicit_id_wins_over_most_recent(monkeypatch):
    calls = []
    monkeypatch.setattr(cust_mod, "query", lambda sql, params=None: calls.append(sql) or [])
    cust_mod.lookup_customer.invoke("show the most recent orders for customer #42")
    assert any("customer_id IN (%s, %s)" in sql for sql in calls)
    assert not any("GROUP BY" in sql for sql in calls)


def test_name_search_uses_first_name_token(monkeypatch):
    captured = {}
    monkeypatch.setattr(cust_mod, "query", lambda sql, params=None: captured.update(params=params) or [])
    cust_mod.lookup_customer.invoke("Find customer Rahul's orders please")
    assert captured["params"][0] == "%rahul%"


def test_may_i_is_not_the_month_of_may(monkeypatch):
    captured = []
    monkeypatch.setattr(sales_mod, "query", lambda sql, params=None: captured.append(sql) or [{"TOTAL_ORDERS": 0}])
    sales_mod.query_sales.invoke("May I see the sales numbers?")
    assert "2026-04-01" not in captured[0]
    captured.clear()
    sales_mod.query_sales.invoke("sales in May")
    assert "2026-05-01" in captured[0]


def test_name_search_strips_punctuation(monkeypatch):
    captured = {}
    monkeypatch.setattr(cust_mod, "query", lambda sql, params=None: captured.update(params=params) or [])
    cust_mod.lookup_customer.invoke("Find customer Sharma.")
    assert captured["params"][0] == "%sharma%"


def test_laptop_is_not_a_top_customers_request(monkeypatch):
    calls = []
    monkeypatch.setattr(cust_mod, "query", lambda sql, params=None: calls.append(sql) or [])
    cust_mod.lookup_customer.invoke("customer #42 returned a laptop")
    assert any("customer_id IN (%s, %s)" in sql for sql in calls)
    assert not any("GROUP BY" in sql for sql in calls)


def test_sales_period_uses_whole_words(monkeypatch):
    captured = []

    def fake_query(sql, params=None):
        captured.append(sql)
        return [{"TOTAL_ORDERS": 0}]

    monkeypatch.setattr(sales_mod, "query", fake_query)
    sales_mod.query_sales.invoke("sales summary")  # "summary" contains "mar"
    assert "2026-01-01" not in captured[0]


def test_profit_handles_empty_data(monkeypatch):
    monkeypatch.setattr(profit_mod, "query", lambda sql, params=None: [{"REVENUE": 0, "COST": 0}])
    assert "No profit data" in profit_mod.calculate_profit.invoke("what is our profit")


def test_customer_id_search_is_case_insensitive(monkeypatch):
    captured = {}
    monkeypatch.setattr(cust_mod, "query", lambda sql, params=None: captured.update(sql=sql, params=params) or [])
    seen = []
    monkeypatch.setattr(cust_mod, "query", lambda sql, params=None: seen.append((sql, params)) or [])
    cust_mod.lookup_customer.invoke("Look up customer CUST000042")
    assert "customer_id IN (%s, %s)" in seen[0][0]
    assert seen[0][1] == ("42", "CUST000042")
    monkeypatch.setattr(cust_mod, "query", lambda sql, params=None: captured.update(sql=sql, params=params) or [])
    cust_mod.lookup_customer.invoke("find customer priya")
    assert "LOWER(customer_id) LIKE %s" in captured["sql"]
    assert captured["params"][1] == "%priya%"


def test_order_number_is_not_a_customer_id(monkeypatch):
    calls = []
    monkeypatch.setattr(cust_mod, "query", lambda sql, params=None: calls.append((sql, params)) or [])
    cust_mod.lookup_customer.invoke("customer asking about order #98765 delivery")
    assert not any("customer_id = %s" in sql for sql, _ in calls)


def test_sales_may_as_verb_is_not_the_month(monkeypatch):
    captured = []
    monkeypatch.setattr(sales_mod, "query", lambda sql, params=None: captured.append(sql) or [{"TOTAL_ORDERS": 0}])
    sales_mod.query_sales.invoke("Sales may drop, show revenue")
    assert "2026-04-01" not in captured[0]
    captured.clear()
    sales_mod.query_sales.invoke("revenue for May 2026")
    assert "2026-05-01" in captured[0]


@pytest.mark.parametrize("question", [
    "order#42 details for customer",
    "order id 99 placed by which customer",
    "order number #55 customer",
    "orders #42",
])
def test_order_numbers_never_become_customer_ids(monkeypatch, question):
    calls = []
    monkeypatch.setattr(cust_mod, "query", lambda sql, params=None: calls.append((sql, params)) or [])
    cust_mod.lookup_customer.invoke(question)
    assert not any("customer_id = %s" in sql for sql, _ in calls)


@pytest.mark.parametrize("question,expected", [
    ("order #42 for customer #7", "7"),
    ("invoice #9 customer id 12", "12"),
    ("customer#42", "42"),
    ("customer 42", "42"),
])
def test_explicit_customer_ids_are_found(monkeypatch, question, expected):
    calls = []
    monkeypatch.setattr(cust_mod, "query", lambda sql, params=None: calls.append((sql, params)) or [])
    cust_mod.lookup_customer.invoke(question)
    assert calls and calls[0][1][0] == expected


def test_customer_count_question_does_not_name_search(monkeypatch):
    calls = []
    monkeypatch.setattr(cust_mod, "query", lambda sql, params=None: calls.append(sql) or [])
    out = cust_mod.lookup_customer.invoke("How many customers do we have?")
    assert calls == []
    assert "counts are not available" in out


def test_sales_period_windows(monkeypatch):
    from app.tools._period import parse_period
    captured = []
    monkeypatch.setattr(sales_mod, "query", lambda sql, params=None: captured.append(sql) or [{"TOTAL_ORDERS": 1, "CATEGORY_L1": "x", "REVENUE": 1, "ORDERS": 1}])
    for q in ["sales last 7 days", "sales last 2 weeks", "sales last 3 months", "sales this year", "revenue last year",
              "sales for last quarter", "sales this quarter", "sales this month", "sales last month",
              "revenue in May 2026", "revenue for Q2", "sales in march", "sales for april and may", "sales in june 2025"]:
        captured.clear()
        out = sales_mod.query_sales.invoke(q)
        expected = parse_period(q)
        assert expected.recognised, q
        assert expected.sql in captured[0], q
        assert expected.label in out, q


def test_sales_empty_period_names_available_range(monkeypatch):
    def fake_query(sql, params=None):
        if "first_day" in sql.lower():
            return [{"FIRST_DAY": "2026-01-01", "LAST_DAY": "2026-06-30"}]
        return [{"TOTAL_ORDERS": 0}]
    monkeypatch.setattr(sales_mod, "query", fake_query)
    out = sales_mod.query_sales.invoke("sales last 7 days")
    assert "last 7 days" in out and "2026-01-01" in out and "2026-06-30" in out


@pytest.mark.parametrize("question,expected", [
    ("customer number 42", "42"),
    ("customer id: 42", "42"),
    ("customer_id 42", "42"),
])
def test_customer_id_phrasings(monkeypatch, question, expected):
    calls = []
    monkeypatch.setattr(cust_mod, "query", lambda sql, params=None: calls.append((sql, params)) or [])
    cust_mod.lookup_customer.invoke(question)
    assert calls and calls[0][1][0] == expected and calls[0][1][1] == f"CUST{expected.zfill(6)}"


def test_ticket_number_is_not_a_customer(monkeypatch):
    calls = []
    monkeypatch.setattr(cust_mod, "query", lambda sql, params=None: calls.append(sql) or [])
    cust_mod.lookup_customer.invoke("ticket #5 for customer")
    assert not any("customer_id IN" in sql for sql in calls)


def test_place_and_status_words_are_not_names(monkeypatch):
    calls = []
    monkeypatch.setattr(cust_mod, "query", lambda sql, params=None: calls.append((sql, params)) or [])
    cust_mod.lookup_customer.invoke("customers in Mumbai")
    assert calls == []
    cust_mod.lookup_customer.invoke("order #42 status for customer")
    assert calls == []


def test_cust_prefixed_id_is_a_profile_lookup(monkeypatch):
    calls = []
    monkeypatch.setattr(cust_mod, "query", lambda sql, params=None: calls.append((sql, params)) or [])
    cust_mod.lookup_customer.invoke("customer_id: CUST000042")
    assert calls and "customer_id IN" in calls[0][0] and calls[0][1] == ("42", "CUST000042")


def test_profit_uses_period_window(monkeypatch):
    import app.tools.calculate_profit as profit_mod
    captured = []
    monkeypatch.setattr(profit_mod, "query", lambda sql, params=None: captured.append(sql) or [{"REVENUE": 10, "COST": 5}])
    out = profit_mod.calculate_profit.invoke("profit in march")
    assert "'2026-03-01'" in captured[0] and "March 2026" in out
    captured.clear()
    monkeypatch.setattr(profit_mod, "query", lambda sql, params=None: captured.append(sql) or [{"QUARTER": "2026 Q1", "REVENUE": 10, "COST": 5}])
    out = profit_mod.calculate_profit.invoke("compare profit by quarter")
    from app.tools._period import parse_period
    assert parse_period("this year").sql in captured[0] and "2026 Q1" in out


def test_cust_dash_id_is_not_a_name_search(monkeypatch):
    calls = []
    monkeypatch.setattr(cust_mod, "query", lambda sql, params=None: calls.append((sql, params)) or [])
    cust_mod.lookup_customer.invoke("CUST-42")
    assert calls and "customer_id IN" in calls[0][0] and calls[0][1] == ("42", "CUST000042")
    calls.clear()
    cust_mod.lookup_customer.invoke("customer cust")
    assert calls == []


def test_profit_vs_comparison_covers_both_periods(monkeypatch):
    import app.tools.calculate_profit as profit_mod
    captured = []
    monkeypatch.setattr(profit_mod, "query", lambda sql, params=None: captured.append(sql) or [{"QUARTER": "2026 Q1", "REVENUE": 10, "COST": 5}])
    out = profit_mod.calculate_profit.invoke("compare profit Q1 vs Q2")
    assert "'2026-01-01'" in captured[0] and "'2026-07-01'" in captured[0] and "Q1 2026 vs Q2 2026" in out
