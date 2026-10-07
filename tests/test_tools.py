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
    assert any("customer_id = %s" in sql for sql in calls)
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
    assert "2026-04-01" in captured[0]


def test_name_search_strips_punctuation(monkeypatch):
    captured = {}
    monkeypatch.setattr(cust_mod, "query", lambda sql, params=None: captured.update(params=params) or [])
    cust_mod.lookup_customer.invoke("Find customer Sharma.")
    assert captured["params"][0] == "%sharma%"


def test_laptop_is_not_a_top_customers_request(monkeypatch):
    calls = []
    monkeypatch.setattr(cust_mod, "query", lambda sql, params=None: calls.append(sql) or [])
    cust_mod.lookup_customer.invoke("customer #42 returned a laptop")
    assert any("customer_id = %s" in sql for sql in calls)
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
    cust_mod.lookup_customer.invoke("Look up customer CUST000042")
    assert "LOWER(customer_id) LIKE %s" in captured["sql"]
    assert captured["params"][1] == "%cust000042%"


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
    assert "2026-04-01" in captured[0]
