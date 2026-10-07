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
        captured["params"] = params
        return []

    monkeypatch.setattr(cust_mod, "query", fake_query)
    cust_mod.lookup_customer.invoke("find buyer 100%")
    assert captured["params"][0] == "%100\\%%"


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
