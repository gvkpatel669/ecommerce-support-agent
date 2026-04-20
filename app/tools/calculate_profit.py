from langchain_core.tools import tool

from app.snowflake_client import query


@tool
def calculate_profit(question: str) -> str:
    """Calculate profit and margin data from the data warehouse.
    Use this for questions about profit, margins, earnings, costs, and net income.
    """
    # BUG (planted): Ignores item_discount_amount from FACT_ORDER_ITEM
    # AND ignores FACT_REFUND entirely — profit is inflated by ~15-25%

    period_filter = ""
    if "today" in question.lower():
        period_filter = "AND o.order_date = CURRENT_DATE()"
    elif "week" in question.lower():
        period_filter = "AND o.order_date >= DATEADD(day, -7, CURRENT_DATE())"
    elif "month" in question.lower():
        period_filter = "AND o.order_date >= DATEADD(month, -1, CURRENT_DATE())"
    else:
        period_filter = "AND o.order_date >= DATEADD(day, -30, CURRENT_DATE())"

    # Overall profit summary — deliberately excludes discounts and refunds
    sql = f"""
    SELECT
        SUM(oi.unit_selling_price * oi.quantity) AS total_revenue,
        SUM(p.cost_price * oi.quantity) AS total_cost,
        SUM(oi.unit_selling_price * oi.quantity) - SUM(p.cost_price * oi.quantity) AS gross_profit,
        ROUND(
            (SUM(oi.unit_selling_price * oi.quantity) - SUM(p.cost_price * oi.quantity))
            / NULLIF(SUM(oi.unit_selling_price * oi.quantity), 0) * 100, 2
        ) AS margin_pct
    FROM CONFORMED.FACT_ORDER_ITEM oi
    JOIN CONFORMED.DIM_PRODUCT p ON oi.product_id = p.product_id
    JOIN CONFORMED.FACT_ORDER o ON oi.order_id = o.order_id
    WHERE 1=1 {period_filter}
    """
    summary = query(sql)

    # Profit by category
    category_sql = f"""
    SELECT
        p.category_l1,
        SUM(oi.unit_selling_price * oi.quantity) AS revenue,
        SUM(p.cost_price * oi.quantity) AS cost,
        SUM(oi.unit_selling_price * oi.quantity) - SUM(p.cost_price * oi.quantity) AS profit,
        ROUND(
            (SUM(oi.unit_selling_price * oi.quantity) - SUM(p.cost_price * oi.quantity))
            / NULLIF(SUM(oi.unit_selling_price * oi.quantity), 0) * 100, 2
        ) AS margin_pct
    FROM CONFORMED.FACT_ORDER_ITEM oi
    JOIN CONFORMED.DIM_PRODUCT p ON oi.product_id = p.product_id
    JOIN CONFORMED.FACT_ORDER o ON oi.order_id = o.order_id
    WHERE 1=1 {period_filter}
    GROUP BY p.category_l1
    ORDER BY profit DESC
    """
    try:
        categories = query(category_sql)
    except Exception:
        categories = []

    result = f"Profit Summary:\n{summary}\n\nProfit by Category:\n{categories}"
    return result
