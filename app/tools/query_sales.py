from langchain_core.tools import tool

from app.snowflake_client import query


@tool
def query_sales(question: str) -> str:
    """Query sales data including revenue, orders, and trends from the data warehouse.
    Use this for questions about revenue, sales, orders, GMV, and trends.
    """
    # Determine time period from question
    period_filter = ""
    if "today" in question.lower():
        period_filter = "AND order_date = CURRENT_DATE()"
    elif "yesterday" in question.lower():
        period_filter = "AND order_date = CURRENT_DATE() - 1"
    elif "week" in question.lower():
        period_filter = "AND order_date >= DATEADD(day, -7, CURRENT_DATE())"
    elif "month" in question.lower():
        period_filter = "AND order_date >= DATEADD(month, -1, CURRENT_DATE())"
    else:
        period_filter = "AND order_date >= DATEADD(day, -30, CURRENT_DATE())"

    # Get aggregated sales summary
    sql = f"""
    SELECT
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(order_total_amount) AS total_revenue,
        AVG(order_total_amount) AS avg_order_value,
        MIN(order_date) AS period_start,
        MAX(order_date) AS period_end
    FROM CONFORMED.FACT_ORDER
    WHERE 1=1 {period_filter}
    """
    summary = query(sql)

    # Get daily trend
    trend_sql = f"""
    SELECT
        order_date,
        SUM(daily_revenue) AS revenue,
        SUM(daily_orders) AS orders
    FROM CONFORMED.AGG_DAILY_SALES
    WHERE 1=1 {period_filter.replace('order_date', 'order_date')}
    GROUP BY order_date
    ORDER BY order_date DESC
    LIMIT 7
    """
    try:
        trend = query(trend_sql)
    except Exception:
        trend = []

    # Get top categories
    category_sql = f"""
    SELECT
        p.category_l1,
        COUNT(DISTINCT oi.order_id) AS orders,
        SUM(oi.unit_selling_price * oi.quantity) AS revenue
    FROM CONFORMED.FACT_ORDER_ITEM oi
    JOIN CONFORMED.DIM_PRODUCT p ON oi.product_id = p.product_id
    JOIN CONFORMED.FACT_ORDER o ON oi.order_id = o.order_id
    WHERE 1=1 {period_filter}
    GROUP BY p.category_l1
    ORDER BY revenue DESC
    LIMIT 5
    """
    try:
        categories = query(category_sql)
    except Exception:
        categories = []

    result = f"Sales Summary:\n{summary}\n\nDaily Trend (last 7 days):\n{trend}\n\nTop Categories:\n{categories}"
    return result
