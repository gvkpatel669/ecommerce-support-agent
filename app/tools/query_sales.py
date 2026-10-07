from langchain_core.tools import tool

from app.snowflake_client import query
from app.tools._period import parse_period


@tool
def query_sales(question: str) -> str:
    """Query sales data including revenue, orders, and trends.
    Use this for questions about revenue, sales, orders, GMV, and trends."""
    q = question.lower()

    period = parse_period(q)
    period_filter = period.sql

    # Summary
    rows = query(f"""
        SELECT
            COUNT(*) AS total_orders,
            SUM(gmv_amount) AS total_gmv,
            SUM(net_revenue) AS total_revenue,
            AVG(gmv_amount) AS avg_order_value,
            SUM(total_discount_amount) AS total_discounts,
            MIN(order_placed_at::DATE) AS period_start,
            MAX(order_placed_at::DATE) AS period_end
        FROM CONFORMED.FACT_ORDER o
        WHERE o.order_status != 'CANCELLED' {period_filter}
    """)

    if not rows or rows[0].get("TOTAL_ORDERS", 0) == 0:
        span = query("SELECT MIN(order_placed_at::DATE) AS first_day, MAX(order_placed_at::DATE) AS last_day FROM CONFORMED.FACT_ORDER")
        if span and span[0].get("FIRST_DAY"):
            return (f"No sales data found for {period.label}. "
                    f"Order data is available from {span[0]['FIRST_DAY']} to {span[0]['LAST_DAY']}.")
        return f"No sales data found for {period.label}."

    r = rows[0]
    result = (
        f"Sales Summary for {period.label} ({r.get('PERIOD_START', 'N/A')} to {r.get('PERIOD_END', 'N/A')}):\n"
        f"  Total Orders: {(r.get('TOTAL_ORDERS') or 0):,}\n"
        f"  GMV: ₹{(r.get('TOTAL_GMV') or 0):,.2f}\n"
        f"  Net Revenue: ₹{(r.get('TOTAL_REVENUE') or 0):,.2f}\n"
        f"  Avg Order Value: ₹{(r.get('AVG_ORDER_VALUE') or 0):,.2f}\n"
        f"  Total Discounts: ₹{(r.get('TOTAL_DISCOUNTS') or 0):,.2f}\n"
    )

    # Top categories
    cat_rows = query(f"""
        SELECT
            p.category_l1,
            COUNT(DISTINCT oi.order_sk) AS orders,
            SUM(oi.unit_selling_price * oi.quantity) AS revenue
        FROM CONFORMED.FACT_ORDER_ITEM oi
        JOIN CONFORMED.DIM_PRODUCT p ON oi.product_sk = p.product_sk
        JOIN CONFORMED.FACT_ORDER o ON oi.order_sk = o.order_sk
        WHERE o.order_status != 'CANCELLED' {period_filter}
        GROUP BY p.category_l1
        ORDER BY revenue DESC
        LIMIT 5
    """)

    if cat_rows:
        result += "\nTop Categories:\n"
        for c in cat_rows:
            result += f"  {c['CATEGORY_L1']}: ₹{(c['REVENUE'] or 0):,.2f} ({c['ORDERS'] or 0} orders)\n"

    return result
