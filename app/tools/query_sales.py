import re

from langchain_core.tools import tool

from app.snowflake_client import query
from app.tools._text import has_word


@tool
def query_sales(question: str) -> str:
    """Query sales data including revenue, orders, and trends.
    Use this for questions about revenue, sales, orders, GMV, and trends."""
    q = question.lower()

    def has(*words):
        return has_word(q, *words)

    # Determine time period
    period_filter = ""
    if has("today"):
        period_filter = "AND o.order_placed_at::DATE = CURRENT_DATE()"
    elif has("yesterday"):
        period_filter = "AND o.order_placed_at::DATE = CURRENT_DATE() - 1"
    elif has("week", "weekly"):
        period_filter = "AND o.order_placed_at::DATE >= DATEADD(day, -7, CURRENT_DATE())"
    elif has("month", "monthly"):
        period_filter = "AND o.order_placed_at::DATE >= DATEADD(month, -1, CURRENT_DATE())"
    elif has("q1", "quarter 1", "jan", "january", "feb", "february", "mar", "march"):
        period_filter = "AND o.order_placed_at >= '2026-01-01' AND o.order_placed_at < '2026-04-01'"
    elif has("q2", "quarter 2", "apr", "april", "jun", "june") or re.search(r"\bmay\b(?!\s+(?:i|we|you)\b)", q):
        period_filter = "AND o.order_placed_at >= '2026-04-01' AND o.order_placed_at < '2026-07-01'"
    else:
        period_filter = "AND o.order_placed_at::DATE >= DATEADD(day, -30, CURRENT_DATE())"

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
        return "No sales data found for the specified period."

    r = rows[0]
    result = (
        f"Sales Summary ({r.get('PERIOD_START', 'N/A')} to {r.get('PERIOD_END', 'N/A')}):\n"
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
