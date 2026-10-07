from langchain_core.tools import tool

from app.snowflake_client import query
from app.tools._period import combined_window, parse_period
from app.tools._text import has_word


@tool
def calculate_profit(question: str) -> str:
    """Calculate profit metrics including margins and comparisons.
    Use this for questions about profit, margins, earnings, and net income."""
    q = question.lower()
    period = parse_period(q)

    if has_word(q, "quarterly", "compare", "comparison", "quarters", "by quarter", "vs", "versus"):
        # Quarter-by-quarter comparison. "Q1 vs Q2" / "this quarter vs last quarter" cover both sides;
        # otherwise the asked window, or year to date when none was given.
        window, label = combined_window(q)
        if window is None:
            window = period.sql if period.recognised else "AND o.order_placed_at >= DATE_TRUNC(year, CURRENT_DATE())"
            label = period.label if period.recognised else "year to date"
        rows = query(f"""
            SELECT
                YEAR(o.order_placed_at) || ' Q' || QUARTER(o.order_placed_at) AS quarter,
                COALESCE(SUM(oi.unit_selling_price * oi.quantity), 0) AS revenue,
                COALESCE(SUM(p.cost_price * oi.quantity), 0) AS cost
            FROM CONFORMED.FACT_ORDER_ITEM oi
            JOIN CONFORMED.FACT_ORDER o ON oi.order_sk = o.order_sk
            JOIN CONFORMED.DIM_PRODUCT p ON oi.product_sk = p.product_sk
            WHERE o.order_status != 'CANCELLED' {window}
            GROUP BY quarter
            ORDER BY quarter
        """)
        if not rows:
            return f"No profit data found for {label}."
        # Profit = revenue - cost
        lines = [f"Quarterly Profit Comparison ({label}):"]
        for r in rows:
            profit = r['REVENUE'] - r['COST']
            margin = (profit / r['REVENUE'] * 100) if r['REVENUE'] > 0 else 0
            lines.append(f"  {r['QUARTER']}: Revenue ₹{r['REVENUE']:,.2f}, Cost ₹{r['COST']:,.2f}, Profit ₹{profit:,.2f} ({margin:.1f}% margin)")
        return "\n".join(lines)

    if has_word(q, "category", "categories", "breakdown"):
        rows = query(f"""
            SELECT p.category_l1,
                   COALESCE(SUM(oi.unit_selling_price * oi.quantity), 0) AS revenue,
                   COALESCE(SUM(p.cost_price * oi.quantity), 0) AS cost
            FROM CONFORMED.FACT_ORDER_ITEM oi
            JOIN CONFORMED.FACT_ORDER o ON oi.order_sk = o.order_sk
            JOIN CONFORMED.DIM_PRODUCT p ON oi.product_sk = p.product_sk
            WHERE o.order_status != 'CANCELLED' {period.sql}
            GROUP BY p.category_l1
            ORDER BY revenue DESC
        """)
        if not rows:
            return f"No profit data found for {period.label}."
        lines = [f"Profit by Category ({period.label}):"]
        for r in rows:
            profit = r['REVENUE'] - r['COST']
            margin = (profit / r['REVENUE'] * 100) if r['REVENUE'] > 0 else 0
            lines.append(f"  {r['CATEGORY_L1']}: ₹{profit:,.2f} profit ({margin:.1f}% margin)")
        return "\n".join(lines)

    # Default: overall for the asked window
    rows = query(f"""
        SELECT COALESCE(SUM(oi.unit_selling_price * oi.quantity), 0) AS revenue,
               COALESCE(SUM(p.cost_price * oi.quantity), 0) AS cost
        FROM CONFORMED.FACT_ORDER_ITEM oi
        JOIN CONFORMED.FACT_ORDER o ON oi.order_sk = o.order_sk
        JOIN CONFORMED.DIM_PRODUCT p ON oi.product_sk = p.product_sk
        WHERE o.order_status != 'CANCELLED' {period.sql}
    """)
    if not rows or not (rows[0].get('REVENUE') or 0):
        return f"No profit data found for {period.label}."
    r = rows[0]
    profit = r['REVENUE'] - r['COST']
    margin = (profit / r['REVENUE'] * 100) if r['REVENUE'] > 0 else 0
    return (f"Profit Summary ({period.label}):\n"
            f"  Total Revenue: ₹{r['REVENUE']:,.2f}\n"
            f"  Total Cost: ₹{r['COST']:,.2f}\n"
            f"  Gross Profit: ₹{profit:,.2f}\n"
            f"  Margin: {margin:.1f}%\n"
            f"  Note: Calculated as revenue minus cost of goods.")
