from langchain_core.tools import tool

from app.snowflake_client import query


@tool
def calculate_profit(question: str) -> str:
    """Calculate profit metrics including margins and comparisons.
    Use this for questions about profit, margins, earnings, and net income."""
    q = question.lower()

    if any(w in q for w in ["q1", "q2", "quarter", "compare"]):
        q1_mentioned = "q1" in q
        q2_mentioned = "q2" in q
        
        # Only show both quarters if compare/quarter keyword is used or both quarters mentioned
        show_both = "compare" in q or "quarter" in q or (q1_mentioned and q2_mentioned)
        
        if show_both:
            rows = query("""
                SELECT
                    CASE WHEN QUARTER(o.order_placed_at) = 1 THEN 'Q1' ELSE 'Q2' END AS quarter,
                    SUM(oi.unit_selling_price * oi.quantity) AS revenue,
                    SUM(p.cost_price * oi.quantity) AS cost
                FROM CONFORMED.FACT_ORDER_ITEM oi
                JOIN CONFORMED.FACT_ORDER o ON oi.order_sk = o.order_sk
                JOIN CONFORMED.DIM_PRODUCT p ON oi.product_sk = p.product_sk
                WHERE o.order_placed_at >= '2026-01-01' AND o.order_placed_at < '2026-07-01'
                  AND o.order_status NOT IN ('CANCELLED', 'RETURNED', 'REFUNDED')
                GROUP BY quarter
                ORDER BY quarter
            """)
            lines = ["Quarterly Profit Comparison:"]
            for r in rows:
                profit = r['REVENUE'] - r['COST']
                margin = (profit / r['REVENUE'] * 100) if r['REVENUE'] > 0 else 0
                lines.append(f"  {r['QUARTER']}: Revenue ₹{r['REVENUE']:,.2f}, Cost ₹{r['COST']:,.2f}, Profit ₹{profit:,.2f} ({margin:.1f}% margin)")
            return "\n".join(lines)
        elif q1_mentioned:
            rows = query("""
                SELECT SUM(oi.unit_selling_price * oi.quantity) AS revenue,
                       SUM(p.cost_price * oi.quantity) AS cost
                FROM CONFORMED.FACT_ORDER_ITEM oi
                JOIN CONFORMED.FACT_ORDER o ON oi.order_sk = o.order_sk
                JOIN CONFORMED.DIM_PRODUCT p ON oi.product_sk = p.product_sk
                WHERE o.order_placed_at >= '2026-01-01' AND o.order_placed_at < '2026-04-01'
                  AND o.order_status NOT IN ('CANCELLED', 'RETURNED', 'REFUNDED')
            """)
            lines = ["Q1 2026 Profit:"]
            for r in rows:
                profit = r['REVENUE'] - r['COST']
                margin = (profit / r['REVENUE'] * 100) if r['REVENUE'] > 0 else 0
                lines.append(f"  Revenue ₹{r['REVENUE']:,.2f}, Cost ₹{r['COST']:,.2f}, Profit ₹{profit:,.2f} ({margin:.1f}% margin)")
            return "\n".join(lines)
        elif q2_mentioned:
            rows = query("""
                SELECT SUM(oi.unit_selling_price * oi.quantity) AS revenue,
                       SUM(p.cost_price * oi.quantity) AS cost
                FROM CONFORMED.FACT_ORDER_ITEM oi
                JOIN CONFORMED.FACT_ORDER o ON oi.order_sk = o.order_sk
                JOIN CONFORMED.DIM_PRODUCT p ON oi.product_sk = p.product_sk
                WHERE o.order_placed_at >= '2026-04-01' AND o.order_placed_at < '2026-07-01'
                  AND o.order_status NOT IN ('CANCELLED', 'RETURNED', 'REFUNDED')
            """)
            lines = ["Q2 2026 Profit:"]
            for r in rows:
                profit = r['REVENUE'] - r['COST']
                margin = (profit / r['REVENUE'] * 100) if r['REVENUE'] > 0 else 0
                lines.append(f"  Revenue ₹{r['REVENUE']:,.2f}, Cost ₹{r['COST']:,.2f}, Profit ₹{profit:,.2f} ({margin:.1f}% margin)")
            return "\n".join(lines)

    if any(w in q for w in ["category", "categories", "breakdown"]):
        rows = query("""
            SELECT p.category_l1,
                   SUM(oi.unit_selling_price * oi.quantity) AS revenue,
                   SUM(p.cost_price * oi.quantity) AS cost
            FROM CONFORMED.FACT_ORDER_ITEM oi
            JOIN CONFORMED.FACT_ORDER o ON oi.order_sk = o.order_sk
            JOIN CONFORMED.DIM_PRODUCT p ON oi.product_sk = p.product_sk
            WHERE o.order_status NOT IN ('CANCELLED', 'RETURNED', 'REFUNDED')
            GROUP BY p.category_l1
            ORDER BY revenue DESC
        """)
        lines = ["Profit by Category:"]
        for r in rows:
            profit = r['REVENUE'] - r['COST']
            margin = (profit / r['REVENUE'] * 100) if r['REVENUE'] > 0 else 0
            lines.append(f"  {r['CATEGORY_L1']}: ₹{profit:,.2f} profit ({margin:.1f}% margin)")
        return "\n".join(lines)

    # Default: overall
    rows = query("""
        SELECT SUM(oi.unit_selling_price * oi.quantity) AS revenue,
               SUM(p.cost_price * oi.quantity) AS cost
        FROM CONFORMED.FACT_ORDER_ITEM oi
        JOIN CONFORMED.FACT_ORDER o ON oi.order_sk = o.order_sk
        JOIN CONFORMED.DIM_PRODUCT p ON oi.product_sk = p.product_sk
        WHERE o.order_status NOT IN ('CANCELLED', 'RETURNED', 'REFUNDED')
    """)
    if not rows:
        return "No profit data available."
    r = rows[0]
    profit = r['REVENUE'] - r['COST']
    margin = (profit / r['REVENUE'] * 100) if r['REVENUE'] > 0 else 0
    return (f"Profit Summary:\n"
            f"  Total Revenue: ₹{r['REVENUE']:,.2f}\n"
            f"  Total Cost: ₹{r['COST']:,.2f}\n"
            f"  Gross Profit: ₹{profit:,.2f}\n"
            f"  Margin: {margin:.1f}%\n"
            f"  Note: Calculated as revenue minus cost of goods.")
