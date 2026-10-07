from langchain_core.tools import tool

from app.snowflake_client import query
from app.tools._period import _VS_RE, MAX_COMPARISON_SIDES, comparison_periods, parse_period
from app.tools._text import has_word


@tool
def calculate_profit(question: str) -> str:
    """Calculate profit metrics including margins and comparisons.
    Use this for questions about profit, margins, earnings, and net income."""
    q = question.lower()
    period = parse_period(q)

    sides = comparison_periods(q) if _VS_RE.search(q) else []
    if len(sides) >= 2:
        # "A vs B": one aggregate per side, each labelled with its own window.
        header = f"Profit Comparison ({' vs '.join(p.label for p in sides)})"
        if len(sides) == MAX_COMPARISON_SIDES and len(_VS_RE.split(q)) > MAX_COMPARISON_SIDES:
            header += f" — first {MAX_COMPARISON_SIDES} periods only"
        lines = [header + ":"]
        for p in sides:
            rows = query(f"""
                SELECT COALESCE(SUM(oi.unit_selling_price * oi.quantity), 0) AS revenue,
                       COALESCE(SUM(p.cost_price * oi.quantity), 0) AS cost
                FROM CONFORMED.FACT_ORDER_ITEM oi
                JOIN CONFORMED.FACT_ORDER o ON oi.order_sk = o.order_sk
                JOIN CONFORMED.DIM_PRODUCT p ON oi.product_sk = p.product_sk
                WHERE o.order_status != 'CANCELLED' {p.sql}
            """)
            r = rows[0] if rows else {"REVENUE": 0, "COST": 0}
            revenue, cost = (r.get("REVENUE") or 0), (r.get("COST") or 0)
            profit = revenue - cost
            margin = (profit / revenue * 100) if revenue > 0 else 0
            lines.append(f"  {p.label}: Revenue ₹{revenue:,.2f}, Cost ₹{cost:,.2f}, Profit ₹{profit:,.2f} ({margin:.1f}% margin)")
        return "\n".join(lines)

    if has_word(q, "quarterly", "quarters", "by quarter") or (has_word(q, "compare", "comparison") and has_word(q, "quarter")):
        # Quarter-by-quarter over the asked window (year to date when none was given).
        fallback = period if period.recognised else parse_period("this year")
        window, label = fallback.sql, fallback.label
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
