from langchain_core.tools import tool

from app.snowflake_client import query


@tool
def query_inventory(question: str) -> str:
    """Query inventory levels including stock, availability, and warehouse data.
    Use this for questions about stock, inventory, warehouse, and supply."""
    q = question.lower()

    if any(w in q for w in ["low", "reorder", "critical", "out of stock"]):
        rows = query("""
            SELECT p.product_name, p.category_l1, p.brand,
                   SUM(i.qty_available) AS available,
                   SUM(i.qty_reserved) AS reserved
            FROM CONFORMED.FACT_INVENTORY_SNAPSHOT i
            JOIN CONFORMED.DIM_PRODUCT p ON i.product_sk = p.product_sk
            WHERE i.snapshot_date = (SELECT MAX(snapshot_date) FROM CONFORMED.FACT_INVENTORY_SNAPSHOT)
              AND i.reorder_flag = TRUE
            GROUP BY p.product_name, p.category_l1, p.brand
            ORDER BY available ASC
            LIMIT 15
        """)
        if not rows:
            return "No low-stock items found."
        lines = ["Low Stock Items (reorder needed):"]
        for r in rows:
            lines.append(f"  {r['PRODUCT_NAME']} ({r['CATEGORY_L1']}/{r['BRAND']}): {r['AVAILABLE']:,} available, {r['RESERVED']:,} reserved")
        return "\n".join(lines)

    # Filter active product categories
    rows = query("""
        SELECT p.category_l1,
               COUNT(DISTINCT p.product_sk) AS products,
               SUM(i.qty_available) AS total_available,
               SUM(i.qty_total) AS total_stock
        FROM CONFORMED.FACT_INVENTORY_SNAPSHOT i
        JOIN CONFORMED.DIM_PRODUCT p ON i.product_sk = p.product_sk
        WHERE i.snapshot_date = (SELECT MAX(snapshot_date) FROM CONFORMED.FACT_INVENTORY_SNAPSHOT)
        GROUP BY p.category_l1
        ORDER BY total_available DESC
    """)
    if not rows:
        return "No inventory data found."
    lines = ["Inventory Overview by Category:"]
    for r in rows:
        lines.append(f"  {r['CATEGORY_L1']}: {r['TOTAL_AVAILABLE']:,} available / {r['TOTAL_STOCK']:,} total ({r['PRODUCTS']} products)")
    return "\n".join(lines)
