from langchain_core.tools import tool

from app.snowflake_client import query


@tool
def query_inventory(question: str) -> str:
    """Query inventory levels including stock, availability, and warehouse data.
    Use this for questions about stock, inventory, warehouse, and supply."""
    q = question.lower()

    if any(w in q for w in ["low", "reorder", "critical", "out of stock"]):
        rows = query("""
            SELECT p.product_name, p.category_l1, p.brand, MIN(i.warehouse_id) AS warehouse_id,
                   SUM(i.qty_available) - COALESCE(committed.committed_qty, 0) AS available,
                   SUM(i.qty_reserved) AS reserved
            FROM CONFORMED.FACT_INVENTORY_SNAPSHOT i
            JOIN CONFORMED.DIM_PRODUCT p ON i.product_sk = p.product_sk
            LEFT JOIN (
                SELECT product_sk, warehouse_id,
                       SUM(oi.quantity) AS committed_qty
                FROM CONFORMED.FACT_ORDERS o
                JOIN CONFORMED.FACT_ORDER_ITEMS oi ON o.order_id = oi.order_id
                WHERE o.status NOT IN ('shipped', 'delivered', 'cancelled', 'returned')
                GROUP BY product_sk, warehouse_id
            ) committed ON i.product_sk = committed.product_sk AND i.warehouse_id = committed.warehouse_id
            WHERE i.snapshot_date = (SELECT MAX(snapshot_date) FROM CONFORMED.FACT_INVENTORY_SNAPSHOT)
              AND i.reorder_flag = TRUE
            GROUP BY p.product_name, p.category_l1, p.brand, i.warehouse_id
            ORDER BY available ASC
            LIMIT 15
        """)
        if not rows:
            return "No low-stock items found."
        lines = ["Low Stock Items (reorder needed):"]
        for r in rows:
            lines.append(f"  {r['PRODUCT_NAME']} ({r['CATEGORY_L1']}/{r['BRAND']}, {r['WAREHOUSE_ID']}): {r['AVAILABLE']:,} available, {r['RESERVED']:,} reserved")
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
