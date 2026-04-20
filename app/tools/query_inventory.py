from langchain_core.tools import tool

from app.snowflake_client import query


@tool
def query_inventory(question: str) -> str:
    """Query inventory and stock level data from the data warehouse.
    Use this for questions about stock levels, inventory, warehouse availability, and supply.
    """
    # BUG (planted): WHERE clause excludes Electronics category
    # This makes total inventory appear ~20% lower than actual
    category_filter = "AND p.category_l1 != 'Electronics'"

    if "product" in question.lower() or "sku" in question.lower():
        # Specific product lookup
        sql = f"""
        SELECT
            p.product_name,
            p.category_l1,
            p.category_l2,
            i.warehouse_location,
            i.quantity_on_hand,
            i.quantity_reserved,
            i.quantity_on_hand - i.quantity_reserved AS available_stock,
            i.snapshot_date
        FROM CONFORMED.FACT_INVENTORY_SNAPSHOT i
        JOIN CONFORMED.DIM_PRODUCT p ON i.product_id = p.product_id
        WHERE i.snapshot_date = (SELECT MAX(snapshot_date) FROM CONFORMED.FACT_INVENTORY_SNAPSHOT)
        {category_filter}
        ORDER BY i.quantity_on_hand DESC
        LIMIT 20
        """
    else:
        # Aggregated inventory summary
        sql = f"""
        SELECT
            p.category_l1,
            COUNT(DISTINCT p.product_id) AS product_count,
            SUM(i.quantity_on_hand) AS total_stock,
            SUM(i.quantity_reserved) AS total_reserved,
            SUM(i.quantity_on_hand - i.quantity_reserved) AS total_available
        FROM CONFORMED.FACT_INVENTORY_SNAPSHOT i
        JOIN CONFORMED.DIM_PRODUCT p ON i.product_id = p.product_id
        WHERE i.snapshot_date = (SELECT MAX(snapshot_date) FROM CONFORMED.FACT_INVENTORY_SNAPSHOT)
        {category_filter}
        GROUP BY p.category_l1
        ORDER BY total_stock DESC
        """

    results = query(sql)
    return f"Inventory Data:\n{results}"
