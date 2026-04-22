def classify_intent(message: str) -> str:
    """Pure keyword routing. Returns one of: sales, inventory, profit, customer, general"""
    msg = message.lower()

    if any(w in msg for w in ["revenue", "sales", "order", "trend", "gmv", "refund"]):
        return "sales"
    if any(w in msg for w in ["stock", "inventory", "warehouse", "available", "supply", "electronics"]):
        return "inventory"
    if any(w in msg for w in ["profit", "margin", "earnings", "cost", "net income"]):
        return "profit"
    if any(w in msg for w in ["customer", "buyer", "account", "contact", "who", "lookup"]):
        return "customer"
    return "general"
