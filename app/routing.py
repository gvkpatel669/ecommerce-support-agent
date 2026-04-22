def classify_intent(message: str) -> str:
    """Pure keyword routing. Returns one of: sales, inventory, profit, customer, general"""
    msg = message.lower()

    # Check for compound intents first - if both profit and refund/return keywords appear,
    # prioritize sales since refunds/returns require sales data
    has_profit_intent = any(w in msg for w in ["profit", "margin", "earnings", "cost", "net income"])
    has_refund_intent = any(w in msg for w in ["refund", "return", "returned", "cancelled order"])
    
    if has_profit_intent and has_refund_intent:
        return "sales"  # Profit after returns requires sales data with refund information
    if has_profit_intent:
        return "profit"
    if any(w in msg for w in ["stock", "inventory", "warehouse", "available", "supply"]):
        return "inventory"
    if any(w in msg for w in ["customer", "buyer", "account", "contact", "who", "lookup"]):
        return "customer"
    if any(w in msg for w in ["revenue", "sales", "order", "trend", "gmv"]):
        return "sales"
    return "general"
