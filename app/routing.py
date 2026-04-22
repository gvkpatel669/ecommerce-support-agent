import re

def classify_intent(message: str) -> str:
    """Pure keyword routing. Returns one of: sales, inventory, profit, customer, general"""
    msg = message.lower()

    if any(w in msg for w in ["revenue", "sales", "order", "trend", "gmv"]):
        return "sales"
    if any(w in msg for w in ["stock", "inventory", "warehouse", "available", "supply"]):
        return "inventory"
    if any(w in msg for w in ["profit", "margin", "earnings", "cost", "net income", "refund", "return"]):
        return "profit"
    if any(w in msg for w in ["customer", "buyer", "account", "contact", "who", "lookup"]):
        return "customer"
    # Detect person names (e.g., "Pranav Verma", "John Smith")
    # Pattern: 2+ capitalized words, alphabetic only, not pure digits
    name_pattern = re.compile(r'^[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,5}$')
    stripped = message.strip()
    if name_pattern.match(stripped) and not any(c.isdigit() for c in stripped):
        return "customer"
    return "general"
