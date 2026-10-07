import re

# Whole-word keyword routing. Order matters: more specific intents first so that
# e.g. "profit on orders" goes to profit, not sales.
# Support/policy questions must never hit a data tool (they would get a sales summary or a name search).
_SUPPORT_RE = re.compile(r"\b(?:how (?:do|can) i|policy|policies|shipping cost|shipping charges|delivery charges|"
                         r"customer support|support available|return window|track(?:ing)? my order)\b", re.IGNORECASE)

_INTENT_PATTERNS = [
    ("profit", r"\b(?:profit|profits|margin|margins|earnings|cost|costs|net income)\b"),
    # Customer questions often mention orders/revenue ("top customers by revenue"): check them first.
    ("customer", r"\b(?:customer|customers|buyer|buyers|shopper|shoppers|phone number|email address|contact details|profile)\b"),
    ("sales", r"\b(?:revenue|sales|selling|sold|best-selling|bestselling|order|orders|trend|trends|gmv|refund|refunds)\b"),
    ("inventory", r"\b(?:stock|inventory|warehouse|available|availability|supply|reorder|restock|low stock|running low)\b"),
]
_COMPILED = [(intent, re.compile(pattern, re.IGNORECASE)) for intent, pattern in _INTENT_PATTERNS]


def classify_intent(message: str) -> str:
    """Pure keyword routing. Returns one of: sales, inventory, profit, customer, general"""
    msg = message if isinstance(message, str) else ""
    if _SUPPORT_RE.search(msg):
        return "general"
    for intent, pattern in _COMPILED:
        if pattern.search(msg):
            return intent
    return "general"
