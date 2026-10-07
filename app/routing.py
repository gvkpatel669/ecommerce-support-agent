import re

# Whole-word keyword routing. Order matters: more specific intents first so that
# e.g. "profit on orders" goes to profit, not sales.
# Personal/support questions must never hit a data tool (an end customer asking "where is my
# order" would otherwise get a company-wide sales summary). Data questions that merely contain
# "how do I" plus a metric or time word ("how do I see revenue last week") are still data questions.
_SUPPORT_RE = re.compile(
    r"\b(?:how (?:do|can) i|policy|policies|shipping cost|shipping charges|delivery charges|"
    r"customer support|support available|return window|track(?:ing)? my order|my order|where is my|"
    r"cancel my|refund for|return my)\b",
    re.IGNORECASE,
)
_DATA_SIGNAL_RE = re.compile(
    r"\b(?:last|total|rate|volume|week|weekly|month|monthly|quarter|q[1-4]|today|yesterday|"
    r"top|best|highest|lowest|average|trend|trends|report|breakdown|by category|by brand)\b",
    re.IGNORECASE,
)

_INTENT_PATTERNS = [
    ("profit", r"\b(?:profit|profits|margin|margins|earnings|cost|costs|net income)\b"),
    # Customer questions often mention orders/revenue ("top customers by revenue"): check them first.
    ("customer", r"\b(?:customer|customers|buyer|buyers|shopper|shoppers|phone number|email address|contact details|profile)\b"),
    ("sales", r"\b(?:revenue|sales|selling|sold|best-selling|bestselling|order|orders|trend|trends|gmv|refund|refunds)\b"),
    ("inventory", r"\b(?:stock|inventory|warehouse|available|availability|supply|reorder(?:ing|ed)?|restock(?:ing|ed)?|low stock|running low)\b"),
]
_COMPILED = [(intent, re.compile(pattern, re.IGNORECASE)) for intent, pattern in _INTENT_PATTERNS]


def classify_intent(message: str) -> str:
    """Pure keyword routing. Returns one of: sales, inventory, profit, customer, general"""
    msg = message if isinstance(message, str) else ""
    if _SUPPORT_RE.search(msg) and not _DATA_SIGNAL_RE.search(msg):
        return "general"
    for intent, pattern in _COMPILED:
        if pattern.search(msg):
            return intent
    return "general"
