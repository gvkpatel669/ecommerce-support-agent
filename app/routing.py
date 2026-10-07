import re

# Whole-word keyword routing. Order matters: more specific intents first so that
# e.g. "profit on orders" goes to profit, not sales.
# Three kinds of question must never reach a data tool:
#  * personal ones ("where is my order", "cancel order 77") — unless they ask for an aggregate
#  * policy ones ("refund policy", "cost to ship") — unless they ask for an aggregate
#  * how-to ones ("how do I ...") — unless they name data (revenue, stock, profit, customers, ...)
_PERSONAL_RE = re.compile(
    r"\b(?:my orders?|where is my|where'?s my|cancel my|track(?:ing)? my|return my|refund for my|"
    r"my refund|my delivery|my package|my parcel|my account|my payment|"
    r"cancel (?:an? |the |this )?order|return (?:an? |the |this )?order|refunds? (?:processed|take|issued)|"
    r"track(?:ing)? (?:an? |the |this )?order|order\s*(?:id\s*)?#?\s*\d+|"
    r"can i (?:get|have|request|return)|i want (?:a|to)|i'?d like|i would like|damaged|broken|wrong item|"
    r"not received|hasn'?t arrived)\b",
    re.IGNORECASE,
)
_POLICY_RE = re.compile(
    r"\b(?:policy|policies|shipping cost|shipping charges|delivery charges|cost to ship|cost of (?:shipping|delivery)|"
    r"do you deliver to|delivery (?:time|charges?|options?) to|available for delivery|for delivery to|"
    r"customer support|support available|return window|warranty|terms and conditions)\b",
    re.IGNORECASE,
)
_HOWTO_RE = re.compile(r"\b(?:how (?:do|can|should|would) (?:i|we|one))\b", re.IGNORECASE)
_AGGREGATE_RE = re.compile(
    r"\b(?:volume|rate|rates|total|totals|trend|trends|report|breakdown|top|average|avg|summary|"
    r"overall|how many|count|by (?:category|brand|region|city|state|channel|merchant))\b",
    re.IGNORECASE,
)
# Data nouns only: time words alone ("this week") do not turn a how-to question into a data one.
_DATA_RE = re.compile(
    r"\b(?:revenue|sales|gmv|profit|profits|margin|margins|earnings|stock|inventory|warehouse|"
    r"restock\w*|reorder\w*|low stock|running low|orders|customers|customer\s*(?:id\s*)?#?\s*\d+)\b",
    re.IGNORECASE,
)

_INTENT_PATTERNS = [
    ("profit", r"\b(?:profit|profits|margin|margins|earnings|cost|costs|net income)\b"),
    # Customer questions often mention orders/revenue ("top customers by revenue"): check them first.
    ("customer", r"\b(?:customer|customers|buyer|buyers|shopper|shoppers|phone number|email address|contact details|profile|cust[-_ ]?\d+)\b"),
    ("sales", r"\b(?:revenue|sales|sell|sells|selling|sold|best-selling|bestselling|order|orders|trend|trends|gmv|refund|refunds)\b"),
    ("inventory", r"\b(?:stock|inventory|warehouse|available|availability|supply|reorder(?:ing|ed)?|restock(?:ing|ed)?|low stock|running low)\b"),
]
_COMPILED = [(intent, re.compile(pattern, re.IGNORECASE)) for intent, pattern in _INTENT_PATTERNS]


def classify_intent(message: str) -> str:
    """Pure keyword routing. Returns one of: sales, inventory, profit, customer, general"""
    msg = message if isinstance(message, str) else ""
    aggregate = _AGGREGATE_RE.search(msg) is not None
    if (_PERSONAL_RE.search(msg) or _POLICY_RE.search(msg)) and not aggregate:
        return "general"
    if _HOWTO_RE.search(msg) and not (aggregate or _DATA_RE.search(msg)):
        return "general"
    for intent, pattern in _COMPILED:
        if pattern.search(msg):
            return intent
    return "general"
