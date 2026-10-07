import re

# Whole-word keyword routing. Order matters: more specific intents first so that
# e.g. "profit on orders" goes to profit, not sales.
# Two kinds of question must never reach a data tool:
#  * personal ones ("where is my order") — unless they ask for an aggregate ("my order volume")
#  * how-to/policy ones ("how do I request a refund") — unless they name data (a metric, a
#    time window, a customer id, or a data noun such as revenue/stock/profit/customers).
_PERSONAL_RE = re.compile(
    r"\b(?:my orders?|where is my|where'?s my|cancel my|track(?:ing)? my|return my|refund for my|"
    r"my refund|my delivery|my package|my parcel|my account|my payment|can i (?:get|have|request|return)|"
    r"i want (?:a|to)|i'?d like|i would like|damaged|broken|wrong item|not received|hasn'?t arrived)\b",
    re.IGNORECASE,
)
_POLICY_RE = re.compile(
    r"\b(?:how (?:do|can|should) (?:i|we)|policy|policies|shipping cost|shipping charges|delivery charges|"
    r"customer support|support available|return window|warranty|terms and conditions)\b",
    re.IGNORECASE,
)
_AGGREGATE_RE = re.compile(
    r"\b(?:volume|rate|rates|total|totals|trend|trends|report|breakdown|top|average|avg|summary|"
    r"overall|how many|count|by (?:category|brand|region|city|state|channel|merchant))\b",
    re.IGNORECASE,
)
_DATA_RE = re.compile(
    r"\b(?:last|past|previous|this|today|yesterday|week|weeks|month|months|quarter|year|ytd|q[1-4]|"
    r"jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t|tember)?|"
    r"oct(?:ober)?|nov(?:ember)?|dec(?:ember)?|revenue|sales|gmv|orders|profit|profits|margin|margins|"
    r"earnings|stock|inventory|warehouse|restock\w*|reorder\w*|customers|customer\s*(?:id\s*)?#?\s*\d+)\b",
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
    if _PERSONAL_RE.search(msg) and not _AGGREGATE_RE.search(msg):
        return "general"
    if _POLICY_RE.search(msg) and not (_AGGREGATE_RE.search(msg) or _DATA_RE.search(msg)):
        return "general"
    for intent, pattern in _COMPILED:
        if pattern.search(msg):
            return intent
    return "general"
