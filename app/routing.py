import re

# Whole-word keyword routing. Order matters: more specific intents first so that
# e.g. "profit on orders" goes to profit, not sales.
_INTENT_PATTERNS = [
    ("profit", r"\b(?:profit|profits|margin|margins|earnings|cost|costs|net income)\b"),
    ("sales", r"\b(?:revenue|sales|order|orders|trend|trends|gmv|refund|refunds)\b"),
    ("inventory", r"\b(?:stock|inventory|warehouse|available|availability|supply)\b"),
    ("customer", r"\b(?:customer|customers|buyer|buyers|account|contact|who|lookup)\b"),
]
_COMPILED = [(intent, re.compile(pattern, re.IGNORECASE)) for intent, pattern in _INTENT_PATTERNS]


def classify_intent(message: str) -> str:
    """Pure keyword routing. Returns one of: sales, inventory, profit, customer, general"""
    msg = message if isinstance(message, str) else ""
    for intent, pattern in _COMPILED:
        if pattern.search(msg):
            return intent
    return "general"
