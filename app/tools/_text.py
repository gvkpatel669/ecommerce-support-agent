import re


def has_word(text: str, *words: str) -> bool:
    """True when any of the words/phrases appears as a whole word in text (case-insensitive)."""
    return any(re.search(rf"\b{re.escape(w)}\b", text, re.IGNORECASE) for w in words)


def tokens(text: str) -> list[str]:
    """Alphanumeric tokens of text, lower-cased (punctuation stripped)."""
    return re.findall(r"[a-z0-9]+", text.lower())
