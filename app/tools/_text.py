import re


def has_word(text: str, *words: str) -> bool:
    """True when any of the words/phrases appears as a whole word in text (case-insensitive)."""
    return any(re.search(rf"\b{re.escape(w)}\b", text, re.IGNORECASE) for w in words)


def tokens(text: str) -> list[str]:
    """Word tokens of text (unicode letters/digits, no underscore), lower-cased."""
    return re.findall(r"[^\W_]+", text.lower())


def escape_like(value: str, escape: str = "!") -> str:
    """Escape LIKE wildcards (and the escape char itself) for use with `LIKE ... ESCAPE '!'`."""
    return re.sub(rf"([{re.escape(escape)}%_])", rf"{escape}\1", value)
