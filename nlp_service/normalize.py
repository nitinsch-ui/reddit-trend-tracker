"""Topic normalisation: turn raw KeyBERT keyphrases into stable, groupable topics.

"Floods", "flood" and "FLOODS " must land in the same 5-minute bucket, and
filler phrases like "til" or "new" must never become a trend.
"""

from __future__ import annotations

import re

TOPIC_STOPLIST = {
    "til", "today", "learned", "learn", "year", "years", "people", "new", "says", "said",
    "world", "news", "just", "like", "time", "day", "week", "way", "thing", "man",
    "woman", "life", "make", "use", "used", "first", "one", "two", "reddit", "post",
    "update", "updates", "live", "report", "reports", "according",
}

# Words where a trailing "s" is not a plural.
SINGULAR_EXCEPTIONS = {"news", "series", "species", "physics", "mathematics", "politics", "economics",
                       "covid", "bus", "gas", "chaos", "lens", "bias", "thesis", "crisis", "analysis"}

_WS = re.compile(r"\s+")
_DIGITS = re.compile(r"^\d+$")


def singularize(word: str) -> str:
    if len(word) <= 3 or word in SINGULAR_EXCEPTIONS or word.endswith(("ss", "us", "is")):
        return word
    if word.endswith("ies") and len(word) > 4:
        return word[:-3] + "y"
    if word.endswith("s"):
        return word[:-1]
    return word


def normalize_topic(phrase: str) -> str | None:
    """Return the canonical topic, or None if the phrase is noise."""
    tokens = _WS.sub(" ", phrase.strip().lower()).split(" ")
    tokens = [singularize(t) for t in tokens if t]
    if not tokens or all(t in TOPIC_STOPLIST or _DIGITS.match(t) for t in tokens):
        return None
    topic = " ".join(tokens)
    return topic if len(topic) >= 3 else None


def _overlaps(topic: str, chosen: list[str]) -> bool:
    """True if a single word is part of a phrase already chosen for this post.

    Without this, a post about "medieval library" also counts towards "library",
    and both trend from the same posts.
    """
    if " " in topic:
        return False
    return any(topic in other.split(" ") for other in chosen)


def normalize_keywords(pairs, top_n: int = 5, min_score: float = 0.15) -> tuple[list[str], list[float]]:
    """(phrase, score) pairs from KeyBERT -> parallel topic/score lists, deduplicated.

    KeyBERT returns pairs sorted by score, so the first occurrence of a
    normalised topic carries its best score.
    """
    topics: list[str] = []
    scores: list[float] = []
    for phrase, score in pairs:
        if score < min_score:
            continue
        topic = normalize_topic(phrase)
        if topic is None or topic in topics or _overlaps(topic, topics):
            continue
        topics.append(topic)
        scores.append(round(float(score), 4))
        if len(topics) == top_n:
            break
    return topics, scores
