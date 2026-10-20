"""Producer-side validation: data-quality layer 1 of 4.

Every post is checked before it reaches Kafka. A failing post is routed to the
dead-letter topic tagged with the reason, so nothing is silently dropped.
"""

from __future__ import annotations

import time
from typing import Iterable, NamedTuple

REQUIRED_FIELDS = ("post_id", "title", "subreddit", "score", "upvote_ratio", "created_utc")

# The five layer-1 checks, in the order they run.
CHECK_NAMES = (
    "required_fields",
    "subreddit_allowlist",
    "upvote_ratio_range",
    "timestamp_sanity",
    "non_empty_title",
)

MAX_FUTURE_SKEW_SECONDS = 300
MAX_AGE_SECONDS = 7 * 24 * 3600  # older than the raw topic's retention is meaningless


class Failure(NamedTuple):
    check: str
    reason: str


def _is_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def validate(post: dict, allowed_subreddits: Iterable[str], now: float | None = None) -> Failure | None:
    """Return None if the post passes every check, else the first failure."""
    now = time.time() if now is None else now

    for field in REQUIRED_FIELDS:
        if post.get(field) is None:
            return Failure("required_fields", f"missing_field:{field}")

    if post["subreddit"].lower() not in {s.lower() for s in allowed_subreddits}:
        return Failure("subreddit_allowlist", "subreddit_not_allowed")

    ratio = post["upvote_ratio"]
    if not _is_number(ratio) or not 0.0 <= ratio <= 1.0:
        return Failure("upvote_ratio_range", "invalid_upvote_ratio")

    created = post["created_utc"]
    if not _is_number(created) or created > now + MAX_FUTURE_SKEW_SECONDS or created < now - MAX_AGE_SECONDS:
        return Failure("timestamp_sanity", "invalid_timestamp")

    if not str(post["title"]).strip():
        return Failure("non_empty_title", "empty_title")

    return None
