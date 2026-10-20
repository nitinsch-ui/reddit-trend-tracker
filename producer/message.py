"""The Kafka message contract for reddit.raw.posts (13 fields, JSON)."""

from __future__ import annotations

import hashlib
import time

SCHEMA_VERSION = "v1"

FIELDS = (
    "post_id",
    "title",
    "body",
    "subreddit",
    "author_hash",
    "score",
    "upvote_ratio",
    "num_comments",
    "created_utc",
    "url",
    "flair",
    "ingested_at",
    "schema_version",
)


def hash_author(name: str | None) -> str | None:
    """Usernames never leave the producer in clear text."""
    if not name:
        return None
    return hashlib.sha256(name.encode("utf-8")).hexdigest()[:16]


def build_post(
    *,
    post_id: str,
    title: str,
    subreddit: str,
    score: int,
    upvote_ratio: float,
    created_utc: float,
    body: str = "",
    author: str | None = None,
    num_comments: int = 0,
    url: str | None = None,
    flair: str | None = None,
    ingested_at: float | None = None,
) -> dict:
    return {
        "post_id": post_id,
        "title": title,
        "body": body or "",
        "subreddit": subreddit.lower(),
        "author_hash": hash_author(author),
        "score": score,
        "upvote_ratio": upvote_ratio,
        "num_comments": num_comments,
        "created_utc": created_utc,
        "url": url,
        "flair": flair,
        "ingested_at": time.time() if ingested_at is None else ingested_at,
        "schema_version": SCHEMA_VERSION,
    }


def from_submission(submission) -> dict:
    """Convert a PRAW Submission into the message dict."""
    return build_post(
        post_id=f"t3_{submission.id}",
        title=submission.title,
        body=submission.selftext,
        subreddit=submission.subreddit.display_name,
        author=submission.author.name if submission.author else None,
        score=int(submission.score),
        upvote_ratio=float(submission.upvote_ratio),
        num_comments=int(submission.num_comments),
        created_utc=float(submission.created_utc),
        url=f"https://www.reddit.com{submission.permalink}",
        flair=submission.link_flair_text,
    )
