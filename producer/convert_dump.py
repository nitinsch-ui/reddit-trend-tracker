"""Convert archived Reddit submissions (Pushshift / Arctic Shift dumps) into replay files.

Input: one or more files of Reddit submission objects, one JSON object per line,
as .zst (zstd-compressed NDJSON), .jsonl/.ndjson or .json. Output: a JSONL file
of pipeline messages (the 13-field contract), sorted by created_utc, for
PRODUCER_MODE=replay.

    python convert_dump.py data/worldnews_submissions.zst data/todayilearned_submissions.zst \\
        --since 2024-06-01 --until 2024-06-03 --out data/real_posts.jsonl

Archives are third-party copies of Reddit content: keep them and the output
local (data/*.zst and data/real_*.jsonl are gitignored), never in the repo.
"""

from __future__ import annotations

import argparse
import io
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from message import build_post

DEFAULT_SUBREDDITS = ("worldnews", "todayilearned")
REMOVED_TEXT = {"[removed]", "[deleted]"}


def iter_lines(path: Path) -> Iterator[str]:
    if path.suffix == ".zst":
        try:
            import zstandard
        except ImportError:
            sys.exit("reading .zst needs `pip install zstandard` (it is in the producer image)")
        with path.open("rb") as fh:
            # Pushshift / Arctic Shift dumps are written with a 2 GB window.
            reader = zstandard.ZstdDecompressor(max_window_size=2**31).stream_reader(fh)
            yield from io.TextIOWrapper(reader, encoding="utf-8", errors="replace")
    else:
        with path.open(encoding="utf-8", errors="replace") as fh:
            yield from fh


def parse_date(value: str) -> float:
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()


def to_message(obj: dict, fill_ratio: float | None) -> tuple[dict | None, str]:
    """Map one archived submission to a pipeline message, or (None, skip_reason)."""
    title = (obj.get("title") or "").strip()
    if not title:
        return None, "no_title"
    if obj.get("over_18"):
        return None, "nsfw"
    if obj.get("removed_by_category"):  # removed by mods/admins/spam filter: never really public
        return None, "removed"
    ratio = obj.get("upvote_ratio")
    if ratio is None:
        if fill_ratio is None:
            return None, "no_upvote_ratio"
        ratio = fill_ratio
    try:
        created = float(obj["created_utc"])
    except (KeyError, TypeError, ValueError):
        return None, "bad_created_utc"
    body = obj.get("selftext") or ""
    author = obj.get("author")
    msg = build_post(
        post_id=f"t3_{obj['id']}",
        title=title,
        body="" if body in REMOVED_TEXT else body,
        subreddit=obj.get("subreddit") or "",
        author=None if author in REMOVED_TEXT else author,
        score=int(obj.get("score") or 0),
        upvote_ratio=float(ratio),
        num_comments=int(obj.get("num_comments") or 0),
        created_utc=created,
        url=f"https://www.reddit.com{obj['permalink']}" if obj.get("permalink") else None,
        flair=obj.get("link_flair_text"),
        ingested_at=created,
    )
    return msg, "ok"


def convert(paths: list[Path], subreddits: set[str], since: float | None, until: float | None,
            fill_ratio: float | None) -> tuple[list[dict], Counter]:
    stats: Counter = Counter()
    seen: set[str] = set()
    out: list[dict] = []
    for path in paths:
        for line in iter_lines(path):
            line = line.strip()
            if not line:
                continue
            stats["read"] += 1
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                stats["skipped_bad_json"] += 1
                continue
            if (obj.get("subreddit") or "").lower() not in subreddits:
                stats["skipped_other_subreddit"] += 1
                continue
            created = obj.get("created_utc")
            try:
                created = float(created)
            except (TypeError, ValueError):
                created = None
            if created is not None and ((since and created < since) or (until and created >= until)):
                stats["skipped_outside_dates"] += 1
                continue
            msg, reason = to_message(obj, fill_ratio)
            if msg is None:
                stats[f"skipped_{reason}"] += 1
                continue
            if msg["post_id"] in seen:
                stats["skipped_duplicate"] += 1
                continue
            seen.add(msg["post_id"])
            out.append(msg)
            stats["written"] += 1
    out.sort(key=lambda m: m["created_utc"])
    return out, stats


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("inputs", nargs="+", type=Path, help=".zst / .jsonl / .json submission dumps")
    parser.add_argument("--out", type=Path, default=Path("data/real_posts.jsonl"))
    parser.add_argument("--subreddits", default=",".join(DEFAULT_SUBREDDITS))
    parser.add_argument("--since", help="YYYY-MM-DD (UTC, inclusive)")
    parser.add_argument("--until", help="YYYY-MM-DD (UTC, exclusive)")
    parser.add_argument("--fill-missing-ratio", type=float, default=None,
                        help="upvote_ratio to use for old posts that lack one (default: skip them)")
    args = parser.parse_args(argv)

    posts, stats = convert(
        args.inputs,
        {s.strip().lower() for s in args.subreddits.split(",") if s.strip()},
        parse_date(args.since) if args.since else None,
        parse_date(args.until) if args.until else None,
        args.fill_missing_ratio,
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", encoding="utf-8") as fh:
        for post in posts:
            fh.write(json.dumps(post) + "\n")

    for key, value in sorted(stats.items()):
        print(f"{key:28} {value}")
    if posts:
        first = datetime.fromtimestamp(posts[0]["created_utc"], timezone.utc)
        last = datetime.fromtimestamp(posts[-1]["created_utc"], timezone.utc)
        hours = (posts[-1]["created_utc"] - posts[0]["created_utc"]) / 3600
        print(f"\n{len(posts)} posts from {first:%Y-%m-%d %H:%M} to {last:%Y-%m-%d %H:%M} UTC "
              f"({hours:.1f} h of real time; ~{hours:.0f} min to replay at REPLAY_SPEEDUP=60) -> {args.out}")
    else:
        print("\nno posts written - check --subreddits and the date range")


if __name__ == "__main__":
    main()
