"""Download archived submissions for a few days from the Arctic Shift API.

Arctic Shift (https://github.com/ArthurHeitmann/arctic_shift) is a community
archive of public Reddit data for research. This pages through its search API
politely (one request at a time, with pauses and backoff) and writes raw
Reddit submission objects as JSONL, ready for convert_dump.py.

    python fetch_archive.py --subreddits worldnews,todayilearned --since 2026-10-14 --until 2026-10-16

Posts are archived soon after they are created and refreshed after ~36 hours,
so pick days at least two days old to get settled score and upvote_ratio.
For months of data, download the bulk .zst dumps instead.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

API = "https://arctic-shift.photon-reddit.com/api/posts/search"
USER_AGENT = "reddit-trend-tracker/1.0 (portfolio project; github.com/nitinsch-ui/reddit-trend-tracker)"
PAGE_SIZE = 100


def epoch(day: str) -> int:
    return int(datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())


def fetch_page(session, subreddit: str, after: int, before: int, retries: int = 6) -> list[dict]:
    params = {"subreddit": subreddit, "after": after, "before": before, "limit": PAGE_SIZE, "sort": "asc"}
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(API, params=params, timeout=60)
            body = resp.json()
        except Exception as exc:  # noqa: BLE001 - network/JSON errors are retried
            body, error = None, str(exc)
        else:
            error = body.get("error")
            if resp.status_code == 200 and not error:
                return body.get("data") or []
        wait = min(5 * attempt, 30)
        print(f"  {subreddit}: {error or 'HTTP error'} - retrying in {wait}s ({attempt}/{retries})")
        time.sleep(wait)
    raise RuntimeError(f"giving up on {subreddit} after {retries} attempts")


def fetch(subreddit: str, since: int, until: int, out: Path, pause: float) -> int:
    import requests

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    cursor, total, seen = since, 0, set()
    with out.open("w", encoding="utf-8") as fh:
        while cursor < until:
            page = fetch_page(session, subreddit, cursor, until)
            new = [p for p in page if p["id"] not in seen]
            for post in new:
                seen.add(post["id"])
                fh.write(json.dumps(post) + "\n")
            total += len(new)
            if len(page) < PAGE_SIZE or not new:
                break
            cursor = int(page[-1]["created_utc"])  # inclusive; already-seen ids are skipped
            when = datetime.fromtimestamp(cursor, timezone.utc)
            print(f"  {subreddit}: {total} posts, up to {when:%Y-%m-%d %H:%M} UTC")
            time.sleep(pause)
    return total


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--subreddits", default="worldnews,todayilearned")
    parser.add_argument("--since", required=True, help="YYYY-MM-DD (UTC, inclusive)")
    parser.add_argument("--until", required=True, help="YYYY-MM-DD (UTC, exclusive)")
    parser.add_argument("--out-dir", type=Path, default=Path("data"))
    parser.add_argument("--pause", type=float, default=1.5, help="seconds between requests")
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    since, until = epoch(args.since), epoch(args.until)
    for subreddit in [s.strip() for s in args.subreddits.split(",") if s.strip()]:
        out = args.out_dir / f"{subreddit}_submissions_{args.since}_{args.until}.jsonl"
        print(f"{subreddit}: fetching {args.since} .. {args.until}")
        print(f"{subreddit}: {fetch(subreddit, since, until, out, args.pause)} posts -> {out}")


if __name__ == "__main__":
    main()
