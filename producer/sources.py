"""Post sources for the producer.

live       Reddit API via PRAW (needs OAuth credentials).
synthetic  Generated posts with rotating "breaking" storylines, so the full
           pipeline and dashboard can be demoed without Reddit access.
replay     Re-streams a JSONL capture (e.g. one recorded in live mode),
           re-stamping event times to now.
"""

from __future__ import annotations

import json
import logging
import random
import time
import uuid
from collections import OrderedDict
from pathlib import Path

from message import build_post, from_submission

log = logging.getLogger(__name__)


class SeenIds:
    """Bounded LRU of post ids already sent, so each poll only emits new posts.

    It is lost on restart; downstream dedup (Kafka key + Spark watermark +
    Delta MERGE) covers the re-sends that causes.
    """

    def __init__(self, capacity: int = 20_000):
        self.capacity = capacity
        self._ids: OrderedDict[str, None] = OrderedDict()

    def add(self, post_id: str) -> bool:
        """Record the id; return False if it was already present."""
        if post_id in self._ids:
            self._ids.move_to_end(post_id)
            return False
        self._ids[post_id] = None
        if len(self._ids) > self.capacity:
            self._ids.popitem(last=False)
        return True


class LiveSource:
    def __init__(self, client_id: str, client_secret: str, user_agent: str, subreddits, limit: int = 100):
        import praw

        self.reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
            check_for_async=False,
        )
        self.reddit.read_only = True
        self.subreddits = list(subreddits)
        self.limit = limit
        self.seen = SeenIds()

    def poll(self, elapsed_seconds: float) -> list[dict]:
        posts = []
        for name in self.subreddits:
            for submission in self.reddit.subreddit(name).new(limit=self.limit):
                post = from_submission(submission)
                if self.seen.add(post["post_id"]):
                    posts.append(post)
        return posts


WORLDNEWS_STORYLINES = [
    "monsoon flooding",
    "semiconductor export controls",
    "central bank interest rates",
    "wildfire evacuations",
    "ceasefire negotiations",
    "measles outbreak",
    "space station mission",
    "drought crop failure",
    "election recount",
    "hospital cyberattack",
    "record heatwave",
    "oil price surge",
]
TIL_STORYLINES = [
    "honey bees",
    "roman concrete",
    "octopus intelligence",
    "deep sea volcanoes",
    "medieval libraries",
    "tardigrade survival",
    "ancient egyptian beer",
    "lighthouse keepers",
    "coral reef spawning",
    "printing press",
]
CONTROVERSIAL = {"election recount", "central bank interest rates"}
PLACES = ["Kerala", "Jakarta", "Lagos", "Lima", "Manila", "Nairobi", "Hanoi", "Dhaka", "Quito", "Accra"]

TEMPLATES = {
    "worldnews": [
        "{s} prompts emergency response in {place}",
        "Officials warn {s} could worsen this week",
        "{s}: thousands affected as crisis deepens in {place}",
        "UN calls for urgent action on {s}",
        "Live updates on {s} as {place} braces for impact",
        "Analysts say {s} will shape the coming months",
    ],
    "todayilearned": [
        "TIL that {s} were first documented centuries ago",
        "TIL about the surprising history of {s}",
        "TIL scientists are still debating {s}",
        "TIL {s} played a role in shaping {place}",
    ],
}
STORYLINES = {"worldnews": WORLDNEWS_STORYLINES, "todayilearned": TIL_STORYLINES}


class SyntheticSource:
    """Generates clearly-labelled synthetic posts (flair "synthetic", ids t3_syn*).

    Every `burst_minutes` one storyline per subreddit becomes "hot" and takes
    `hot_share` of that subreddit's posts, which is what makes trends appear.
    """

    def __init__(self, subreddits, posts_per_minute: float = 20, burst_minutes: int = 10,
                 hot_share: float = 0.5, seed: int | None = None):
        self.subreddits = [s for s in subreddits if s in STORYLINES] or ["worldnews"]
        self.posts_per_minute = posts_per_minute
        self.burst_seconds = burst_minutes * 60
        self.hot_share = hot_share
        self.rng = random.Random(seed)
        self._hot: dict[str, str] = {}
        self._hot_since = 0.0
        self._carry = 0.0

    def _rotate_hot(self, now: float) -> None:
        if now - self._hot_since >= self.burst_seconds or not self._hot:
            self._hot = {s: self.rng.choice(STORYLINES[s]) for s in self.subreddits}
            self._hot_since = now
            log.info("synthetic hot storylines: %s", self._hot)

    def _make_post(self, subreddit: str, created_utc: float) -> dict:
        if self.rng.random() < self.hot_share:
            storyline = self._hot[subreddit]
        else:
            storyline = self.rng.choice(STORYLINES[subreddit])
        title = self.rng.choice(TEMPLATES[subreddit]).format(s=storyline, place=self.rng.choice(PLACES))
        title = title[0].upper() + title[1:]
        if storyline in CONTROVERSIAL:
            ratio = round(self.rng.uniform(0.30, 0.60), 2)
        else:
            ratio = round(self.rng.uniform(0.70, 0.99), 2)
        return build_post(
            post_id=f"t3_syn{uuid.UUID(int=self.rng.getrandbits(128)).hex[:10]}",
            title=title,
            subreddit=subreddit,
            score=int(self.rng.paretovariate(1.2) * 5),
            upvote_ratio=ratio,
            num_comments=int(self.rng.paretovariate(1.5) * 3),
            created_utc=created_utc,
            flair="synthetic",
        )

    def poll(self, elapsed_seconds: float, now: float | None = None) -> list[dict]:
        now = time.time() if now is None else now
        self._rotate_hot(now)
        expected = self.posts_per_minute * elapsed_seconds / 60 + self._carry
        count = int(expected)
        self._carry = expected - count
        posts = []
        for _ in range(count):
            created = now - self.rng.uniform(0, max(elapsed_seconds, 1))
            posts.append(self._make_post(self.rng.choice(self.subreddits), created))
        return posts


class ReplaySource:
    """Streams a JSONL file of messages at a fixed rate, looping when exhausted."""

    def __init__(self, path: str, posts_per_minute: float = 20):
        self.lines = [ln for ln in Path(path).read_text(encoding="utf-8").splitlines() if ln.strip()]
        if not self.lines:
            raise ValueError(f"replay file {path} is empty")
        self.posts_per_minute = posts_per_minute
        self._pos = 0
        self._loop = 0
        self._carry = 0.0

    def poll(self, elapsed_seconds: float, now: float | None = None) -> list[dict]:
        now = time.time() if now is None else now
        expected = self.posts_per_minute * elapsed_seconds / 60 + self._carry
        count = int(expected)
        self._carry = expected - count
        posts = []
        for _ in range(count):
            post = json.loads(self.lines[self._pos])
            if self._loop:
                post["post_id"] = f"{post['post_id']}_r{self._loop}"
            post["created_utc"] = now
            post["ingested_at"] = now
            posts.append(post)
            self._pos += 1
            if self._pos == len(self.lines):
                self._pos = 0
                self._loop += 1
        return posts
