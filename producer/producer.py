"""Reddit producer: poll a source every POLL_INTERVAL_SECONDS, validate, publish to Kafka.

Valid posts go to reddit.raw.posts keyed by post_id, one partition per
subreddit. Invalid posts and failed deliveries go to reddit.dead.letter.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from collections import Counter
from dataclasses import dataclass, field

from dq_writer import DqWriter
from validator import validate

log = logging.getLogger("producer")


@dataclass
class Config:
    mode: str = "synthetic"
    subreddits: list[str] = field(default_factory=lambda: ["worldnews", "todayilearned"])
    poll_interval: float = 60.0
    bootstrap: str = "kafka:9092"
    raw_topic: str = "reddit.raw.posts"
    dlq_topic: str = "reddit.dead.letter"
    posts_per_minute: float = 20.0
    replay_path: str = "/data/sample_posts.jsonl"
    replay_speedup: float = 0.0
    capture_path: str = ""
    pg_dsn: str = ""
    reddit_client_id: str = ""
    reddit_client_secret: str = ""
    reddit_user_agent: str = ""
    max_consecutive_errors: int = 5

    @classmethod
    def from_env(cls) -> Config:
        e = os.environ.get
        pg_dsn = ""
        if e("POSTGRES_HOST"):
            pg_dsn = (f"host={e('POSTGRES_HOST')} port={e('POSTGRES_PORT', '5432')} dbname={e('POSTGRES_DB')} "
                      f"user={e('POSTGRES_USER')} password={e('POSTGRES_PASSWORD')}")
        return cls(
            mode=e("PRODUCER_MODE", "synthetic"),
            subreddits=[s.strip().lower() for s in e("SUBREDDITS", "worldnews,todayilearned").split(",") if s.strip()],
            poll_interval=float(e("POLL_INTERVAL_SECONDS", "60")),
            bootstrap=e("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            raw_topic=e("KAFKA_RAW_TOPIC", "reddit.raw.posts"),
            dlq_topic=e("KAFKA_DLQ_TOPIC", "reddit.dead.letter"),
            posts_per_minute=float(e("SYNTHETIC_POSTS_PER_MINUTE", "20")),
            replay_path=e("REPLAY_PATH", "/data/sample_posts.jsonl"),
            replay_speedup=float(e("REPLAY_SPEEDUP", "0") or 0),
            capture_path=e("CAPTURE_PATH", ""),
            pg_dsn=pg_dsn,
            reddit_client_id=e("REDDIT_CLIENT_ID", ""),
            reddit_client_secret=e("REDDIT_CLIENT_SECRET", ""),
            reddit_user_agent=e("REDDIT_USER_AGENT", "reddit-trend-tracker/1.0"),
        )


def build_source(cfg: Config):
    from sources import LiveSource, ReplaySource, SyntheticSource

    if cfg.mode == "live":
        if not (cfg.reddit_client_id and cfg.reddit_client_secret):
            sys.exit("PRODUCER_MODE=live needs REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET in .env")
        return LiveSource(cfg.reddit_client_id, cfg.reddit_client_secret, cfg.reddit_user_agent, cfg.subreddits)
    if cfg.mode == "replay":
        return ReplaySource(cfg.replay_path, cfg.posts_per_minute, cfg.replay_speedup)
    if cfg.mode == "synthetic":
        return SyntheticSource(cfg.subreddits, cfg.posts_per_minute)
    sys.exit(f"unknown PRODUCER_MODE {cfg.mode!r}; use live, synthetic or replay")


def partition_for(subreddit: str, subreddits: list[str], num_partitions: int = 2) -> int:
    """worldnews -> 0, todayilearned -> 1: per-subreddit ordering and isolation."""
    return subreddits.index(subreddit) % num_partitions


def dlq_message(stage: str, reason: str, post: dict) -> str:
    return json.dumps({
        "stage": stage,
        "error_reason": reason,
        "post_id": post.get("post_id"),
        "payload": json.dumps(post),
        "failed_at": time.time(),
    })


class Publisher:
    def __init__(self, cfg: Config):
        from confluent_kafka import Producer

        self.cfg = cfg
        self.producer = Producer({
            "bootstrap.servers": cfg.bootstrap,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 50,
            "compression.type": "lz4",
            "client.id": "reddit-producer",
        })
        self.delivered = 0
        self.failed = 0

    def _on_delivery(self, err, msg) -> None:
        if err is None:
            self.delivered += 1
            return
        self.failed += 1
        log.error("delivery failed for key=%s: %s", msg.key(), err)
        post = json.loads(msg.value())
        self.producer.produce(self.cfg.dlq_topic, key=msg.key(),
                              value=dlq_message("producer", f"kafka_delivery_failed:{err.name()}", post))

    def send(self, post: dict, partition: int) -> None:
        self.producer.produce(self.cfg.raw_topic, key=post["post_id"], value=json.dumps(post),
                              partition=partition, on_delivery=self._on_delivery)
        self.producer.poll(0)

    def send_dlq(self, post: dict, reason: str) -> None:
        self.producer.produce(self.cfg.dlq_topic, key=post.get("post_id"),
                              value=dlq_message("producer", reason, post))
        self.producer.poll(0)

    def flush(self, timeout: float = 30) -> int:
        return self.producer.flush(timeout)


def run(cfg: Config) -> None:
    source = build_source(cfg)
    publisher = Publisher(cfg)
    dq = DqWriter(cfg.pg_dsn) if cfg.pg_dsn else None
    capture = open(cfg.capture_path, "a", encoding="utf-8") if cfg.capture_path else None

    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    log.info("producer started: mode=%s subreddits=%s interval=%ss", cfg.mode, cfg.subreddits, cfg.poll_interval)
    errors = 0
    last_poll = time.time() - cfg.poll_interval
    while not stopping:
        started = time.time()
        try:
            posts = source.poll(started - last_poll)
            errors = 0
        except Exception:  # noqa: BLE001 - Reddit/network errors; retry next cycle
            errors += 1
            log.exception("poll failed (%d consecutive)", errors)
            if errors >= cfg.max_consecutive_errors:
                sys.exit(1)  # let Docker's restart policy take over
            time.sleep(min(cfg.poll_interval, 5 * errors))
            continue
        last_poll = started

        failures: Counter = Counter()
        for post in posts:
            failure = validate(post, cfg.subreddits)
            if failure:
                failures[failure.check] += 1
                publisher.send_dlq(post, failure.reason)
                continue
            publisher.send(post, partition_for(post["subreddit"], cfg.subreddits))
            if capture:
                capture.write(json.dumps(post) + "\n")
        pending = publisher.flush()
        if capture:
            capture.flush()

        invalid = sum(failures.values())
        log.info("cycle: polled=%d valid=%d invalid=%d undelivered=%d delivered_total=%d",
                 len(posts), len(posts) - invalid, invalid, pending, publisher.delivered)
        if dq:
            dq.write_cycle(dict(failures), len(posts))

        remaining = cfg.poll_interval - (time.time() - started)
        while remaining > 0 and not stopping:
            time.sleep(min(remaining, 1))
            remaining -= 1

    publisher.flush()
    log.info("producer stopped")


if __name__ == "__main__":
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run(Config.from_env())
