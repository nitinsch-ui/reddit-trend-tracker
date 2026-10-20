import json

from dq_writer import cycle_rows
from producer import Config, dlq_message, partition_for


def test_partition_per_subreddit():
    subs = ["worldnews", "todayilearned"]
    assert partition_for("worldnews", subs) == 0
    assert partition_for("todayilearned", subs) == 1


def test_dlq_message_preserves_payload():
    msg = json.loads(dlq_message("producer", "invalid_upvote_ratio", {"post_id": "t3_x", "upvote_ratio": 2}))
    assert msg["stage"] == "producer"
    assert msg["error_reason"] == "invalid_upvote_ratio"
    assert msg["post_id"] == "t3_x"
    assert json.loads(msg["payload"])["upvote_ratio"] == 2


def test_config_from_env(monkeypatch):
    monkeypatch.setenv("PRODUCER_MODE", "replay")
    monkeypatch.setenv("SUBREDDITS", "WorldNews, todayilearned ,")
    monkeypatch.setenv("POSTGRES_HOST", "pg")
    monkeypatch.setenv("POSTGRES_DB", "trends")
    cfg = Config.from_env()
    assert cfg.mode == "replay"
    assert cfg.subreddits == ["worldnews", "todayilearned"]
    assert "host=pg" in cfg.pg_dsn


def test_cycle_rows_cover_all_five_checks():
    rows = cycle_rows({"timestamp_sanity": 2}, total=10)
    assert len(rows) == 5
    status = {r[1]: r[2] for r in rows}
    assert status["timestamp_sanity"] == "fail"
    assert status["required_fields"] == "pass"
