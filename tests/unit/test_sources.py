import json

from sources import ReplaySource, SeenIds, SyntheticSource
from validator import validate

NOW = 1_760_000_000.0


def test_seen_ids_dedups_and_is_bounded():
    seen = SeenIds(capacity=2)
    assert seen.add("a") and seen.add("b")
    assert not seen.add("a")
    assert seen.add("c")  # evicts the least recently used: "b"
    assert seen.add("b")


def test_synthetic_rate_and_validity():
    src = SyntheticSource(["worldnews", "todayilearned"], posts_per_minute=30, seed=1)
    posts = src.poll(60, now=NOW)
    assert len(posts) == 30
    assert all(validate(p, ["worldnews", "todayilearned"], now=NOW) is None for p in posts)
    assert all(p["flair"] == "synthetic" and p["post_id"].startswith("t3_syn") for p in posts)
    assert len({p["post_id"] for p in posts}) == len(posts)


def test_synthetic_rate_carries_fractions():
    src = SyntheticSource(["worldnews"], posts_per_minute=1, seed=1)
    counts = [len(src.poll(30, now=NOW + i * 30)) for i in range(4)]
    assert sum(counts) == 2


def test_synthetic_hot_storyline_dominates():
    src = SyntheticSource(["worldnews"], posts_per_minute=200, hot_share=0.6, seed=3)
    posts = src.poll(60, now=NOW)
    hot = src._hot["worldnews"]
    share = sum(hot in p["title"].lower() for p in posts) / len(posts)
    assert share > 0.5


def test_synthetic_hot_storyline_rotates():
    src = SyntheticSource(["worldnews"], posts_per_minute=10, burst_minutes=10, seed=5)
    picks = set()
    for i in range(10):
        src.poll(60, now=NOW + i * 601)
        picks.add(src._hot["worldnews"])
    assert len(picks) > 1


def test_replay_restamps_and_suffixes_loops(tmp_path):
    path = tmp_path / "posts.jsonl"
    path.write_text("\n".join(json.dumps({"post_id": f"t3_{i}", "created_utc": 1.0}) for i in range(2)))
    src = ReplaySource(str(path), posts_per_minute=4)
    posts = src.poll(60, now=NOW)
    assert [p["post_id"] for p in posts] == ["t3_0", "t3_1", "t3_0_r1", "t3_1_r1"]
    assert all(p["created_utc"] == NOW for p in posts)
