import json

import pytest

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


def test_no_filler_bigram_dominates_synthetic_titles():
    """Only storylines should repeat often enough to trend, never template text."""
    from collections import Counter

    from sources import STORYLINES

    src = SyntheticSource(["worldnews", "todayilearned"], posts_per_minute=600, hot_share=0.0, seed=2)
    posts = src.poll(60, now=NOW)
    storyline_words = {w for lines in STORYLINES.values() for s in lines for w in s.split()}
    bigrams = Counter()
    for p in posts:
        words = [w.strip(":,").lower() for w in p["title"].split()]
        bigrams.update({" ".join(b) for b in zip(words, words[1:]) if not set(b) & storyline_words})
    top_share = bigrams.most_common(1)[0][1] / len(posts)
    assert top_share < 0.12, bigrams.most_common(3)


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


def test_timed_replay_keeps_real_gaps(tmp_path):
    # Real posts at t=0s, 60s, 120s and a burst of three at 3600s.
    times = [0, 60, 120, 3600, 3601, 3602]
    path = tmp_path / "real.jsonl"
    path.write_text("\n".join(json.dumps({"post_id": f"t3_{i}", "created_utc": 1_717_200_000 + t})
                              for i, t in enumerate(times)))
    src = ReplaySource(str(path), speedup=60)  # one real hour per wall minute
    assert [p["post_id"] for p in src.poll(0, now=NOW)] == ["t3_0"]
    second = src.poll(1, now=NOW + 1)  # replay clock at +60s real
    assert [p["post_id"] for p in second] == ["t3_1"]
    assert second[0]["created_utc"] == pytest.approx(NOW + 1)
    burst = src.poll(59, now=NOW + 60)  # replay clock at +3600s real
    assert [p["post_id"] for p in burst] == ["t3_2", "t3_3"]
    assert burst[0]["created_utc"] == pytest.approx(NOW + 2)  # 120s real -> 2s wall
    assert [p["post_id"] for p in src.poll(1, now=NOW + 61)] == ["t3_4", "t3_5"]


def test_timed_replay_loops_without_overlap(tmp_path):
    path = tmp_path / "real.jsonl"
    path.write_text("\n".join(json.dumps({"post_id": f"t3_{i}", "created_utc": 1000.0 + i * 60}) for i in range(3)))
    src = ReplaySource(str(path), speedup=60)
    ids = [p["post_id"] for t in range(0, 7) for p in src.poll(1, now=NOW + t)]
    assert ids == ["t3_0", "t3_1", "t3_2", "t3_0_r1", "t3_1_r1", "t3_2_r1", "t3_0_r2"]
