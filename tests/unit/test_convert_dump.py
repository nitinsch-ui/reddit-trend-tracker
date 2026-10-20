import json

import pytest
from convert_dump import convert, main

from validator import validate

T0 = 1_717_200_000  # 2024-06-01 00:00:00 UTC


def submission(i, **overrides):
    obj = {"id": f"abc{i}", "title": f"Story {i} about floods", "selftext": "", "subreddit": "worldnews",
           "author": "someone", "score": 10 + i, "upvote_ratio": 0.9, "num_comments": 3,
           "created_utc": T0 + i * 60, "permalink": f"/r/worldnews/comments/abc{i}/x/",
           "link_flair_text": None, "over_18": False}
    obj.update(overrides)
    return obj


def write_jsonl(path, objs):
    path.write_text("\n".join(json.dumps(o) for o in objs) + "\n")
    return path


def test_maps_to_valid_messages_sorted(tmp_path):
    path = write_jsonl(tmp_path / "dump.jsonl", [submission(2), submission(0), submission(1)])
    posts, stats = convert([path], {"worldnews"}, None, None, None)
    assert [p["post_id"] for p in posts] == ["t3_abc0", "t3_abc1", "t3_abc2"]
    assert stats["written"] == 3
    msg = posts[0]
    assert len(msg) == 13 and msg["author_hash"] != "someone"
    assert msg["url"] == "https://www.reddit.com/r/worldnews/comments/abc0/x/"
    assert validate(msg, ["worldnews"], now=msg["created_utc"]) is None


def test_filters_and_cleans(tmp_path):
    path = write_jsonl(tmp_path / "dump.jsonl", [
        submission(0, subreddit="politics"),
        submission(1, over_18=True),
        submission(2, removed_by_category="moderator"),
        submission(3, title="  "),
        submission(4, upvote_ratio=None),
        submission(5, created_utc=str(T0 + 300), selftext="[removed]", author="[deleted]"),
        submission(5),  # duplicate id
        submission(6, created_utc=T0 + 3 * 86400),
    ])
    posts, stats = convert([path], {"worldnews"}, T0, T0 + 86400, None)
    assert [p["post_id"] for p in posts] == ["t3_abc5"]
    assert posts[0]["body"] == "" and posts[0]["author_hash"] is None
    assert posts[0]["created_utc"] == float(T0 + 300)
    for reason in ("other_subreddit", "nsfw", "removed", "no_title", "no_upvote_ratio", "duplicate", "outside_dates"):
        assert stats[f"skipped_{reason}"] == 1, reason


def test_fill_missing_ratio(tmp_path):
    path = write_jsonl(tmp_path / "dump.jsonl", [submission(0, upvote_ratio=None)])
    posts, _ = convert([path], {"worldnews"}, None, None, 1.0)
    assert posts[0]["upvote_ratio"] == 1.0


def test_reads_zst_and_writes_output(tmp_path, capsys):
    zstandard = pytest.importorskip("zstandard")
    raw = ("\n".join(json.dumps(submission(i)) for i in range(3)) + "\n").encode()
    (tmp_path / "dump.zst").write_bytes(zstandard.ZstdCompressor().compress(raw))
    out = tmp_path / "real_posts.jsonl"
    main([str(tmp_path / "dump.zst"), "--out", str(out), "--since", "2024-06-01", "--until", "2024-06-02"])
    assert len(out.read_text().splitlines()) == 3
    assert "3 posts from 2024-06-01 00:00" in capsys.readouterr().out
