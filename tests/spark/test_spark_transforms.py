"""Spark local-mode tests for the transformation logic (no Kafka, Delta or Postgres)."""

import json
from datetime import datetime

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F

from common import parse_posts, valid_posts_filter
from job2_trend import build_trends
from scoring import trend_score
from text_clean import clean_text, clean_text_column

T0 = 1_760_000_000  # 2025-10-09 08:53:20 UTC


def kafka_rows(spark, values):
    return spark.createDataFrame([
        Row(value=bytearray(v.encode()), topic="reddit.raw.posts", partition=0, offset=i,
            timestamp=datetime(2025, 10, 9), timestampType=0)
        for i, v in enumerate(values)])


def message(post_id="t3_a", **overrides):
    msg = {"post_id": post_id, "title": "Floods hit Kerala", "body": "", "subreddit": "worldnews",
           "author_hash": None, "score": 5, "upvote_ratio": 0.9, "num_comments": 1, "created_utc": float(T0),
           "url": None, "flair": None, "ingested_at": float(T0 + 3), "schema_version": "v1"}
    msg.update(overrides)
    return json.dumps(msg)


def test_parse_posts_types_and_malformed(spark):
    df = parse_posts(kafka_rows(spark, [message(), "{not json", message("t3_b", created_utc=None)]))
    rows = {r["kafka_offset"]: r for r in df.collect()}
    assert rows[0]["post_id"] == "t3_a"
    assert rows[0]["created_utc"] == datetime(2025, 10, 9, 8, 53, 20)
    assert str(rows[0]["created_date"]) == "2025-10-09"
    assert rows[0]["raw_json"].startswith("{")
    assert rows[1]["post_id"] is None
    valid = df.filter(valid_posts_filter()).select("post_id").collect()
    assert [r["post_id"] for r in valid] == ["t3_a"]


def test_spark_and_python_cleaning_agree(spark):
    cases = [("Breaking: Floods hit Kerala!!", "see https://x.y/z now"), ("TIL", None), (None, "Body ONLY www.a.b")]
    df = spark.createDataFrame(cases, "title string, body string")
    got = [r["c"] for r in df.select(clean_text_column(F.col("title"), F.col("body")).alias("c")).collect()]
    assert got == [clean_text(t, b) for t, b in cases]


def processed(spark, rows):
    schema = ("subreddit string, created_utc timestamp, score long, num_comments long, "
              "upvote_ratio double, topics array<string>")
    return spark.createDataFrame(rows, schema)


def test_build_trends_windows_and_scores(spark):
    t = datetime(2025, 10, 9, 9, 1)
    t_next = datetime(2025, 10, 9, 9, 6)
    df = processed(spark, [
        ("worldnews", t, 10, 1, 0.9, ["flood", "kerala"]),
        ("worldnews", t, 20, 2, 0.7, ["flood"]),
        ("worldnews", t_next, 5, 0, 0.4, ["flood"]),
        ("todayilearned", t, 1, 0, 1.0, []),  # no topics -> contributes nothing
    ])
    out = {(r["topic"], r["window_start"]): r for r in build_trends(df, min_posts=2, threshold=1.0).collect()}
    assert len(out) == 3

    flood = out[("flood", datetime(2025, 10, 9, 9, 0))]
    assert flood["post_count"] == 2
    assert flood["window_end"] == datetime(2025, 10, 9, 9, 5)
    assert flood["trend_score"] == pytest.approx(trend_score(2, 30, 0.8))
    assert flood["is_trending"] and not flood["is_controversial"]

    kerala = out[("kerala", datetime(2025, 10, 9, 9, 0))]
    assert kerala["post_count"] == 1 and not kerala["is_trending"]  # min_posts guard

    later = out[("flood", datetime(2025, 10, 9, 9, 5))]
    assert later["is_controversial"]
