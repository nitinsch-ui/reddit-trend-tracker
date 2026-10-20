"""Spark Job 2: Delta processed (Silver) -> topic_trends in Postgres (Gold).

Reads the processed table as a stream, explodes topics, aggregates 5-minute
tumbling windows per (topic, subreddit) with a 10-minute watermark, scores
each window and upserts it. Update output mode re-emits a window each time
it changes, and the upsert makes that idempotent.
"""

from __future__ import annotations

import logging
import os
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common import (
    CHECKPOINT_ROOT,
    PROCESSED_TABLE,
    ProgressReporter,
    build_spark,
    configure_logging,
    ensure_bucket,
    pg_connect,
    write_dq_results,
)
from dq_checks import zero_check
from scoring import add_trend_columns

log = logging.getLogger("job2")

WINDOW_MINUTES = int(os.environ.get("TREND_WINDOW_MINUTES", "5"))
WATERMARK = os.environ.get("TREND_WATERMARK", "10 minutes")
MIN_POSTS = int(os.environ.get("TREND_MIN_POSTS", "2"))
THRESHOLD = float(os.environ.get("TREND_THRESHOLD", "1.0"))
TRIGGER = os.environ.get("JOB2_TRIGGER", "1 minute")
LAYER = "spark_job2"

UPSERT_SQL = """
INSERT INTO topic_trends (topic, subreddit, window_start, window_end, post_count, total_score, total_comments,
                          avg_upvote_ratio, velocity, trend_score, is_trending, is_controversial, computed_at)
VALUES %s
ON CONFLICT (topic, subreddit, window_start) DO UPDATE SET
    window_end = EXCLUDED.window_end,
    post_count = EXCLUDED.post_count,
    total_score = EXCLUDED.total_score,
    total_comments = EXCLUDED.total_comments,
    avg_upvote_ratio = EXCLUDED.avg_upvote_ratio,
    velocity = EXCLUDED.velocity,
    trend_score = EXCLUDED.trend_score,
    is_trending = EXCLUDED.is_trending,
    is_controversial = EXCLUDED.is_controversial,
    computed_at = EXCLUDED.computed_at
"""


def build_trends(df: DataFrame, window_minutes: int = WINDOW_MINUTES, watermark: str = WATERMARK,
                 min_posts: int = MIN_POSTS, threshold: float = THRESHOLD) -> DataFrame:
    """Processed posts -> one scored row per (topic, subreddit, window). Works on batch or stream input."""
    exploded = df.select("subreddit", "created_utc", "score", "num_comments", "upvote_ratio",
                         F.explode("topics").alias("topic"))
    aggregated = (
        exploded.withWatermark("created_utc", watermark)
        .groupBy(F.window("created_utc", f"{window_minutes} minutes"), "topic", "subreddit")
        .agg(F.count(F.lit(1)).alias("post_count"),
             F.sum("score").alias("total_score"),
             F.sum("num_comments").alias("total_comments"),
             F.avg("upvote_ratio").alias("avg_upvote_ratio"))
        .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
                "topic", "subreddit", "post_count", "total_score", "total_comments", "avg_upvote_ratio")
    )
    return add_trend_columns(aggregated, window_minutes, min_posts, threshold)


def upsert_trends(batch_df: DataFrame, batch_id: int) -> None:
    rows = batch_df.collect()
    if not rows:
        return
    from psycopg2.extras import execute_values

    window_seconds = WINDOW_MINUTES * 60
    bad_scores = sum(1 for r in rows if r["trend_score"] is None or r["trend_score"] < 0 or r["post_count"] < 1)
    bad_windows = sum(1 for r in rows if (r["window_end"] - r["window_start"]).total_seconds() != window_seconds)
    computed_at = time.strftime("%Y-%m-%d %H:%M:%S+00", time.gmtime())
    values = [(r["topic"], r["subreddit"], r["window_start"], r["window_end"], r["post_count"],
               r["total_score"] or 0, r["total_comments"] or 0, r["avg_upvote_ratio"], r["velocity"],
               r["trend_score"], r["is_trending"], bool(r["is_controversial"]), computed_at) for r in rows]
    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            execute_values(cur, UPSERT_SQL, values)
    finally:
        conn.close()
    log.info("trend_scorer batch %d: upserted %d windows (%d trending)",
             batch_id, len(rows), sum(1 for r in rows if r["is_trending"]))
    write_dq_results([
        zero_check(LAYER, "negative_scores", bad_scores, len(rows)),
        zero_check(LAYER, "window_integrity", bad_windows, len(rows)),
    ], batch_id)


def wait_for_table(spark: SparkSession, path: str, poll_seconds: int = 15) -> None:
    """Job 1 creates the processed table on its first batch."""
    from delta.tables import DeltaTable

    while not DeltaTable.isDeltaTable(spark, path):
        log.info("waiting for Delta table %s", path)
        time.sleep(poll_seconds)


def main() -> None:
    configure_logging()
    ensure_bucket()
    spark = build_spark("reddit-job2-trend")
    wait_for_table(spark, PROCESSED_TABLE)
    stream = spark.readStream.format("delta").option("maxFilesPerTrigger", 200).load(PROCESSED_TABLE)
    query = (build_trends(stream)
             .writeStream.queryName("trend_scorer")
             .outputMode("update")
             .foreachBatch(upsert_trends)
             .option("checkpointLocation", f"{CHECKPOINT_ROOT}/job2")
             .trigger(processingTime=TRIGGER)
             .start())
    ProgressReporter([query]).start()
    log.info("job2 running: %s -> topic_trends (window=%dm, watermark=%s, threshold=%s)",
             PROCESSED_TABLE, WINDOW_MINUTES, WATERMARK, THRESHOLD)
    query.awaitTermination()


if __name__ == "__main__":
    main()
