"""Batch backfill: re-run NLP for posts that are in Delta raw but missing from processed.

Use after an NLP outage (nlp_error / nlp_missing_result dead letters) or a
model upgrade. Replaying those messages through Kafka would not work: they are
older than the dedup watermark, so the streaming job would drop them as late.

    make backfill                     # everything missing (stops and restarts spark-job1)
    make backfill SINCE=2026-10-01    # only posts created on/after a date

Backfilled posts land in Silver. Trend windows that already closed in Job 2
are not recomputed, because the watermark has passed them.
"""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import functions as F

from common import PROCESSED_TABLE, RAW_TABLE, build_spark, configure_logging, merge_insert_only
from job1_ingest import enrich_with_topics
from text_clean import clean_text_column

log = logging.getLogger("backfill")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--since", help="YYYY-MM-DD; only posts created on or after this date")
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()

    configure_logging()
    spark = build_spark("reddit-backfill")
    from delta.tables import DeltaTable

    raw = spark.read.format("delta").load(RAW_TABLE)
    if args.since:
        raw = raw.filter(F.col("created_date") >= F.lit(args.since).cast("date"))
    if DeltaTable.isDeltaTable(spark, PROCESSED_TABLE):
        raw = raw.join(spark.read.format("delta").load(PROCESSED_TABLE).select("post_id"), "post_id", "left_anti")

    missing = raw.withColumn("clean_text", clean_text_column(F.col("title"), F.col("body"))).persist()
    post_ids = [r["post_id"] for r in missing.select("post_id").collect()]
    log.info("%d posts to backfill", len(post_ids))
    total = 0
    for start in range(0, len(post_ids), args.batch_size):
        chunk = missing.filter(F.col("post_id").isin(post_ids[start:start + args.batch_size]))
        enriched, _ = enrich_with_topics(spark, chunk)
        total += merge_insert_only(spark, enriched, PROCESSED_TABLE)
        log.info("backfilled %d / %d", total, len(post_ids))
    spark.stop()


if __name__ == "__main__":
    main()
