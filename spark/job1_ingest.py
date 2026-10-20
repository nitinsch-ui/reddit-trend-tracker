"""Spark Job 1: Kafka -> Bronze (raw) and Silver (processed) Delta tables.

Three streaming queries share one driver:

raw_writer   reddit.raw.posts -> Delta raw. Every post as received, plus raw_json
             for audit and reprocessing. Unparseable messages -> DLQ.
nlp_enrich   reddit.raw.posts -> clean text -> watermark dedup -> NLP service ->
             Delta processed (+ reddit.processed.posts). NLP failures -> DLQ.
dlq_sink     reddit.dead.letter -> Postgres dead_letter_events, so Grafana can
             chart error rates.
"""

from __future__ import annotations

import logging
import os
from typing import Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType

from common import (
    CHECKPOINT_ROOT,
    DLQ_SCHEMA,
    DLQ_TOPIC,
    KAFKA_BOOTSTRAP,
    PROCESSED_TABLE,
    PROCESSED_TOPIC,
    RAW_TABLE,
    RAW_TOPIC,
    ProgressReporter,
    build_spark,
    configure_logging,
    ensure_bucket,
    merge_insert_only,
    parse_posts,
    pg_connect,
    read_kafka,
    valid_posts_filter,
    write_dlq,
    write_dq_results,
)
from dq_checks import rate_check
from nlp_client import NlpServiceError, extract_topics
from text_clean import clean_text_column

log = logging.getLogger("job1")

NLP_URL = os.environ.get("NLP_SERVICE_URL", "http://nlp-service:8000/extract-topics")
NLP_CHUNK_SIZE = int(os.environ.get("NLP_CHUNK_SIZE", "100"))
TRIGGER = os.environ.get("JOB1_TRIGGER", "30 seconds")
MAX_OFFSETS = int(os.environ.get("JOB1_MAX_OFFSETS_PER_TRIGGER", "500"))
WATERMARK = os.environ.get("DEDUP_WATERMARK", "10 minutes")
LAYER = "spark_job1"

TOPIC_SCHEMA = StructType([
    StructField("post_id", StringType()),
    StructField("topics", ArrayType(StringType())),
    StructField("topic_scores", ArrayType(DoubleType())),
    StructField("nlp_model_version", StringType()),
])

PROCESSED_COLUMNS = [
    "post_id", "title", "body", "subreddit", "author_hash", "score", "upvote_ratio", "num_comments",
    "created_utc", "url", "flair", "ingested_at", "schema_version", "clean_text", "topics", "topic_scores",
    "nlp_model_version", "processed_at", "created_date",
]


def enrich_with_topics(spark: SparkSession, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Call the NLP service for every row of a (small, bounded) batch.

    Returns (enriched rows, rows the service returned nothing for).
    Raises NlpServiceError if the service is unavailable.
    """
    docs = [{"post_id": r["post_id"], "text": r["clean_text"] or ""}
            for r in df.select("post_id", "clean_text").collect()]
    topic_map = extract_topics(docs, NLP_URL, chunk_size=NLP_CHUNK_SIZE)
    topics_df = spark.createDataFrame(
        [(pid, topics, [float(s) for s in scores], version) for pid, (topics, scores, version) in topic_map.items()],
        schema=TOPIC_SCHEMA,
    )
    enriched = (df.join(topics_df, "post_id", "inner")
                  .withColumn("processed_at", F.current_timestamp())
                  .select(*PROCESSED_COLUMNS))
    missing = df.join(topics_df, "post_id", "left_anti")
    return enriched, missing


def publish_processed(df: DataFrame) -> None:
    (df.select(
        F.col("post_id").alias("key"),
        F.to_json(F.struct("post_id", "subreddit", "title", "topics", "topic_scores", "score", "upvote_ratio",
                           "num_comments", "created_utc", "nlp_model_version")).alias("value"))
       .write.format("kafka")
       .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
       .option("topic", PROCESSED_TOPIC)
       .save())


def start_raw_writer(spark: SparkSession):
    def handle(batch_df: DataFrame, batch_id: int) -> None:
        batch_df = batch_df.persist()
        try:
            total = batch_df.count()
            if total == 0:
                return
            invalid = batch_df.filter(~valid_posts_filter())
            n_invalid = invalid.count()
            if n_invalid:
                write_dlq(invalid, LAYER, "parse_error")
            n_valid = total - n_invalid
            inserted = merge_insert_only(spark, batch_df.filter(valid_posts_filter()), RAW_TABLE) if n_valid else 0
            log.info("raw_writer batch %d: rows=%d invalid=%d inserted=%d", batch_id, total, n_invalid, inserted)
            write_dq_results([
                rate_check(LAYER, "parse_failure_rate", n_invalid, total, 0.05),
                # Re-sent posts (e.g. after a producer restart) are expected and harmless,
                # because MERGE drops them - so this warns rather than fails.
                rate_check(LAYER, "duplicate_rate", n_valid - inserted, n_valid, 0.05, severity="warn"),
            ], batch_id)
        finally:
            batch_df.unpersist()

    return (parse_posts(read_kafka(spark, RAW_TOPIC, MAX_OFFSETS))
            .writeStream.queryName("raw_writer")
            .foreachBatch(handle)
            .option("checkpointLocation", f"{CHECKPOINT_ROOT}/job1_raw")
            .trigger(processingTime=TRIGGER)
            .start())


def start_nlp_enrich(spark: SparkSession):
    def handle(batch_df: DataFrame, batch_id: int) -> None:
        batch_df = batch_df.persist()
        try:
            total = batch_df.count()
            if total == 0:
                return
            try:
                enriched, missing = enrich_with_topics(spark, batch_df)
            except NlpServiceError as exc:
                log.error("nlp_enrich batch %d: %s - sending %d posts to DLQ", batch_id, exc, total)
                write_dlq(batch_df, LAYER, "nlp_error")
                write_dq_results([rate_check(LAYER, "nlp_failure_rate", total, total, 0.10)], batch_id)
                return
            enriched = enriched.persist()
            n_missing = missing.count()
            if n_missing:
                write_dlq(missing, LAYER, "nlp_missing_result")
            inserted = merge_insert_only(spark, enriched, PROCESSED_TABLE)
            publish_processed(enriched)
            enriched.unpersist()
            log.info("nlp_enrich batch %d: rows=%d enriched=%d missing=%d", batch_id, total, inserted, n_missing)
            write_dq_results([rate_check(LAYER, "nlp_failure_rate", n_missing, total, 0.10)], batch_id)
        finally:
            batch_df.unpersist()

    stream = (parse_posts(read_kafka(spark, RAW_TOPIC, MAX_OFFSETS))
              .filter(valid_posts_filter())
              .withColumn("clean_text", clean_text_column(F.col("title"), F.col("body")))
              # Event-time watermark bounds the dedup state; without it Spark
              # would remember every post_id forever.
              .withWatermark("created_utc", WATERMARK)
              .dropDuplicates(["post_id", "created_utc"]))
    return (stream.writeStream.queryName("nlp_enrich")
            .foreachBatch(handle)
            .option("checkpointLocation", f"{CHECKPOINT_ROOT}/job1_enrich")
            .trigger(processingTime=TRIGGER)
            .start())


def start_dlq_sink(spark: SparkSession):
    def handle(batch_df: DataFrame, batch_id: int) -> None:
        rows = batch_df.collect()
        if not rows:
            return
        from psycopg2.extras import execute_values

        conn = pg_connect()
        try:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    "INSERT INTO dead_letter_events (kafka_partition, kafka_offset, kafka_timestamp, stage, "
                    "error_reason, post_id, payload) VALUES %s "
                    "ON CONFLICT (kafka_partition, kafka_offset) DO NOTHING",
                    [(r["kafka_partition"], r["kafka_offset"], r["kafka_timestamp"], r["stage"] or "unknown",
                      r["error_reason"] or "unparseable_dlq_message", r["post_id"], r["payload"]) for r in rows],
                )
        finally:
            conn.close()
        log.info("dlq_sink batch %d: %d dead letters recorded", batch_id, len(rows))

    stream = (read_kafka(spark, DLQ_TOPIC, 1000)
              .select(F.col("value").cast("string").alias("raw"),
                      F.col("partition").alias("kafka_partition"),
                      F.col("offset").alias("kafka_offset"),
                      F.col("timestamp").alias("kafka_timestamp"))
              .withColumn("d", F.from_json("raw", DLQ_SCHEMA))
              .select("kafka_partition", "kafka_offset", "kafka_timestamp", "d.stage", "d.error_reason",
                      "d.post_id", F.coalesce(F.col("d.payload"), F.col("raw")).alias("payload")))
    return (stream.writeStream.queryName("dlq_sink")
            .foreachBatch(handle)
            .option("checkpointLocation", f"{CHECKPOINT_ROOT}/job1_dlq")
            .trigger(processingTime=TRIGGER)
            .start())


def main() -> None:
    configure_logging()
    ensure_bucket()
    spark = build_spark("reddit-job1-ingest")
    queries = [start_raw_writer(spark), start_nlp_enrich(spark), start_dlq_sink(spark)]
    ProgressReporter(queries).start()
    log.info("job1 running: %s -> %s, %s", RAW_TOPIC, RAW_TABLE, PROCESSED_TABLE)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
