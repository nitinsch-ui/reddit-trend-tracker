"""Shared configuration, schemas and sinks for both streaming jobs.

Runs inside apache/spark:3.5.1-python3 (Ubuntu Focal, Python 3.8), so keep
syntax 3.8-compatible.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Iterable, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from dq_checks import CheckResult

log = logging.getLogger(__name__)


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


KAFKA_BOOTSTRAP = _env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
RAW_TOPIC = _env("KAFKA_RAW_TOPIC", "reddit.raw.posts")
PROCESSED_TOPIC = _env("KAFKA_PROCESSED_TOPIC", "reddit.processed.posts")
DLQ_TOPIC = _env("KAFKA_DLQ_TOPIC", "reddit.dead.letter")

S3_ENDPOINT = _env("S3_ENDPOINT", "http://s3:8333")
S3_ACCESS_KEY = _env("S3_ACCESS_KEY", "lakeadmin")
S3_SECRET_KEY = _env("S3_SECRET_KEY", "lakeadmin-secret")
LAKE_BUCKET = _env("LAKE_BUCKET", "reddit-lake")
# s3a://<bucket> by default; set LAKE_ROOT=file:///lake to use a local volume instead.
LAKE_ROOT = _env("LAKE_ROOT", f"s3a://{LAKE_BUCKET}").rstrip("/")
RAW_TABLE = f"{LAKE_ROOT}/raw"
PROCESSED_TABLE = f"{LAKE_ROOT}/processed"
CHECKPOINT_ROOT = _env("CHECKPOINT_ROOT", "/checkpoints").rstrip("/")

PG = {
    "host": _env("POSTGRES_HOST", "postgres"),
    "port": int(_env("POSTGRES_PORT", "5432")),
    "dbname": _env("POSTGRES_DB", "trends"),
    "user": _env("POSTGRES_USER", "trends"),
    "password": _env("POSTGRES_PASSWORD", "trends"),
}

PARTITION_COLUMNS = ["subreddit", "created_date"]

# Explicit schema: inference would block the stream and drift silently.
POST_SCHEMA = StructType([
    StructField("post_id", StringType()),
    StructField("title", StringType()),
    StructField("body", StringType()),
    StructField("subreddit", StringType()),
    StructField("author_hash", StringType()),
    StructField("score", LongType()),
    StructField("upvote_ratio", DoubleType()),
    StructField("num_comments", LongType()),
    StructField("created_utc", DoubleType()),
    StructField("url", StringType()),
    StructField("flair", StringType()),
    StructField("ingested_at", DoubleType()),
    StructField("schema_version", StringType()),
])

DLQ_SCHEMA = StructType([
    StructField("stage", StringType()),
    StructField("error_reason", StringType()),
    StructField("post_id", StringType()),
    StructField("payload", StringType()),
    StructField("failed_at", DoubleType()),
])


def configure_logging() -> None:
    logging.basicConfig(level=_env("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def build_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # Fail fast on a stalled object store instead of Hadoop's defaults (200 s
        # socket timeout x 20 attempts), which can freeze every query for an hour
        # because local[2] has only two task slots to block.
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
        .config("spark.hadoop.fs.s3a.connection.timeout", "30000")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "3")
        .config("spark.hadoop.fs.s3a.retry.limit", "5")
        .config("spark.hadoop.fs.s3a.retry.interval", "1s")
        # Streaming state is tiny; 200 default shuffle partitions would mean 200 state stores per batch.
        .config("spark.sql.shuffle.partitions", _env("SPARK_SHUFFLE_PARTITIONS", "4"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.metricsEnabled", "true")
        # A long-running streaming driver otherwise keeps the UI history of every
        # job, stage and SQL execution in heap (1000 jobs by default) and slowly
        # starves itself of memory.
        .config("spark.ui.retainedJobs", "50")
        .config("spark.ui.retainedStages", "50")
        .config("spark.ui.retainedTasks", "2000")
        .config("spark.sql.ui.retainedExecutions", "20")
        .config("spark.streaming.ui.retainedBatches", "50")
        .config("spark.sql.streaming.ui.retainedQueries", "10")
        .config("spark.sql.streaming.ui.retainedProgressUpdates", "50")
        # The tables are small: rebuilding a Delta snapshot with the default 50
        # partitions launches dozens of empty tasks on every MERGE.
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(_env("SPARK_LOG_LEVEL", "WARN"))
    return spark


def ensure_bucket(attempts: int = 30) -> None:
    """Create the lake bucket if it does not exist (S3A cannot create buckets)."""
    if not LAKE_ROOT.startswith("s3a://"):
        return
    import boto3
    from botocore.exceptions import ClientError, EndpointConnectionError

    s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT, aws_access_key_id=S3_ACCESS_KEY,
                      aws_secret_access_key=S3_SECRET_KEY, region_name="us-east-1")
    for attempt in range(1, attempts + 1):
        try:
            s3.create_bucket(Bucket=LAKE_BUCKET)
            log.info("created bucket %s", LAKE_BUCKET)
            return
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                return
            raise
        except EndpointConnectionError:
            log.info("waiting for object store at %s (%d/%d)", S3_ENDPOINT, attempt, attempts)
            time.sleep(2)
    raise RuntimeError(f"object store {S3_ENDPOINT} unreachable")


def read_kafka(spark: SparkSession, topic: str, max_offsets: int) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", max_offsets)
        # After a long outage, retention may have deleted offsets in the checkpoint;
        # keep running and surface the gap in logs rather than crash-looping.
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_posts(kafka_df: DataFrame) -> DataFrame:
    """Kafka rows -> typed post columns plus raw_json and Kafka coordinates.

    Malformed JSON (PERMISSIVE parsing) comes out with a null post_id.
    """
    return (
        kafka_df.select(
            F.col("value").cast("string").alias("raw_json"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn("p", F.from_json("raw_json", POST_SCHEMA))
        .select("p.*", "raw_json", "kafka_partition", "kafka_offset", "kafka_timestamp")
        .withColumn("created_utc", F.timestamp_seconds("created_utc"))
        .withColumn("ingested_at", F.timestamp_seconds("ingested_at"))
        .withColumn("created_date", F.to_date("created_utc"))
    )


def valid_posts_filter():
    return F.col("post_id").isNotNull() & F.col("created_utc").isNotNull() & F.col("subreddit").isNotNull()


def merge_insert_only(spark: SparkSession, df: DataFrame, path: str) -> int:
    """Idempotent write: insert posts whose post_id is not already in the table.

    Insert-only MERGE only ever adds files, so the table stays a valid
    streaming source for downstream jobs. Returns rows inserted.
    """
    from delta.tables import DeltaTable

    df = df.dropDuplicates(["post_id"])
    if not DeltaTable.isDeltaTable(spark, path):
        df.write.format("delta").partitionBy(*PARTITION_COLUMNS).mode("append").save(path)
        return df.count()
    table = DeltaTable.forPath(spark, path)
    (table.alias("t")
          .merge(df.alias("s"), "t.post_id = s.post_id")
          .whenNotMatchedInsertAll()
          .execute())
    metrics = table.history(1).select("operationMetrics").collect()[0][0] or {}
    return int(metrics.get("numTargetRowsInserted", 0))


def write_dlq(df: DataFrame, stage: str, reason) -> None:
    """Send rows (which must carry post_id and raw_json) to the dead-letter topic."""
    reason_col = reason if not isinstance(reason, str) else F.lit(reason)
    (df.select(
        F.col("post_id").alias("key"),
        F.to_json(F.struct(
            F.lit(stage).alias("stage"),
            reason_col.alias("error_reason"),
            F.col("post_id").alias("post_id"),
            F.col("raw_json").alias("payload"),
            F.unix_timestamp(F.current_timestamp()).cast("double").alias("failed_at"),
        )).alias("value"))
       .write.format("kafka")
       .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
       .option("topic", DLQ_TOPIC)
       .save())


def pg_connect():
    import psycopg2

    conn = psycopg2.connect(connect_timeout=10, **PG)
    conn.autocommit = True
    return conn


def write_dq_results(results: Iterable[Optional[CheckResult]], batch_id: int) -> None:
    rows = [r for r in results if r is not None]
    if not rows:
        return
    from psycopg2.extras import execute_values

    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                "INSERT INTO dq_results (layer, check_name, status, metric_value, threshold, details, batch_id) "
                "VALUES %s",
                [(r.layer, r.check_name, r.status, r.metric_value, r.threshold, r.details, batch_id) for r in rows],
            )
    finally:
        conn.close()
    for r in rows:
        if r.status != "pass":
            log.warning("DQ %s/%s %s: %s (threshold %s) - %s",
                        r.layer, r.check_name, r.status, r.metric_value, r.threshold, r.details)


class ProgressReporter(threading.Thread):
    """Copies each query's micro-batch progress into pipeline_metrics for Grafana.

    This is where the dashboard's throughput and Kafka-lag numbers come from:
    the Kafka source reports maxOffsetsBehindLatest per batch.
    """

    def __init__(self, queries: List, interval: float = 15):
        super().__init__(daemon=True, name="progress-reporter")
        self.queries = queries
        self.interval = interval

    @staticmethod
    def _row(progress: dict) -> tuple:
        sources = progress.get("sources") or [{}]
        lag = (sources[0].get("metrics") or {}).get("maxOffsetsBehindLatest")
        return (
            progress["runId"],
            progress["batchId"],
            progress.get("name"),
            progress["timestamp"],
            int(progress.get("numInputRows") or 0),
            float(progress.get("inputRowsPerSecond") or 0.0),
            float(progress.get("processedRowsPerSecond") or 0.0),
            int(lag) if lag is not None else None,
            int((progress.get("durationMs") or {}).get("triggerExecution") or 0),
        )

    def run(self) -> None:
        from psycopg2.extras import execute_values

        while True:
            time.sleep(self.interval)
            rows = [self._row(p) for q in self.queries for p in q.recentProgress]
            if not rows:
                continue
            try:
                conn = pg_connect()
                try:
                    with conn.cursor() as cur:
                        execute_values(
                            cur,
                            "INSERT INTO pipeline_metrics (run_id, batch_id, query_name, batch_timestamp, "
                            "num_input_rows, input_rows_per_sec, processed_rows_per_sec, "
                            "max_offsets_behind_latest, trigger_duration_ms) VALUES %s "
                            "ON CONFLICT (run_id, batch_id) DO NOTHING",
                            rows,
                        )
                finally:
                    conn.close()
            except Exception as exc:  # noqa: BLE001 - metrics must never kill the job
                log.warning("could not write pipeline_metrics: %s", exc)

