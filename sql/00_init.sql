-- Gold / serving layer for Grafana. Runs once, on first start of the postgres volume.

-- One row per (topic, subreddit, 5-minute window), upserted by Spark Job 2.
CREATE TABLE IF NOT EXISTS topic_trends (
    topic             TEXT             NOT NULL,
    subreddit         TEXT             NOT NULL,
    window_start      TIMESTAMPTZ      NOT NULL,
    window_end        TIMESTAMPTZ      NOT NULL,
    post_count        INTEGER          NOT NULL,
    total_score       BIGINT           NOT NULL DEFAULT 0,
    total_comments    BIGINT           NOT NULL DEFAULT 0,
    avg_upvote_ratio  DOUBLE PRECISION,
    velocity          DOUBLE PRECISION NOT NULL,
    trend_score       DOUBLE PRECISION NOT NULL,
    is_trending       BOOLEAN          NOT NULL DEFAULT FALSE,
    is_controversial  BOOLEAN          NOT NULL DEFAULT FALSE,
    computed_at       TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    PRIMARY KEY (topic, subreddit, window_start)
);
CREATE INDEX IF NOT EXISTS idx_topic_trends_topic_window ON topic_trends (topic, window_start);
CREATE INDEX IF NOT EXISTS idx_topic_trends_window ON topic_trends (window_start DESC);
CREATE INDEX IF NOT EXISTS idx_topic_trends_computed ON topic_trends (computed_at DESC);

-- Results of all 12 checks across the 4 data-quality layers.
CREATE TABLE IF NOT EXISTS dq_results (
    id            BIGSERIAL PRIMARY KEY,
    checked_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    layer         TEXT        NOT NULL,  -- producer | spark_job1 | spark_job2 | scheduled_sql
    check_name    TEXT        NOT NULL,
    status        TEXT        NOT NULL CHECK (status IN ('pass', 'warn', 'fail')),
    metric_value  DOUBLE PRECISION,
    threshold     DOUBLE PRECISION,
    details       TEXT,
    batch_id      BIGINT
);
CREATE INDEX IF NOT EXISTS idx_dq_results_checked ON dq_results (checked_at DESC);
CREATE INDEX IF NOT EXISTS idx_dq_results_check ON dq_results (check_name, checked_at DESC);

-- Mirror of reddit.dead.letter; (partition, offset) makes the sink idempotent.
CREATE TABLE IF NOT EXISTS dead_letter_events (
    id               BIGSERIAL PRIMARY KEY,
    received_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    kafka_partition  INTEGER     NOT NULL,
    kafka_offset     BIGINT      NOT NULL,
    kafka_timestamp  TIMESTAMPTZ,
    stage            TEXT        NOT NULL,
    error_reason     TEXT        NOT NULL,
    post_id          TEXT,
    payload          TEXT,
    UNIQUE (kafka_partition, kafka_offset)
);
CREATE INDEX IF NOT EXISTS idx_dead_letter_received ON dead_letter_events (received_at DESC);

-- Micro-batch progress of every streaming query (throughput, Kafka lag).
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    run_id                     TEXT        NOT NULL,
    batch_id                   BIGINT      NOT NULL,
    query_name                 TEXT,
    batch_timestamp            TIMESTAMPTZ NOT NULL,
    num_input_rows             BIGINT      NOT NULL,
    input_rows_per_sec         DOUBLE PRECISION,
    processed_rows_per_sec     DOUBLE PRECISION,
    max_offsets_behind_latest  BIGINT,
    trigger_duration_ms        BIGINT,
    recorded_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, batch_id)
);
CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_query_time ON pipeline_metrics (query_name, batch_timestamp DESC);
