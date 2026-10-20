-- Data-quality layer 4 of 4: SQL assertions run every DQ_INTERVAL_SECONDS by dq/run_scheduled.py.
--
-- Blocks start with "-- check: <name>" or "-- maintenance: <name>".
-- A check returns exactly one row: (status, metric_value, threshold, details).

-- check: trend_freshness
-- Spark Job 2 should write at least every 10 minutes; a gap means it stalled or Postgres is unreachable.
SELECT
    CASE WHEN MAX(computed_at) >= NOW() - INTERVAL '10 minutes' THEN 'pass' ELSE 'fail' END,
    COALESCE(ROUND((EXTRACT(EPOCH FROM NOW() - MAX(computed_at)) / 60.0)::numeric, 2), -1),
    10,
    'minutes since the last topic_trends write (-1 = never written)'
FROM topic_trends;

-- check: trending_topics_present
-- Zero trending topics for 30 minutes usually means the threshold is too high for current activity.
SELECT
    CASE WHEN COUNT(DISTINCT topic) > 0 THEN 'pass' ELSE 'warn' END,
    COUNT(DISTINCT topic),
    1,
    'distinct trending topics in the last 30 minutes'
FROM topic_trends
WHERE is_trending AND window_start >= NOW() - INTERVAL '30 minutes';

-- maintenance: retention
DELETE FROM dq_results WHERE checked_at < NOW() - INTERVAL '30 days';
DELETE FROM pipeline_metrics WHERE recorded_at < NOW() - INTERVAL '7 days';
DELETE FROM dead_letter_events WHERE received_at < NOW() - INTERVAL '30 days';
