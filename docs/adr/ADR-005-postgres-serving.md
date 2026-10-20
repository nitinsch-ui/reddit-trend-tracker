# ADR-005: PostgreSQL as the serving layer

**Status:** accepted

## Context
Grafana needs fast, simple time-range queries over a few thousand rows a day.
Querying Delta directly would need a running Spark or Trino.

## Decision
Job 2 upserts one row per `(topic, subreddit, window_start)` into
`topic_trends` with `ON CONFLICT DO UPDATE`. The table is indexed on
`(topic, window_start)`, and Grafana connects as a read-only role.

## Consequences
- Update-mode streaming re-emits a window each time it changes. The upsert
  makes that safe and idempotent.
- Grafana stays usable even when Spark is down (it just shows stale data, which an alert catches).
- At much larger scale this table would need range partitioning, or a move to TimescaleDB or ClickHouse.
