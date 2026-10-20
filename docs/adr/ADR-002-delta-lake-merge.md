# ADR-002: Delta Lake with insert-only MERGE

**Status:** accepted

## Context
Micro-batches can be retried after a failure, so writes must be idempotent.
Plain Parquet has no transactions, no MERGE, and no time travel for
reprocessing after an NLP model change.

## Decision
Bronze and Silver are Delta tables partitioned by `subreddit` and
`created_date`. Both are written with
`MERGE ... ON t.post_id = s.post_id WHEN NOT MATCHED THEN INSERT *`.

## Consequences
- A retried batch inserts nothing new: the write is idempotent.
- An *insert-only* MERGE only ever adds files. This matters because Job 2 reads
  Silver as a stream, and a Delta streaming source fails on commits that
  rewrite existing files (updates or deletes).
- Bronze keeps `raw_json`, so any post can be re-enriched later (`spark/backfill.py`).
- Correcting an already-enriched row needs a batch rewrite plus a restart of
  Job 2's stream; streaming MERGE updates are deliberately not used.
- Rejected: Iceberg (equally valid; Delta's PySpark integration is simpler here).
