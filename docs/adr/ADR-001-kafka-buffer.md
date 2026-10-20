# ADR-001: Kafka between the producer and Spark

**Status:** accepted

## Context
Reddit is polled once a minute, but posting rates spike during breaking news,
and downstream stages (NLP especially) can be slow or briefly unavailable.
Having Spark read from the producer directly would tie their lifecycles together.

## Decision
The producer publishes to Kafka (`reddit.raw.posts`). Settings:
- `acks=all` and `enable.idempotence=true`
- `post_id` as the message key
- one partition per subreddit
- 7-day retention

## Consequences
- A burst is absorbed by the log, not by Spark memory.
- Any consumer can restart from its checkpointed offsets, or reprocess the past 7 days.
- Bronze and Silver are written by independent queries with their own offsets.
- One more component to run. KRaft mode removes ZooKeeper to keep it light.
- Rejected: Kinesis (cloud cost, harder to run locally) and direct PRAW → Spark (no durability or replay).
