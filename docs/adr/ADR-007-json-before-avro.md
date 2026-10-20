# ADR-007: JSON messages now, Avro + Schema Registry later

**Status:** accepted (phase 1)

## Context
Schema Registry catches contract drift when a message is written, but it costs
another JVM (around 400 MB) on a memory-constrained laptop.

## Decision
Phase 1 uses JSON with an explicit `schema_version` field. The contract is
enforced in three places:
- the producer validator
- Spark's explicit `StructType` (no inference)
- the parse-failure DQ check, which routes bad messages to the DLQ

Bronze keeps `raw_json`, so a new upstream field is never lost.

## Consequences
- Drift is caught on read, not on write. That is acceptable at one producer and two consumers.
- Phase 2: move to Avro with Confluent Schema Registry in BACKWARD compatibility
  mode once a second producer or team joins.
