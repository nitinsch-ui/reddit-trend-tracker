# ADR-004: Two Spark jobs joined by a Delta table

**Status:** accepted

## Context
One job could read Kafka, enrich, aggregate and write Postgres. But then a
change to the scoring formula, or a slow NLP service, would affect the whole
chain, and both stages would share one checkpoint.

## Decision
- **Job 1** handles Kafka → Bronze/Silver, with a 30 s trigger.
- **Job 2** handles Silver (as a Delta stream) → windowed scores in Postgres, with a 1 min trigger.

Each job has its own checkpoint, and the Silver table is the boundary between them.

## Consequences
- If NLP is down, Job 2 keeps serving scores from data already enriched.
- If Job 2 is down, Job 1 keeps enriching and builds a backlog in Silver that Job 2 catches up on.
- The scoring logic can change (by resetting only Job 2's checkpoint) without re-running NLP.
- Two JVMs to run. Each uses `local[2]` with capped driver memory to fit the laptop budget.
