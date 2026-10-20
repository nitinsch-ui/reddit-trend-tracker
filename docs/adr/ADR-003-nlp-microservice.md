# ADR-003: KeyBERT as a separate service

**Status:** accepted

## Context
Topic extraction needs a sentence-transformer model plus torch, a few hundred MB
in memory. If it were imported inside Spark, every executor's Python worker
would load its own copy. That is too much on an 8 GB laptop that also runs
Kafka, Spark and Postgres.

## Decision
KeyBERT (`all-MiniLM-L6-v2`, CPU) runs in a FastAPI container. The model is
loaded once in the lifespan hook. Job 1 sends each micro-batch from the driver
as chunked HTTP POSTs to `/extract-topics`.

## Consequences
- One copy of the model, whatever Spark's parallelism.
- The model can be swapped or scaled without touching Spark (`nlp_model_version`
  is stored on every Silver row).
- An HTTP hop and a new failure mode. It is handled with retries and backoff,
  then the batch goes to the DLQ and is recovered by backfill (ADR-002).
- Rejected: spaCy NER (finds entities, not themes) and BERTopic (clusters a
  corpus; this needs per-document tags).
