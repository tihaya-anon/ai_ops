# Ingest layer (pre-Flink)

Ingest pulls **raw upstream messages** (mock API for local dev), adds **event metadata** (fetch/ingest timestamps, source system, trace IDs), and publishes the envelope to Kafka.

This layer intentionally does **no cleaning**. Cleaning/dedup/normalization will be done in Flink (next step).

## Run (docker)

```bash
make m1-up
make m1-ingest-news limit=20 seed=42
make m1-consume-news n=3
```

