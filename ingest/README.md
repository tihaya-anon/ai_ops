# Ingest layer (pre-Flink)

Ingest pulls **raw upstream messages** (mock API for local dev), adds **event metadata** (fetch/ingest timestamps, source system, trace IDs), and publishes the envelope to Kafka.

This layer intentionally does **no cleaning**. Cleaning/dedup/normalization will be done in Flink (next step).

## Run (docker)

```bash
make ingest-up
make ingest-news limit=20 seed=42
make ingest-consume-news n=3
```
