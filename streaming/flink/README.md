# Streaming layer (Flink)

This folder contains Flink jobs for near-real-time **news/event normalization**, **dedup**, and **entity extraction** (speed layer).

Batch layer is under `batch/spark/`.

Suggested layout:

- `jobs/` Flink jobs (Java) per pipeline step
- `connectors/` Kafka / DB connectors (if needed)

## Run (local)

```bash
make stream-smoke limit=10 seed=7
```

This will:

- start Redpanda + mock API + Flink cluster
- publish dirty messages to `news_events_raw_v1`
- run `news_normalize_job` to produce `news_events_v1`
