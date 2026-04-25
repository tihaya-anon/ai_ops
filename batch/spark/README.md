# Spark batch layer

This folder contains **Spark batch jobs** that implement the Lambda-style **batch layer** described in `docs/ARCHITECTURE.md`.

## Local run (Docker Compose)

Start Spark:

- `make up`

Run a job (example: daily incident aggregates):

- `make spark-sample dt=2026-04-25`
- `make spark-daily dt=2026-04-25`

Outputs are written under `data/lake/analytics/`.

## Jobs

- `jobs/generate_sample_incidents.py`: writes a tiny `processed/incidents` Parquet partition for quick testing.
- `jobs/daily_incident_aggregates.py`: aggregates incidents per day into analytics tables.
