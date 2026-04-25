# Project structure

This is a practice repo. The structure is designed to support:

- Fast local iteration (Docker Compose)
- Clear separation of speed/batch/serving layers (Lambda-like)
- Replaceable components (cloud later, but not required now)

## Top-level

- `batch/` Spark batch layer (backfill, analytics, offline eval)
- `streaming/` Flink streaming layer (real-time features/decisions)
- `serving/` APIs and query layer (dashboards, case review)
- `schemas/` event schemas (JSON Schema / Avro later)
- `infra/` docker/k8s/terraform (local first)
- `data/` local “lake” paths (mounted into containers)
- `docs/` design docs and plans
- `scripts/` helper scripts (local dev, data generation)

## Key subfolders

- `batch/spark/jobs/` PySpark jobs (batch layer)
- `streaming/flink/jobs/` Flink jobs (speed layer; Java planned)
- `infra/docker/` local Docker Compose definitions
- `data/lake/{raw,processed,analytics}/` local datasets and aggregates
- `docs/scenarios/` scenario write-ups (keep more than one)
