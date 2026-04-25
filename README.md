## ai_ops (practice project)

This repo is a personal practice project for building a **Lambda-style AIOps pipeline**:

- Batch layer (Spark) is implemented first for fast iteration.
- Streaming layer and serving layer can be added incrementally later.

Docs:

- `docs/SCENARIO.md` – concrete AIOps scenario
- `docs/ARCHITECTURE.md` – lambda-style architecture

### Local: start Spark

- `make up`
- `make ps`

### Run a batch job

- `make spark-sample dt=2026-04-25`
- `make spark-daily dt=2026-04-25`

Outputs go to `data/lake/analytics/`.
