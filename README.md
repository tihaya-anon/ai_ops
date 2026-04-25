## ai_ops (practice project)

This repo is a personal practice project for building a **Lambda-style AIOps pipeline**:

- Batch layer (Spark) is implemented first for fast iteration.
- Streaming layer and serving layer can be added incrementally later.

Docs:

- `docs/SCENARIO.md` – selected scenario
- `docs/ARCHITECTURE.md` – lambda-style architecture
- `docs/PROJECT_STRUCTURE.md` – folder responsibilities

### Local: start Spark

- `make up`
- `make ps`

### Run a batch job

- `make spark-sample dt=2026-04-25`
- `make spark-daily dt=2026-04-25`

Outputs go to `data/lake/analytics/`.

### Troubleshooting (Docker)

- First run may take a while because it needs to pull `bitnami/spark:3.5`.
- Verify containers:
  - `docker ps -a | rg aiops-spark`
  - `make ps`
