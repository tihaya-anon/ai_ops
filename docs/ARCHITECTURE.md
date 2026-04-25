# Architecture: Lambda-style AIOps pipeline (practice project)

This project models the selected scenario in `docs/SCENARIO.md`, using a **Lambda-like architecture**:

- **Speed layer (streaming)**: near-real-time incident candidates and enrichment.
- **Batch layer (Spark)**: daily/offline recomputation, backfill, and quality analytics.
- **Serving layer**: queryable outputs for dashboards / APIs.

The goal is not to use technologies “because we can”, but because the AIOps domain has:

- High-volume event streams (alerts/logs/traces)
- Strong timeliness requirements (MTTA/MTTR)
- A continuously changing knowledge base (runbooks/postmortems)
- A need for reproducibility and offline evaluation

## Batch layer: what Spark is for (concrete use cases)

Spark batch jobs are responsible for tasks that are **expensive**, **backfill-heavy**, or require **global recomputation**:

1) **Daily incident analytics**
- Aggregate candidates/cases into daily metrics (counts, top entities, severity distribution).
- Produce datasets for dashboards and for tracking key SLO/SLA trends.

2) **Backfill / reprocessing**
- Re-run aggregation logic on historical data after you change event schemas or correlation rules.

3) **Offline evaluation datasets**
- Create labeled/curated datasets from outcomes (e.g., accepted suggestions, resolved incidents).
- Compute retrieval quality metrics offline (topK hit rates) using deterministic snapshots.

4) **Periodic index rebuild (optional)**
- If the RAG corpus changes significantly, batch rebuild embeddings/index snapshots for reproducibility.

## Data layout (local)

All jobs read/write under `data/` (mounted to `/data` in containers):

- `/data/lake/raw/` raw inputs (JSON/Parquet)
- `/data/lake/processed/` processed datasets (Parquet)
- `/data/lake/analytics/` aggregates for dashboards (Parquet)

## “Lambda” reconciliation rule

- Streaming produces **fresh** but possibly approximate results.
- Batch produces **corrected** results for a given time partition (e.g., `dt=2026-04-25`).
- Serving prefers batch outputs when available, else falls back to streaming outputs.
