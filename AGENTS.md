# Agent instructions (repo-wide)

This file applies to the entire repository.

## Goals

- Prefer **working, testable skeletons** over extensive prose.
- Keep components **replaceable** (local dev today; cloud later). Do not hardcode vendor-specific APIs.
- Optimize for **fast local iteration**: one command to start deps, one command to run a job, one command to run tests.

## Conventions

- Use `snake_case` for Python files and functions.
- Use deterministic outputs for batch jobs (partitioned by date where applicable).
- Configuration comes from **environment variables and CLI flags**, not code constants.
- Keep docs in `docs/` and keep them in sync with runnable commands in `Makefile`.

## Local development

- Infra is started via Docker Compose under `infra/docker/`.
- Data is stored under `data/` and mounted into containers as `/data`.

## Validation

- Prefer quick smoke checks first (e.g. `make smoke`) before broader tests.
- Don’t introduce new tooling unless it’s used immediately by the repo.

