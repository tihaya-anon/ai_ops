# Schemas

This folder holds event schemas used across the pipeline.

Start simple (JSON + documentation), then evolve to JSON Schema / Avro/Protobuf when needed.

Suggested subfolders:

- `events/` core Kafka message shapes
- `serving/` API response/request shapes

Conventions used in this repo:

- JSON Schema files end with `.schema.json`
- Versioning: bump suffix `_v1`, `_v2`, ... and keep old versions for replay/backfill
