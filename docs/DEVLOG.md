# 开发记录（Devlog）

> 目的：记录“新增/补齐了什么”，方便回溯项目演进。相比 Changelog，这里更偏开发过程中的持续迭代与里程碑。

---

## 2026-04-25

- 新增项目骨架与文档体系（主场景：实时金融资讯/事件 Copilot）。
- 新增 Batch（Spark）批处理链路与本机 smoke：
  - `make up`
  - `make smoke dt=YYYY-MM-DD`
- 新增 Streaming 的 pre-Flink 最小闭环（Kafka + mock upstream + ingest）：
  - Kafka/Redpanda：`infra/docker/docker-compose.ingest.yml`
  - Mock upstream API（脏数据）：`serving/api/app/main.py`（`localhost:8000`）
  - Ingest（只加元数据，不做清洗）：`ingest/news_ingest.py`
  - 一键验证：`make ingest-smoke limit=10 seed=7`
- 新增 Kafka message schema（JSON Schema，先文档化，后续可迁移到 Avro/Schema Registry）：
  - `schemas/events/news_raw_payload_v1.schema.json`
  - `schemas/events/news_raw_envelope_v1.schema.json`
  - 预留 Flink 输出契约：`schemas/events/news_event_v1.schema.json`
- 新增 Flink 清洗/归一化 job（Streaming 最小闭环）
  - Job：`streaming/flink/jobs/news_normalize_job`
  - 输入：`news_events_raw_v1` -> 输出：`news_events_v1`
  - 一键验证：`make stream-smoke limit=10 seed=7`
