- **M0：环境与工程骨架（先做，保证开发可快速测试）**
  - 文件夹结构对齐 Lambda：`docs/PROJECT_STRUCTURE.md`
  - 本机一键启动依赖：`infra/docker/docker-compose.yml` + `Makefile`
    - Batch（Spark）已完成：`make up` / `make spark-sample dt=...` / `make spark-daily dt=...`
    - Streaming（Flink/Kafka/OTel/DB）后续按 profile 逐步加，避免一次性引入太多组件
  - 测试策略（先轻后重）：
    - Spark job 的 smoke：生成 sample 分区 -> 跑聚合 -> 检查输出分区存在
    - 后续再补单测（Python/Java）与集成测（Compose/Testcontainers）
  - CI（后续）：先跑 smoke + lint，再逐步加 integration

- **M1：事件主链路（Speed layer，先跑通再变强）**
  - 选定主场景：`docs/scenarios/rt_market_intel_copilot.md`
  - Kafka topic + schema 先定“最小可用”（先 JSON 文档化，后续再 Avro/Schema Registry）
    - Raw topic（pre-Flink）：`news_events_raw_v1`
    - Schema（JSON Schema）：`schemas/events/news_raw_envelope_v1.schema.json`
    - 本机启动/验证：`make ingest-smoke limit=10 seed=7`
      - Redpanda 对宿主机暴露端口：Kafka `localhost:19092`，HTTP Proxy `localhost:18082`
  - Flink job（Java）：去重 + 归一化 + 实体/主题抽取（可先规则后模型），输出 `event_candidates`
  - Serving 最小落地：先把 `event_candidates` 写入 Postgres（或文件），提供一个最小查询接口（后续补）

- **M2：RAG 文档流（投研框架/术语表/历史事件，可追溯）**
  - `doc_updates` 文档变更流进入 Kafka（本机先用文件模拟）
  - 分块 + embedding 入库（实时/准实时），保留 chunk 引用 id（可追溯）
  - `event_candidates` 检索证据（先不接 LLM），优先把“证据包”做稳定

- **M3：LLM Copilot（不进关键决策链路）**
  - vLLM 作为高吞吐推理服务（OpenAI 兼容接口）
  - LLM 仅用于 `event_candidates` 的事件卡片生成/影响假设/待验证清单，输出必须结构化 JSON + citations
  - 限流/超时/回退：LLM 挂了也不影响 streaming 产出 `event_candidates`

- **M4：可观测与韧性（像生产一样可解释）**
  - OTel trace 覆盖：Flink、检索、LLM、Serving；并记录 token/latency/cost
  - 分层降级：RAG/LLM 出问题时，回退到模板化解释 + 固定策略条款
  - Chaos：注入延迟/宕机/超时，验收“决策链路不断、解释链路可降级”

- **Backlog：云上形态（AWS）**
  - 先不做，但所有组件保持“容器化 + 配置外置 + 可替换依赖”的形态，为后续迁移留空间
