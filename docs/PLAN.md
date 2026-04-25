- **M0：环境与工程骨架（先做，保证开发可快速测试）**
  - 代码组织：一个 repo（`services/`、`flink-jobs/`、`infra/`、`docs/`、`scripts/`），统一 `make`/`task` 命令入口
  - 本机一键启动：`docker compose` 拉起 Kafka、Flink（JobManager/TaskManager）、向量库（先选一个轻量可替换的）、Postgres、OTel Collector、Grafana/Prometheus（可选）
  - 测试通道打通：`pytest`（Python）+ `mvn test`（Java），再加一套“集成测试”可以在 compose 环境里跑（推荐用 Testcontainers/Compose profile）
  - CI：GitHub Actions（或本地等价）跑 `lint + unit + integration(smoke)`，保证每次改动能快速验证

- **M1：事件主链路（先把“流式事件处理系统”跑通）**
  - Kafka topic 约定 + 事件 schema（JSON/Avro 均可，个人项目建议先 JSON + 明确字段）
  - Flink job（Java）：窗口聚合/去重/事件包生成，输出到 `incident_candidates`
  - 一个最小输出端：把事件包写到 Postgres + 控制台/简单 API，先验证“数据流通”

- **M2：RAG 文档流（体现“实时更新”而不是离线索引）**
  - 文档变更进入 Kafka（模拟 runbook/复盘文档）
  - Flink/Python worker 做分块 + embedding + 写入向量库
  - API 支持按事件包检索证据（先不接 LLM，先把检索质量/可追溯做对）

- **M3：LLM 服务与网关（vLLM + 受控输出）**
  - 本机用 vLLM 起 OpenAI 兼容接口（容器化），网关统一超时/重试/限流
  - 输出强约束：LLM 只产出结构化 JSON（分类、优先级、建议步骤、引用证据 id），避免“泛泛总结”

- **M4：可观测与韧性（OTel + 降级 + Chaos）**
  - OTel trace 覆盖：Flink 处理、检索、LLM 调用、输出落地；并记录 token/latency/cost
  - 降级路径：LLM 或向量库不可用时，回退模板化摘要 + 固定 runbook
  - Chaos：注入 vLLM 延迟/宕机、向量库超时、Kafka 抖动，验收系统仍能出结果（哪怕是降级结果）

- **M5：AWS 可迁移落地（展示能力的“第二运行态”，不影响本机开发）**
  - 设计原则：**所有组件容器化 + 配置从环境变量注入 + 基础设施用 IaC**（`infra/terraform` 或 `infra/cdk`）
  - 映射建议（尽量“对位替换”）：本机 Kafka→AWS MSK；本机 Flink→AWS Kinesis Data Analytics for Apache Flink；本机容器→ECS Fargate（或 EKS）；观测→CloudWatch + ADOT(OTel)；文档/结果存储→RDS(Postgres)
  - 交付物：一套 `infra/aws` 可以一键部署最小栈（先跑通一条链路），并在 README 写清“本机/云上两种启动方式”