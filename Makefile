SPARK_PROJECT ?= aiops-spark
COMPOSE ?= docker compose -p $(SPARK_PROJECT) -f infra/docker/docker-compose.yml
SPARK_MASTER ?= local[*]

.PHONY: up down ps logs spark-sample spark-daily smoke \
	ingest-up ingest-down ingest-ps ingest-logs ingest-news ingest-consume-news ingest-smoke

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=200

# Generate a tiny Parquet input partition for quick local validation.
# Usage: make spark-sample dt=2026-04-25
spark-sample:
	@if [ -z "$(dt)" ]; then echo "missing dt=YYYY-MM-DD"; exit 2; fi
	docker exec -i aiops-spark-master spark-submit \
	  --master "$(SPARK_MASTER)" \
	  --conf spark.jars.ivy=/tmp/.ivy2 \
	  /workspace/batch/spark/jobs/generate_sample_incidents.py \
	  --dt $(dt)

# Run Spark batch job inside the master container.
# Usage: make spark-daily dt=2026-04-25
spark-daily:
	@if [ -z "$(dt)" ]; then echo "missing dt=YYYY-MM-DD"; exit 2; fi
	docker exec -i aiops-spark-master spark-submit \
	  --master "$(SPARK_MASTER)" \
	  --conf spark.jars.ivy=/tmp/.ivy2 \
	  /workspace/batch/spark/jobs/daily_incident_aggregates.py \
	  --dt $(dt)

# Quick validation: start deps -> generate sample -> run aggregate -> verify output partition exists.
# Usage: make smoke dt=2026-04-25
smoke:
	@if [ -z "$(dt)" ]; then echo "missing dt=YYYY-MM-DD"; exit 2; fi
	$(MAKE) up
	$(MAKE) spark-sample dt=$(dt)
	$(MAKE) spark-daily dt=$(dt)
	@test -d data/lake/analytics/incident_daily/dt=$(dt)

# ---- M1: Kafka + mock API + ingest (pre-Flink) ----

INGEST_PROJECT ?= aiops-ingest
INGEST_COMPOSE ?= docker compose -p $(INGEST_PROJECT) -f infra/docker/docker-compose.ingest.yml

ingest-up:
	@docker rm -f aiops-redpanda aiops-mock-api >/dev/null 2>&1 || true
	$(INGEST_COMPOSE) up -d redpanda mock-api

ingest-down:
	$(INGEST_COMPOSE) down
	@docker rm -f aiops-redpanda aiops-mock-api >/dev/null 2>&1 || true

ingest-ps:
	$(INGEST_COMPOSE) ps

ingest-logs:
	$(INGEST_COMPOSE) logs -f --tail=200

# Pull raw messages from mock-api, add ingest metadata, and publish to Kafka topic.
# Usage: make ingest-news limit=20 seed=42
ingest-news:
	COMPOSE_PROFILES=ingest $(INGEST_COMPOSE) run --rm ingest-news \
	  --limit $(or $(limit),20) \
	  --seed $(or $(seed),42)

# Quick debug: read a few messages from the raw topic (via rpk in the redpanda container).
# Usage: make ingest-consume-news n=3
ingest-consume-news:
	@N=$(or $(n),3); \
	  $(INGEST_COMPOSE) exec -T redpanda rpk topic consume news_events_raw_v1 --offset -$$N -n $$N

# End-to-end (pre-Flink) smoke: start deps -> publish -> consume.
# Usage: make ingest-smoke limit=10 seed=7
ingest-smoke:
	$(MAKE) ingest-up
	$(MAKE) ingest-news limit=$(or $(limit),10) seed=$(or $(seed),7)
	$(MAKE) ingest-consume-news n=1
