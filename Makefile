SPARK_PROJECT ?= aiops-spark
COMPOSE ?= docker compose -p $(SPARK_PROJECT) -f infra/docker/docker-compose.yml
SPARK_MASTER ?= local[*]

.PHONY: up down ps logs spark-sample spark-daily smoke \
	ingest-up ingest-down ingest-ps ingest-logs ingest-init-topics ingest-news ingest-consume-news ingest-smoke \
	stream-up stream-down stream-ps stream-logs stream-build-news-normalize stream-run-news-normalize \
	stream-init-topics stream-consume-news-events stream-consume-event-candidates stream-smoke

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

ingest-init-topics:
	@for i in 1 2 3 4 5 6 7 8 9 10; do \
	  $(INGEST_COMPOSE) exec -T redpanda rpk cluster health >/dev/null 2>&1 && break; \
	  sleep 1; \
	done
	@$(INGEST_COMPOSE) exec -T redpanda rpk topic create -p 1 -r 1 news_events_raw_v1 >/dev/null 2>&1 || true

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
	$(MAKE) ingest-init-topics
	$(MAKE) ingest-news limit=$(or $(limit),10) seed=$(or $(seed),7)
	$(MAKE) ingest-consume-news n=1

# ---- Streaming (Flink) ----

STREAM_PROJECT ?= aiops-stream
STREAM_COMPOSE ?= docker compose -p $(STREAM_PROJECT) -f infra/docker/docker-compose.ingest.yml -f infra/docker/docker-compose.flink.yml

stream-up:
	$(STREAM_COMPOSE) up -d redpanda mock-api flink-jobmanager flink-taskmanager

stream-init-topics:
	@for i in 1 2 3 4 5 6 7 8 9 10; do \
	  $(STREAM_COMPOSE) exec -T redpanda rpk cluster health >/dev/null 2>&1 && break; \
	  sleep 1; \
	done
	@$(STREAM_COMPOSE) exec -T redpanda rpk topic create -p 1 -r 1 news_events_raw_v1 >/dev/null 2>&1 || true
	@$(STREAM_COMPOSE) exec -T redpanda rpk topic create -p 1 -r 1 news_events_v1 >/dev/null 2>&1 || true
	@$(STREAM_COMPOSE) exec -T redpanda rpk topic create -p 1 -r 1 event_candidates_v1 >/dev/null 2>&1 || true

stream-down:
	$(STREAM_COMPOSE) down

stream-ps:
	$(STREAM_COMPOSE) ps

stream-logs:
	$(STREAM_COMPOSE) logs -f --tail=200

# Build Flink job jar (host Maven).
stream-build-news-normalize:
	mvn -q -U -f streaming/flink/jobs/news_normalize_job/pom.xml -DskipTests package

# Submit the job to the running Flink cluster.
stream-run-news-normalize:
	$(STREAM_COMPOSE) exec -T flink-jobmanager flink run -d /opt/flink/usrlib/news_normalize_job.jar \
	  -- --bootstrap redpanda:9092 --input-topic news_events_raw_v1 --output-topic news_events_v1 --candidates-topic event_candidates_v1

# Consume a few normalized events for debugging.
stream-consume-news-events:
	@N=$(or $(n),3); \
	  $(STREAM_COMPOSE) exec -T redpanda rpk topic consume news_events_v1 --offset -$$N -n $$N

# Consume a few event candidates for debugging.
stream-consume-event-candidates:
	@N=$(or $(n),3); \
	  $(STREAM_COMPOSE) exec -T redpanda rpk topic consume event_candidates_v1 --offset -$$N -n $$N

# End-to-end smoke (streaming): up -> ingest some raw -> build jar -> run normalize -> consume output.
stream-smoke:
	$(MAKE) stream-up
	$(MAKE) stream-init-topics
	$(MAKE) stream-build-news-normalize
	$(MAKE) stream-run-news-normalize
	@sleep 3
	COMPOSE_PROFILES=ingest $(STREAM_COMPOSE) run --rm ingest-news --limit $(or $(limit),10) --seed $(or $(seed),7)
	@sleep 3
	$(MAKE) stream-consume-news-events n=1
	$(MAKE) stream-consume-event-candidates n=1
