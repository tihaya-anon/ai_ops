COMPOSE ?= docker compose -f infra/docker/docker-compose.yml
SPARK_MASTER ?= local[*]

.PHONY: up down ps logs spark-sample spark-daily smoke

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
