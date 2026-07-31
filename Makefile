KAFKA_BOOTSTRAP_SERVER := localhost:9092
KAFKA_TOPIC := market.trades.raw
KAFKA_TOPIC_PARTITIONS := 1
KAFKA_TOPIC_REPLICATION_FACTOR := 1
ICEBERG_REST_CONFIG_URL := http://localhost:8181/v1/config
ICEBERG_READY_MAX_ATTEMPTS := 60
CLICKHOUSE_WAIT_TIMEOUT_SECONDS := 120
CLICKHOUSE_WAIT_INTERVAL_SECONDS := 2
SPARK_ICEBERG_TRADE_PACKAGES := org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2,org.apache.hadoop:hadoop-aws:3.4.2,org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0

.PHONY: install-dev test status kafka-up kafka-down kafka-create-topic kafka-describe-topic kafka-consume-one kafka-smoke-publish-one iceberg-up iceberg-down iceberg-ps iceberg-trade-stream iceberg-inspect iceberg-migrate-bronze-quality iceberg-rebuild-silver clickhouse-up clickhouse-wait clickhouse-status clickhouse-stop

install-dev:
	python -m pip install -e ".[dev]"

test:
	python -m pytest

status:
	git status --short

kafka-smoke-publish-one:
	python -m jobs.producer.smoke_publish_one

kafka-up:
	docker compose up -d kafka

kafka-down:
	docker compose down

kafka-create-topic:
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server $(KAFKA_BOOTSTRAP_SERVER) \
		--create \
		--if-not-exists \
		--topic $(KAFKA_TOPIC) \
		--partitions $(KAFKA_TOPIC_PARTITIONS) \
		--replication-factor $(KAFKA_TOPIC_REPLICATION_FACTOR)

kafka-describe-topic:
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server $(KAFKA_BOOTSTRAP_SERVER) \
		--describe \
		--topic $(KAFKA_TOPIC)

kafka-consume-one:
	docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server $(KAFKA_BOOTSTRAP_SERVER) \
		--topic $(KAFKA_TOPIC) \
		--from-beginning \
		--max-messages 1 \
		--timeout-ms 10000

iceberg-up:
	docker compose up -d minio minio-init iceberg-rest
	attempt=1; \
	while ! curl -fsS $(ICEBERG_REST_CONFIG_URL) >/dev/null; do \
		if [ "$$attempt" -ge "$(ICEBERG_READY_MAX_ATTEMPTS)" ]; then \
			docker compose ps minio minio-init iceberg-rest; \
			docker compose logs --tail=80 minio minio-init iceberg-rest; \
			exit 1; \
		fi; \
		attempt=$$((attempt + 1)); \
		sleep 1; \
	done

iceberg-down:
	docker compose stop iceberg-rest minio-init minio
	docker compose rm -f iceberg-rest minio-init minio

iceberg-ps:
	docker compose ps minio minio-init iceberg-rest

clickhouse-up:
	docker compose up -d clickhouse
	@echo "ClickHouse started; persisted data is preserved."

clickhouse-wait:
	@container_id="$$( docker compose ps -q clickhouse )"; \
	diagnostics() { \
		docker compose ps clickhouse; \
		docker compose logs --no-color --tail=100 clickhouse; \
	}; \
	if [ -z "$$container_id" ]; then \
		echo "ClickHouse container not found." >&2; \
		diagnostics; \
		exit 1; \
	fi; \
	deadline=$$(( $$(date +%s) + $(CLICKHOUSE_WAIT_TIMEOUT_SECONDS) )); \
	while :; do \
		health_status="$$(docker inspect --format '{{.State.Health.Status}}' "$$container_id" 2>/dev/null || true)"; \
		case "$$health_status" in \
			healthy) echo "ClickHouse is healthy."; exit 0 ;; \
			unhealthy) echo "ClickHouse became unhealthy." >&2; diagnostics; exit 1 ;; \
			"") echo "ClickHouse container disappeared or has no health status." >&2; diagnostics; exit 1 ;; \
		esac; \
		if [ "$$(date +%s)" -ge "$$deadline" ]; then \
			echo "Timed out waiting 120 seconds for ClickHouse to become healthy." >&2; \
			diagnostics; \
			exit 1; \
		fi; \
		sleep $(CLICKHOUSE_WAIT_INTERVAL_SECONDS); \
	done

clickhouse-status:
	docker compose ps clickhouse

clickhouse-stop:
	docker compose stop clickhouse
	@echo "ClickHouse stopped; container and persisted data are preserved."

iceberg-trade-stream:
	PYTHONPATH=. spark-submit \
		--packages "$(SPARK_ICEBERG_TRADE_PACKAGES)" \
		jobs/streaming/iceberg_trade_streaming_job.py

iceberg-inspect:
	PYTHONPATH=. spark-submit \
		--packages "$(SPARK_ICEBERG_TRADE_PACKAGES)" \
		jobs/streaming/iceberg_inspection.py

iceberg-migrate-bronze-quality:
	PYTHONPATH=. spark-submit \
		--packages "$(SPARK_ICEBERG_TRADE_PACKAGES)" \
		jobs/streaming/iceberg_bronze_migration.py

iceberg-rebuild-silver:
	PYTHONPATH=. spark-submit \
		--packages "$(SPARK_ICEBERG_TRADE_PACKAGES)" \
		jobs/streaming/iceberg_silver.py
