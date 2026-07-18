KAFKA_BOOTSTRAP_SERVER := localhost:9092
KAFKA_TOPIC := market.trades.raw
KAFKA_TOPIC_PARTITIONS := 1
KAFKA_TOPIC_REPLICATION_FACTOR := 1
ICEBERG_REST_CONFIG_URL := http://localhost:8181/v1/config
ICEBERG_READY_MAX_ATTEMPTS := 60

.PHONY: install-dev test status kafka-up kafka-down kafka-create-topic kafka-describe-topic kafka-consume-one kafka-smoke-publish-one iceberg-up iceberg-down iceberg-ps

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
