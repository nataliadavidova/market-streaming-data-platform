KAFKA_BOOTSTRAP_SERVER := localhost:9092
KAFKA_TOPIC := market.trades.raw
KAFKA_TOPIC_PARTITIONS := 1
KAFKA_TOPIC_REPLICATION_FACTOR := 1

.PHONY: install-dev test status kafka-up kafka-down kafka-create-topic kafka-describe-topic kafka-consume-one

install-dev:
	python -m pip install -e ".[dev]"

test:
	python -m pytest

status:
	git status --short

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
