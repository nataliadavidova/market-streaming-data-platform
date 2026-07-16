"""Adapt confluent-kafka Producer to the producer client protocol."""

from typing import Any

try:
    from confluent_kafka import Producer
except ImportError:  # pragma: no cover - exercised only without installed dependency
    Producer = None  # type: ignore[assignment]


class ConfluentKafkaProducerClient:
    def __init__(self, config: dict[str, Any]) -> None:
        if Producer is None:
            raise ImportError(
                "confluent-kafka is required to use ConfluentKafkaProducerClient"
            )
        self._producer = Producer(config)

    def send(self, topic: str, key: bytes, value: bytes) -> object:
        return self._producer.produce(topic=topic, key=key, value=value)

    def flush(self) -> object:
        return self._producer.flush()


def build_kafka_client(bootstrap_servers: str) -> ConfluentKafkaProducerClient:
    return ConfluentKafkaProducerClient({"bootstrap.servers": bootstrap_servers})
