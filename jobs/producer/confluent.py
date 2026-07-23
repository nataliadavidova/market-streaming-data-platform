"""Adapt confluent-kafka Producer to the producer client protocol."""

from typing import Any

from jobs.producer.publisher import KafkaDeliveryCallback

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

    def send(
        self,
        topic: str,
        key: bytes,
        value: bytes,
        *,
        on_delivery: KafkaDeliveryCallback | None = None,
    ) -> object:
        produce_kwargs = {
            "topic": topic,
            "key": key,
            "value": value,
        }
        if on_delivery is not None:
            produce_kwargs["on_delivery"] = on_delivery

        return self._producer.produce(**produce_kwargs)

    def flush(self, timeout: float | None = None) -> int:
        if timeout is None:
            return self._producer.flush()

        return self._producer.flush(timeout)


def build_kafka_client(bootstrap_servers: str) -> ConfluentKafkaProducerClient:
    return ConfluentKafkaProducerClient({"bootstrap.servers": bootstrap_servers})
