"""Test the confluent-kafka producer adapter without requiring Kafka."""

from typing import Any

from jobs.producer import confluent
from jobs.producer.confluent import ConfluentKafkaProducerClient


class FakeConfluentProducer:
    created_with_config: dict[str, Any] | None = None

    def __init__(self, config: dict[str, Any]) -> None:
        self.created_with_config = config
        FakeConfluentProducer.created_with_config = config
        self.produced_messages: list[dict[str, object]] = []
        self.flush_count = 0

    def produce(self, *, topic: str, key: bytes, value: bytes) -> object:
        self.produced_messages.append(
            {
                "topic": topic,
                "key": key,
                "value": value,
            }
        )
        return object()

    def flush(self) -> object:
        self.flush_count += 1
        return object()


def test_confluent_kafka_producer_client_creates_producer_with_config(
    monkeypatch,
) -> None:
    config = {"bootstrap.servers": "localhost:9092"}
    monkeypatch.setattr(confluent, "Producer", FakeConfluentProducer)

    ConfluentKafkaProducerClient(config)

    assert FakeConfluentProducer.created_with_config == config


def test_confluent_kafka_producer_client_delegates_send_to_produce(
    monkeypatch,
) -> None:
    monkeypatch.setattr(confluent, "Producer", FakeConfluentProducer)
    client = ConfluentKafkaProducerClient({"bootstrap.servers": "localhost:9092"})

    client.send(
        topic="market.trades.raw",
        key=b"binance:BTCUSDT",
        value=b'{"price":"1"}',
    )

    assert client._producer.produced_messages == [
        {
            "topic": "market.trades.raw",
            "key": b"binance:BTCUSDT",
            "value": b'{"price":"1"}',
        }
    ]


def test_confluent_kafka_producer_client_delegates_flush(monkeypatch) -> None:
    monkeypatch.setattr(confluent, "Producer", FakeConfluentProducer)
    client = ConfluentKafkaProducerClient({"bootstrap.servers": "localhost:9092"})

    client.flush()

    assert client._producer.flush_count == 1
