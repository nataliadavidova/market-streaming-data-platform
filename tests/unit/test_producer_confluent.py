"""Test the confluent-kafka producer adapter without requiring Kafka."""

from typing import Any

from jobs.producer import confluent
from jobs.producer.confluent import ConfluentKafkaProducerClient, build_kafka_client


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


class FakeConfluentKafkaProducerClient:
    created_with_config: dict[str, Any] | None = None
    creation_count = 0

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.send_count = 0
        self.flush_count = 0
        FakeConfluentKafkaProducerClient.created_with_config = config
        FakeConfluentKafkaProducerClient.creation_count += 1

    def send(self, topic: str, key: bytes, value: bytes) -> object:
        self.send_count += 1
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


def test_build_kafka_client_creates_client_with_bootstrap_servers(monkeypatch) -> None:
    FakeConfluentKafkaProducerClient.creation_count = 0
    monkeypatch.setattr(
        confluent,
        "ConfluentKafkaProducerClient",
        FakeConfluentKafkaProducerClient,
    )

    client = build_kafka_client("broker.example:19092")

    assert isinstance(client, FakeConfluentKafkaProducerClient)
    assert FakeConfluentKafkaProducerClient.created_with_config == {
        "bootstrap.servers": "broker.example:19092"
    }
    assert FakeConfluentKafkaProducerClient.creation_count == 1
    assert client.send_count == 0
    assert client.flush_count == 0
