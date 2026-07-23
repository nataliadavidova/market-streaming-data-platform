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
        self.flush_arguments: list[tuple[float, ...]] = []
        self.flush_remaining_messages = 0
        self.flush_error: Exception | None = None

    def produce(
        self,
        *,
        topic: str,
        key: bytes,
        value: bytes,
        on_delivery=None,
    ) -> object:
        produced_message = {
            "topic": topic,
            "key": key,
            "value": value,
        }
        if on_delivery is not None:
            produced_message["on_delivery"] = on_delivery
        self.produced_messages.append(
            produced_message
        )
        return object()

    def flush(self, *args: float) -> int:
        self.flush_count += 1
        self.flush_arguments.append(args)
        if self.flush_error is not None:
            raise self.flush_error
        return self.flush_remaining_messages


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

    def flush(self, timeout: float | None = None) -> int:
        self.flush_count += 1
        return 0


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


def test_confluent_kafka_producer_client_forwards_delivery_callback(
    monkeypatch,
) -> None:
    monkeypatch.setattr(confluent, "Producer", FakeConfluentProducer)
    client = ConfluentKafkaProducerClient({"bootstrap.servers": "localhost:9092"})
    callback = object()

    client.send(
        topic="market.trades.raw",
        key=b"binance:BTCUSDT",
        value=b'{"price":"1"}',
        on_delivery=callback,
    )

    assert client._producer.produced_messages[0]["on_delivery"] is callback


def test_confluent_kafka_producer_client_delegates_flush_without_timeout(
    monkeypatch,
) -> None:
    monkeypatch.setattr(confluent, "Producer", FakeConfluentProducer)
    client = ConfluentKafkaProducerClient({"bootstrap.servers": "localhost:9092"})
    client._producer.flush_remaining_messages = 4

    remaining_messages = client.flush()

    assert client._producer.flush_count == 1
    assert client._producer.flush_arguments == [()]
    assert remaining_messages == 4


def test_confluent_kafka_producer_client_delegates_explicit_none_flush_without_timeout(
    monkeypatch,
) -> None:
    monkeypatch.setattr(confluent, "Producer", FakeConfluentProducer)
    client = ConfluentKafkaProducerClient({"bootstrap.servers": "localhost:9092"})

    client.flush(None)

    assert client._producer.flush_count == 1
    assert client._producer.flush_arguments == [()]


def test_confluent_kafka_producer_client_delegates_flush_timeout(
    monkeypatch,
) -> None:
    monkeypatch.setattr(confluent, "Producer", FakeConfluentProducer)
    client = ConfluentKafkaProducerClient({"bootstrap.servers": "localhost:9092"})
    client._producer.flush_remaining_messages = 2

    remaining_messages = client.flush(5.0)

    assert client._producer.flush_count == 1
    assert client._producer.flush_arguments == [(5.0,)]
    assert remaining_messages == 2


def test_confluent_kafka_producer_client_flush_exception_propagates(
    monkeypatch,
) -> None:
    class FlushError(Exception):
        pass

    monkeypatch.setattr(confluent, "Producer", FakeConfluentProducer)
    client = ConfluentKafkaProducerClient({"bootstrap.servers": "localhost:9092"})
    error = FlushError("flush failed")
    client._producer.flush_error = error

    try:
        client.flush(5.0)
    except FlushError as exc:
        assert exc is error
    else:
        raise AssertionError("flush error did not propagate")


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
