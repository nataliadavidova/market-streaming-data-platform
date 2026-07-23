"""Test the one-event producer smoke entry point without requiring Kafka."""

import inspect

from jobs.producer import smoke_publish_one


class FakeKafkaClient:
    def __init__(self) -> None:
        self.sent_messages: list[tuple[str, bytes, bytes]] = []
        self.flush_count = 0
        self.delivery_callback = None

    def send(
        self,
        topic: str,
        key: bytes,
        value: bytes,
        *,
        on_delivery=None,
    ) -> object:
        self.sent_messages.append((topic, key, value))
        self.delivery_callback = on_delivery
        return object()

    def flush(self, timeout: float | None = None) -> int:
        self.flush_count += 1
        if self.delivery_callback is not None:
            self.delivery_callback(None, object())
        return 0


def test_publish_one_synthetic_trade_event_publishes_one_prepared_event() -> None:
    client = FakeKafkaClient()

    smoke_publish_one.publish_one_synthetic_trade_event(client=client)

    assert client.sent_messages == [
        (
            "market.trades.raw",
            b"binance:BTCUSDT",
            (
                b'{"exchange":"binance","symbol":"BTCUSDT",'
                b'"trade_id":"smoke-test-1","price":"68250.12",'
                b'"quantity":"0.015","event_time_ms":1735689600123,'
                b'"ingested_at_ms":1735689600456}'
            ),
        )
    ]
    assert client.flush_count == 1


def test_publish_one_synthetic_trade_event_uses_expected_topic() -> None:
    client = FakeKafkaClient()

    smoke_publish_one.publish_one_synthetic_trade_event(client=client)

    topic, _, _ = client.sent_messages[0]
    assert topic == "market.trades.raw"


def test_smoke_publish_one_does_not_use_binance_streaming_logic() -> None:
    source = inspect.getsource(smoke_publish_one)

    assert "jobs.producer.binance" not in source
    assert "parse_binance" not in source
    assert "websocket" not in source.lower()


def test_build_local_kafka_client_uses_local_default(monkeypatch) -> None:
    built_with_bootstrap_servers = None
    client = object()

    def fake_build_kafka_client(bootstrap_servers: str) -> object:
        nonlocal built_with_bootstrap_servers
        built_with_bootstrap_servers = bootstrap_servers
        return client

    monkeypatch.setattr(
        smoke_publish_one,
        "build_kafka_client",
        fake_build_kafka_client,
    )

    returned_client = smoke_publish_one.build_local_kafka_client()

    assert returned_client is client
    assert built_with_bootstrap_servers == "localhost:9092"


def test_build_local_kafka_client_forwards_custom_bootstrap_servers(monkeypatch) -> None:
    built_with_bootstrap_servers = None
    client = object()

    def fake_build_kafka_client(bootstrap_servers: str) -> object:
        nonlocal built_with_bootstrap_servers
        built_with_bootstrap_servers = bootstrap_servers
        return client

    monkeypatch.setattr(
        smoke_publish_one,
        "build_kafka_client",
        fake_build_kafka_client,
    )

    returned_client = smoke_publish_one.build_local_kafka_client("custom:9092")

    assert returned_client is client
    assert built_with_bootstrap_servers == "custom:9092"
