"""Test the one-event producer smoke entry point without requiring Kafka."""

import inspect

from jobs.producer import smoke_publish_one


class FakeKafkaClient:
    def __init__(self) -> None:
        self.sent_messages: list[tuple[str, bytes, bytes]] = []
        self.flush_count = 0

    def send(self, topic: str, key: bytes, value: bytes) -> object:
        self.sent_messages.append((topic, key, value))
        return object()

    def flush(self) -> object:
        self.flush_count += 1
        return object()


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
