"""Test publishing prepared KafkaMessage objects without requiring Kafka."""

import pytest

from jobs.producer.kafka import KafkaMessage
from jobs.producer.publisher import KafkaDeliveryError, KafkaPublisher


class FakeKafkaClient:
    def __init__(self) -> None:
        self.sent_messages: list[tuple[str, bytes, bytes]] = []
        self.flush_count = 0
        self.flush_timeouts: list[float | None] = []
        self.delivery_callback = None
        self.delivery_error = None
        self.invoke_delivery_callback = True

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
        self.flush_timeouts.append(timeout)
        if self.invoke_delivery_callback and self.delivery_callback is not None:
            self.delivery_callback(self.delivery_error, object())
        return 0


def test_kafka_publisher_sends_message_to_expected_topic() -> None:
    client = FakeKafkaClient()
    publisher = KafkaPublisher(topic="market.trades.raw", client=client)

    publisher.publish_message(KafkaMessage(key="binance:BTCUSDT", value="{}"))

    assert client.sent_messages[0][0] == "market.trades.raw"


def test_kafka_publisher_encodes_key_and_value_as_utf8_bytes() -> None:
    client = FakeKafkaClient()
    publisher = KafkaPublisher(topic="market.trades.raw", client=client)

    publisher.publish_message(KafkaMessage(key="binance:BTCUSDT", value='{"price":"1"}'))

    _, key, value = client.sent_messages[0]
    assert key == b"binance:BTCUSDT"
    assert value == b'{"price":"1"}'


def test_kafka_publisher_flushes_by_default() -> None:
    client = FakeKafkaClient()
    publisher = KafkaPublisher(topic="market.trades.raw", client=client)

    publisher.publish_message(KafkaMessage(key="binance:BTCUSDT", value="{}"))

    assert client.flush_count == 1
    assert client.flush_timeouts == [None]


def test_kafka_publisher_returns_after_successful_delivery_callback() -> None:
    client = FakeKafkaClient()
    publisher = KafkaPublisher(topic="market.trades.raw", client=client)

    publisher.publish_message(KafkaMessage(key="binance:BTCUSDT", value="{}"))

    assert client.flush_count == 1


def test_kafka_publisher_raises_delivery_error() -> None:
    client = FakeKafkaClient()
    client.delivery_error = RuntimeError("broker rejected message")
    publisher = KafkaPublisher(topic="market.trades.raw", client=client)

    with pytest.raises(KafkaDeliveryError, match="broker rejected message"):
        publisher.publish_message(KafkaMessage(key="binance:BTCUSDT", value="{}"))


def test_kafka_publisher_fails_when_delivery_callback_is_missing() -> None:
    client = FakeKafkaClient()
    client.invoke_delivery_callback = False
    publisher = KafkaPublisher(topic="market.trades.raw", client=client)

    with pytest.raises(KafkaDeliveryError, match="callback was not observed"):
        publisher.publish_message(KafkaMessage(key="binance:BTCUSDT", value="{}"))


def test_kafka_publisher_send_failure_skips_flush() -> None:
    class SendFailureClient(FakeKafkaClient):
        def send(self, topic, key, value, *, on_delivery=None) -> object:
            raise OSError("send failed")

    client = SendFailureClient()
    publisher = KafkaPublisher(topic="market.trades.raw", client=client)

    with pytest.raises(OSError, match="send failed"):
        publisher.publish_message(KafkaMessage(key="binance:BTCUSDT", value="{}"))

    assert client.flush_count == 0


def test_kafka_publisher_can_skip_flush_explicitly() -> None:
    client = FakeKafkaClient()
    publisher = KafkaPublisher(topic="market.trades.raw", client=client)

    publisher.publish_message(
        KafkaMessage(key="binance:BTCUSDT", value="{}"),
        flush=False,
    )

    assert client.flush_count == 0
