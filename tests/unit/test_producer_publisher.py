"""Test publishing prepared KafkaMessage objects without requiring Kafka."""

from jobs.producer.kafka import KafkaMessage
from jobs.producer.publisher import KafkaPublisher


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


def test_kafka_publisher_can_skip_flush_explicitly() -> None:
    client = FakeKafkaClient()
    publisher = KafkaPublisher(topic="market.trades.raw", client=client)

    publisher.publish_message(
        KafkaMessage(key="binance:BTCUSDT", value="{}"),
        flush=False,
    )

    assert client.flush_count == 0
