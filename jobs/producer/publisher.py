"""Publish prepared KafkaMessage objects through an injectable Kafka client."""

from typing import Protocol

from jobs.producer.kafka import KafkaMessage


class KafkaProducerClient(Protocol):
    def send(self, topic: str, key: bytes, value: bytes) -> object:
        ...

    def flush(self) -> object:
        ...


class KafkaPublisher:
    def __init__(self, topic: str, client: KafkaProducerClient) -> None:
        self._topic = topic
        self._client = client

    def publish_message(self, message: KafkaMessage, *, flush: bool = True) -> None:
        self._client.send(
            self._topic,
            key=message.key.encode("utf-8"),
            value=message.value.encode("utf-8"),
        )
        if flush:
            self._client.flush()
