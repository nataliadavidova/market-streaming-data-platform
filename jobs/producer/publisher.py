"""Publish prepared KafkaMessage objects through an injectable Kafka client."""

from collections.abc import Callable
from typing import Protocol

from jobs.producer.kafka import KafkaMessage


KafkaDeliveryCallback = Callable[[object | None, object], None]


class KafkaProducerClient(Protocol):
    def send(
        self,
        topic: str,
        key: bytes,
        value: bytes,
        *,
        on_delivery: KafkaDeliveryCallback | None = None,
    ) -> object:
        ...

    def flush(self, timeout: float | None = None) -> int:
        ...


class KafkaDeliveryError(RuntimeError):
    """Raised when Kafka reports a delivery failure or no delivery result."""


class KafkaPublisher:
    def __init__(self, topic: str, client: KafkaProducerClient) -> None:
        self._topic = topic
        self._client = client

    def publish_message(self, message: KafkaMessage, *, flush: bool = True) -> None:
        delivery_observed = False
        delivery_error: object | None = None

        def record_delivery_result(error: object | None, _message: object) -> None:
            nonlocal delivery_observed, delivery_error
            delivery_observed = True
            delivery_error = error

        send_kwargs = {
            "topic": self._topic,
            "key": message.key.encode("utf-8"),
            "value": message.value.encode("utf-8"),
        }
        if flush:
            self._client.send(
                **send_kwargs,
                on_delivery=record_delivery_result,
            )
            self._client.flush()

            if not delivery_observed:
                raise KafkaDeliveryError(
                    "Kafka delivery callback was not observed for "
                    f"topic={self._topic!r} key={message.key!r}"
                )
            if delivery_error is not None:
                raise KafkaDeliveryError(
                    "Kafka delivery failed for "
                    f"topic={self._topic!r} key={message.key!r}: "
                    f"{delivery_error}"
                )
            return

        self._client.send(**send_kwargs)
