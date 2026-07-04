"""Prepare TradeEvent objects for future Kafka publishing.

This module defines the key/value message contract and does not connect to Kafka.
"""

from dataclasses import dataclass

from jobs.producer.events import TradeEvent


@dataclass(frozen=True)
class KafkaMessage:
    key: str
    value: str


def prepare_trade_event_kafka_message(event: TradeEvent) -> KafkaMessage:
    return KafkaMessage(
        key=f"{event.exchange}:{event.symbol}",
        value=event.to_json_message(),
    )
