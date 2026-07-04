"""Test the Kafka message contract helper without requiring Kafka."""

from jobs.producer.events import TradeEvent
from jobs.producer.kafka import KafkaMessage, prepare_trade_event_kafka_message


def valid_trade_event() -> TradeEvent:
    return TradeEvent.model_validate(
        {
            "exchange": "binance",
            "symbol": "BTCUSDT",
            "trade_id": "12345",
            "price": "68250.12",
            "quantity": "0.015",
            "event_time_ms": 1735689600123,
            "ingested_at_ms": 1735689600456,
        }
    )


def test_prepare_trade_event_kafka_message_returns_deterministic_key() -> None:
    message = prepare_trade_event_kafka_message(valid_trade_event())

    assert message.key == "binance:BTCUSDT"


def test_prepare_trade_event_kafka_message_returns_trade_event_json_value() -> None:
    event = valid_trade_event()

    message = prepare_trade_event_kafka_message(event)

    assert isinstance(message, KafkaMessage)
    assert message.value == event.to_json_message()
    assert (
        message.value
        == '{"exchange":"binance","symbol":"BTCUSDT","trade_id":"12345",'
        '"price":"68250.12","quantity":"0.015","event_time_ms":1735689600123,'
        '"ingested_at_ms":1735689600456}'
    )
