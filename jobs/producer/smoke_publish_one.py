"""Publish one synthetic trade event to local Kafka for smoke testing."""

from jobs.producer.confluent import ConfluentKafkaProducerClient, build_kafka_client
from jobs.producer.events import TradeEvent
from jobs.producer.kafka import prepare_trade_event_kafka_message
from jobs.producer.publisher import KafkaProducerClient, KafkaPublisher


DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "market.trades.raw"


def build_synthetic_trade_event() -> TradeEvent:
    return TradeEvent.model_validate(
        {
            "exchange": "binance",
            "symbol": "BTCUSDT",
            "trade_id": "smoke-test-1",
            "price": "68250.12",
            "quantity": "0.015",
            "event_time_ms": 1735689600123,
            "ingested_at_ms": 1735689600456,
        }
    )


def publish_one_synthetic_trade_event(
    *,
    client: KafkaProducerClient,
    topic: str = DEFAULT_TOPIC,
) -> None:
    event = build_synthetic_trade_event()
    message = prepare_trade_event_kafka_message(event)
    publisher = KafkaPublisher(topic=topic, client=client)

    publisher.publish_message(message)


def build_local_kafka_client(
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
) -> ConfluentKafkaProducerClient:
    return build_kafka_client(bootstrap_servers)


def main() -> None:
    publish_one_synthetic_trade_event(client=build_local_kafka_client())


if __name__ == "__main__":
    main()
