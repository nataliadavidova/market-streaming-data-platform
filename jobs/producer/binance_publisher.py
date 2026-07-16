"""Compose Binance trade receiving with Kafka publishing."""

from jobs.producer.binance import BinanceTradeEventReceiver
from jobs.producer.events import TradeEvent
from jobs.producer.kafka import prepare_trade_event_kafka_message
from jobs.producer.publisher import KafkaPublisher


async def receive_and_publish_one_binance_trade(
    receiver: BinanceTradeEventReceiver,
    publisher: KafkaPublisher,
) -> TradeEvent:
    event = await receiver.receive()
    message = prepare_trade_event_kafka_message(event)

    publisher.publish_message(message)

    return event


async def run_binance_trade_publish_loop(
    receiver: BinanceTradeEventReceiver,
    publisher: KafkaPublisher,
) -> None:
    while True:
        await receive_and_publish_one_binance_trade(receiver, publisher)
