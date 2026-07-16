"""Compose Binance trade receiving with Kafka publishing."""

from jobs.producer.binance import (
    BinanceTradeEventReceiver,
    open_binance_trade_event_receiver,
)
from jobs.producer.config import ProducerConfig
from jobs.producer.events import TradeEvent
from jobs.producer.kafka import prepare_trade_event_kafka_message
from jobs.producer.publisher import KafkaPublisher
from jobs.producer.websocket import Clock, WebSocketConnect, current_time_ms


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


async def run_binance_trade_publisher(
    config: ProducerConfig,
    publisher: KafkaPublisher,
    *,
    connect: WebSocketConnect | None = None,
    clock: Clock = current_time_ms,
) -> None:
    async with open_binance_trade_event_receiver(
        config,
        connect=connect,
        clock=clock,
    ) as receiver:
        await run_binance_trade_publish_loop(receiver, publisher)
