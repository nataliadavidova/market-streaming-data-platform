"""Compose Binance trade receiving with Kafka publishing."""

import asyncio
import logging
from collections.abc import Awaitable, Callable

from jobs.producer.binance import (
    BinanceTradeEventReceiver,
    open_binance_trade_event_receiver,
)
from jobs.producer.config import ProducerConfig
from jobs.producer.events import TradeEvent
from jobs.producer.kafka import prepare_trade_event_kafka_message
from jobs.producer.publisher import KafkaPublisher
from jobs.producer.websocket import Clock, WebSocketConnect, current_time_ms
from websockets.asyncio.client import process_exception as process_websocket_exception
from websockets.exceptions import ConnectionClosed


logger = logging.getLogger(__name__)


class _WebSocketTransportFailure(Exception):
    """Mark a failure raised by the WebSocket receive boundary."""


async def receive_and_publish_one_binance_trade(
    receiver: BinanceTradeEventReceiver,
    publisher: KafkaPublisher,
) -> TradeEvent:
    try:
        event = await receiver.receive()
    except (ConnectionClosed, OSError, asyncio.TimeoutError) as error:
        raise _WebSocketTransportFailure(str(error)) from error
    message = prepare_trade_event_kafka_message(event)

    publisher.publish_message(message)

    return event


async def run_binance_trade_publish_loop(
    receiver: BinanceTradeEventReceiver,
    publisher: KafkaPublisher,
    *,
    on_success: Callable[[TradeEvent], None] | None = None,
) -> None:
    while True:
        event = await receive_and_publish_one_binance_trade(receiver, publisher)
        if on_success is not None:
            on_success(event)


def _is_retryable_connection_establishment_error(error: Exception) -> bool:
    """Use websockets' default retry classification for connection setup."""

    return process_websocket_exception(error) is None


async def run_binance_trade_publisher(
    config: ProducerConfig,
    publisher: KafkaPublisher,
    *,
    connect: WebSocketConnect | None = None,
    clock: Clock = current_time_ms,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    initial_delay = config.producer.reconnect_delay_seconds
    max_delay = config.producer.max_reconnect_delay_seconds
    reconnect_delay = initial_delay
    awaiting_recovery = False

    while True:
        session_entered = False

        def on_success(_event: TradeEvent) -> None:
            nonlocal awaiting_recovery, reconnect_delay
            if awaiting_recovery:
                logger.info(
                    "Binance WebSocket session recovered after first successful publication"
                )
                awaiting_recovery = False
                reconnect_delay = initial_delay

        try:
            async with open_binance_trade_event_receiver(
                config,
                connect=connect,
                clock=clock,
            ) as receiver:
                session_entered = True
                await run_binance_trade_publish_loop(
                    receiver,
                    publisher,
                    on_success=on_success,
                )
        except _WebSocketTransportFailure as error:
            failure = error.__cause__ or error
        except Exception as error:
            if session_entered or not _is_retryable_connection_establishment_error(error):
                raise
            failure = error
        else:
            raise AssertionError("Binance trade publisher session exited unexpectedly")

        awaiting_recovery = True
        logger.warning(
            "Binance WebSocket transport failure; reconnecting in %.1f seconds: %s",
            reconnect_delay,
            failure,
        )
        await sleep(reconnect_delay)
        reconnect_delay = min(reconnect_delay * 2, max_delay)
