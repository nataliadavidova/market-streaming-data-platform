"""Test one-event Binance trade publishing composition without services."""

import asyncio

import pytest

from jobs.producer.binance_publisher import (
    receive_and_publish_one_binance_trade,
    run_binance_trade_publish_loop,
)
from jobs.producer.events import TradeEvent
from jobs.producer.kafka import KafkaMessage
from jobs.producer.publisher import KafkaPublisher


class FakeBinanceTradeEventReceiver:
    def __init__(
        self,
        event: TradeEvent | None = None,
        error: Exception | None = None,
    ) -> None:
        self.event = event
        self.error = error
        self.receive_count = 0

    async def receive(self) -> TradeEvent:
        self.receive_count += 1
        if self.error is not None:
            raise self.error
        if self.event is None:
            raise AssertionError("test receiver must define an event or error")

        return self.event


class SequenceStoppingReceiver:
    def __init__(self, events: list[TradeEvent], error: Exception) -> None:
        self._events = events
        self._error = error
        self.receive_count = 0

    async def receive(self) -> TradeEvent:
        self.receive_count += 1
        if self._events:
            return self._events.pop(0)

        raise self._error


class WaitingReceiver:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.receive_count = 0

    async def receive(self) -> TradeEvent:
        self.receive_count += 1
        self.started.set()
        await asyncio.Event().wait()
        raise AssertionError("receive should be cancelled before returning")


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


class RecordingPublisher:
    def __init__(self) -> None:
        self.messages: list[KafkaMessage] = []

    def publish_message(self, message: KafkaMessage, *, flush: bool = True) -> None:
        self.messages.append(message)


class FailingPublisher:
    def publish_message(self, message: KafkaMessage, *, flush: bool = True) -> None:
        raise RuntimeError("publish failed")


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


def test_receive_and_publish_one_binance_trade_publishes_received_event_once() -> None:
    event = valid_trade_event()
    receiver = FakeBinanceTradeEventReceiver(event=event)
    client = FakeKafkaClient()
    publisher = KafkaPublisher(topic="market.trades.raw", client=client)

    returned_event = asyncio.run(
        receive_and_publish_one_binance_trade(receiver, publisher)
    )

    assert returned_event is event
    assert receiver.receive_count == 1
    assert client.sent_messages == [
        (
            "market.trades.raw",
            b"binance:BTCUSDT",
            (
                b'{"exchange":"binance","symbol":"BTCUSDT",'
                b'"trade_id":"12345","price":"68250.12",'
                b'"quantity":"0.015","event_time_ms":1735689600123,'
                b'"ingested_at_ms":1735689600456}'
            ),
        )
    ]
    assert client.flush_count == 1


def test_receive_and_publish_one_binance_trade_calls_publisher_with_prepared_message() -> None:
    event = valid_trade_event()
    receiver = FakeBinanceTradeEventReceiver(event=event)
    publisher = RecordingPublisher()

    returned_event = asyncio.run(
        receive_and_publish_one_binance_trade(receiver, publisher)
    )

    assert returned_event is event
    assert receiver.receive_count == 1
    assert publisher.messages == [
        KafkaMessage(
            key="binance:BTCUSDT",
            value=event.to_json_message(),
        )
    ]


def test_receive_and_publish_one_binance_trade_receive_error_prevents_publish() -> None:
    receiver = FakeBinanceTradeEventReceiver(error=RuntimeError("receive failed"))
    publisher = RecordingPublisher()

    with pytest.raises(RuntimeError, match="receive failed"):
        asyncio.run(receive_and_publish_one_binance_trade(receiver, publisher))

    assert receiver.receive_count == 1
    assert publisher.messages == []


def test_receive_and_publish_one_binance_trade_publish_error_propagates() -> None:
    receiver = FakeBinanceTradeEventReceiver(event=valid_trade_event())

    with pytest.raises(RuntimeError, match="publish failed"):
        asyncio.run(receive_and_publish_one_binance_trade(receiver, FailingPublisher()))

    assert receiver.receive_count == 1


def test_run_binance_trade_publish_loop_repeats_until_exception() -> None:
    class StopLoop(Exception):
        pass

    first = valid_trade_event()
    second = TradeEvent.model_validate(
        {
            "exchange": "binance",
            "symbol": "ETHUSDT",
            "trade_id": "67890",
            "price": "3420.55",
            "quantity": "0.25",
            "event_time_ms": 1735689600345,
            "ingested_at_ms": 1735689600789,
        }
    )
    receiver = SequenceStoppingReceiver([first, second], StopLoop("stop"))
    publisher = RecordingPublisher()

    with pytest.raises(StopLoop, match="stop"):
        asyncio.run(run_binance_trade_publish_loop(receiver, publisher))

    assert receiver.receive_count == 3
    assert publisher.messages == [
        KafkaMessage(
            key="binance:BTCUSDT",
            value=first.to_json_message(),
        ),
        KafkaMessage(
            key="binance:ETHUSDT",
            value=second.to_json_message(),
        ),
    ]


def test_run_binance_trade_publish_loop_propagates_cancellation() -> None:
    receiver = WaitingReceiver()
    publisher = RecordingPublisher()

    async def run_and_cancel() -> None:
        task = asyncio.create_task(run_binance_trade_publish_loop(receiver, publisher))
        await receiver.started.wait()
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run_and_cancel())

    assert receiver.receive_count == 1
    assert publisher.messages == []
