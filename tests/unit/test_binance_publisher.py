"""Test one-event Binance trade publishing composition without services."""

import asyncio

import pytest
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK
from websockets.frames import Close

from jobs.producer.config import ProducerConfig
from jobs.producer.binance_publisher import (
    receive_and_publish_one_binance_trade,
    run_binance_trade_publisher,
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


class FakeWebSocket:
    def __init__(
        self,
        messages: list[str],
        error: Exception | None = None,
        started: asyncio.Event | None = None,
    ) -> None:
        self._messages = messages
        self._error = error
        self._started = started
        self.recv_count = 0
        self.recv_decode_values: list[bool | None] = []

    async def recv(self, decode: bool | None = None) -> str:
        self.recv_count += 1
        self.recv_decode_values.append(decode)
        if self._started is not None:
            self._started.set()
            await asyncio.Event().wait()
            raise AssertionError("receive should be cancelled before returning")
        if self._messages:
            return self._messages.pop(0)
        if self._error is not None:
            raise self._error

        raise AssertionError("test websocket must define messages or error")


class FakeWebSocketContext:
    def __init__(self, websocket: FakeWebSocket) -> None:
        self.websocket = websocket
        self.enter_count = 0
        self.exit_count = 0
        self.exit_exception: tuple[type[BaseException] | None, BaseException | None] | None = None

    async def __aenter__(self) -> FakeWebSocket:
        self.enter_count += 1
        return self.websocket

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback,
    ) -> None:
        self.exit_count += 1
        self.exit_exception = (exc_type, exc)


class FakeWebSocketConnect:
    def __init__(self, context: FakeWebSocketContext) -> None:
        self.context = context
        self.urls: list[str] = []

    def __call__(
        self,
        url: str,
        *,
        close_timeout: float | None,
    ) -> FakeWebSocketContext:
        self.urls.append(url)
        return self.context


class FailingWebSocketContext:
    def __init__(self, error: Exception) -> None:
        self.error = error
        self.enter_count = 0

    async def __aenter__(self):
        self.enter_count += 1
        raise self.error

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        raise AssertionError("a failed enter must not exit the context")


class SequenceWebSocketConnect:
    def __init__(self, contexts: list[object], order: list[str] | None = None) -> None:
        self.contexts = contexts
        self.urls: list[str] = []
        self.order = order

    def __call__(
        self,
        url: str,
        *,
        close_timeout: float | None,
    ) -> object:
        self.urls.append(url)
        if self.order is not None:
            self.order.append("connect")
        return self.contexts.pop(0)


class OrderedWebSocketContext(FakeWebSocketContext):
    def __init__(self, websocket: FakeWebSocket, order: list[str]) -> None:
        super().__init__(websocket)
        self.order = order

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        self.order.append("exit")
        await super().__aexit__(exc_type, exc, traceback)


class FakeKafkaClient:
    def __init__(self) -> None:
        self.sent_messages: list[tuple[str, bytes, bytes]] = []
        self.flush_count = 0

    def send(self, topic: str, key: bytes, value: bytes) -> object:
        self.sent_messages.append((topic, key, value))
        return object()

    def flush(self, timeout: float | None = None) -> int:
        self.flush_count += 1
        return 0


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


def valid_producer_config(symbols: list[str]) -> ProducerConfig:
    return ProducerConfig.model_validate(
        {
            "exchange": "binance",
            "stream": {
                "type": "trades",
                "symbols": symbols,
            },
            "kafka": {
                "raw_topic": "market.trades.raw",
            },
            "producer": {
                "reconnect_delay_seconds": 5,
                "max_reconnect_delay_seconds": 60,
            },
        }
    )


def combined_trade_message(
    *,
    symbol: str,
    trade_id: int,
    price: str,
    quantity: str,
    event_time_ms: int,
) -> str:
    return (
        '{"stream":"'
        f'{symbol.lower()}@trade",'
        '"data":{'
        f'"s":"{symbol}",'
        f'"t":{trade_id},'
        f'"p":"{price}",'
        f'"q":"{quantity}",'
        f'"T":{event_time_ms}'
        "}}"
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


def test_run_binance_trade_publisher_opens_context_and_publishes_until_exception() -> None:
    class StopRuntime(Exception):
        pass

    websocket = FakeWebSocket(
        [
            combined_trade_message(
                symbol="BTCUSDT",
                trade_id=12345,
                price="68250.12",
                quantity="0.015",
                event_time_ms=1735689600123,
            ),
            combined_trade_message(
                symbol="ETHUSDT",
                trade_id=67890,
                price="3420.55",
                quantity="0.25",
                event_time_ms=1735689600345,
            ),
        ],
        error=StopRuntime("stop"),
    )
    context = FakeWebSocketContext(websocket)
    connect = FakeWebSocketConnect(context)
    clock_values = iter([1735689600456, 1735689600789])
    client = FakeKafkaClient()
    publisher = KafkaPublisher(topic="market.trades.raw", client=client)

    with pytest.raises(StopRuntime, match="stop"):
        asyncio.run(
            run_binance_trade_publisher(
                valid_producer_config(["BTCUSDT", "ETHUSDT", "SOLUSDT"]),
                publisher,
                connect=connect,
                clock=lambda: next(clock_values),
            )
        )

    assert connect.urls == [
        "wss://stream.binance.com:9443/stream?"
        "streams=btcusdt@trade/ethusdt@trade/solusdt@trade"
    ]
    assert context.enter_count == 1
    assert context.exit_count == 1
    assert context.exit_exception is not None
    exc_type, exc = context.exit_exception
    assert exc_type is StopRuntime
    assert isinstance(exc, StopRuntime)
    assert str(exc) == "stop"
    assert websocket.recv_count == 3
    assert websocket.recv_decode_values == [True, True, True]
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
        ),
        (
            "market.trades.raw",
            b"binance:ETHUSDT",
            (
                b'{"exchange":"binance","symbol":"ETHUSDT",'
                b'"trade_id":"67890","price":"3420.55",'
                b'"quantity":"0.25","event_time_ms":1735689600345,'
                b'"ingested_at_ms":1735689600789}'
            ),
        ),
    ]
    assert client.flush_count == 2


def test_run_binance_trade_publisher_cancellation_closes_context() -> None:
    started = asyncio.Event()
    websocket = FakeWebSocket([], started=started)
    context = FakeWebSocketContext(websocket)
    connect = FakeWebSocketConnect(context)
    client = FakeKafkaClient()
    publisher = KafkaPublisher(topic="market.trades.raw", client=client)

    async def run_and_cancel() -> None:
        task = asyncio.create_task(
            run_binance_trade_publisher(
                valid_producer_config(["BTCUSDT"]),
                publisher,
                connect=connect,
                clock=lambda: 1735689600456,
            )
        )
        await started.wait()
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run_and_cancel())

    assert connect.urls == [
        "wss://stream.binance.com:9443/stream?streams=btcusdt@trade"
    ]
    assert context.enter_count == 1
    assert context.exit_count == 1
    assert context.exit_exception is not None
    exc_type, exc = context.exit_exception
    assert exc_type is asyncio.CancelledError
    assert isinstance(exc, asyncio.CancelledError)
    assert websocket.recv_count == 1
    assert client.sent_messages == []
    assert client.flush_count == 0


def closed_error() -> ConnectionClosedError:
    return ConnectionClosedError(Close(1011, "transport failed"), None)


def closed_ok() -> ConnectionClosedOK:
    return ConnectionClosedOK(Close(1000, "normal close"), None)


def test_run_binance_trade_publisher_retries_initial_connection_failure() -> None:
    class StopRuntime(Exception):
        pass

    first = FailingWebSocketContext(OSError("connect failed"))
    second = FakeWebSocketContext(
        FakeWebSocket(
            [
                combined_trade_message(
                    symbol="BTCUSDT",
                    trade_id=12345,
                    price="68250.12",
                    quantity="0.015",
                    event_time_ms=1735689600123,
                )
            ],
            error=StopRuntime("stop"),
        )
    )
    connect = SequenceWebSocketConnect([first, second])
    delays: list[float] = []

    async def sleep(delay: float) -> None:
        delays.append(delay)

    with pytest.raises(StopRuntime, match="stop"):
        asyncio.run(
            run_binance_trade_publisher(
                valid_producer_config(["BTCUSDT"]),
                RecordingPublisher(),
                connect=connect,
                sleep=sleep,
            )
        )

    assert first.enter_count == 1
    assert second.enter_count == 1
    assert second.exit_count == 1
    assert delays == [5]


@pytest.mark.parametrize("close_error", [closed_error(), closed_ok()])
def test_run_binance_trade_publisher_reconnects_after_receive_close(
    close_error: Exception,
) -> None:
    class StopRuntime(Exception):
        pass

    first = FakeWebSocketContext(
        FakeWebSocket(
            [
                combined_trade_message(
                    symbol="BTCUSDT",
                    trade_id=12345,
                    price="68250.12",
                    quantity="0.015",
                    event_time_ms=1735689600123,
                )
            ],
            error=close_error,
        )
    )
    second = FakeWebSocketContext(
        FakeWebSocket(
            [
                combined_trade_message(
                    symbol="ETHUSDT",
                    trade_id=67890,
                    price="3420.55",
                    quantity="0.25",
                    event_time_ms=1735689600345,
                )
            ],
            error=StopRuntime("stop"),
        )
    )
    connect = SequenceWebSocketConnect([first, second])
    delays: list[float] = []

    async def sleep(delay: float) -> None:
        delays.append(delay)

    with pytest.raises(StopRuntime, match="stop"):
        asyncio.run(
            run_binance_trade_publisher(
                valid_producer_config(["BTCUSDT", "ETHUSDT"]),
                RecordingPublisher(),
                connect=connect,
                sleep=sleep,
            )
        )

    assert len(connect.urls) == 2
    assert first.exit_count == 1
    assert second.exit_count == 1
    assert delays == [5]


def test_run_binance_trade_publisher_backoff_reaches_configured_cap() -> None:
    class StopReconnect(Exception):
        pass

    contexts = [
        FakeWebSocketContext(FakeWebSocket([], error=closed_error()))
        for _ in range(5)
    ]
    connect = SequenceWebSocketConnect(contexts)
    delays: list[float] = []

    async def sleep(delay: float) -> None:
        delays.append(delay)
        if len(delays) == 5:
            raise StopReconnect("stop retry test")

    with pytest.raises(StopReconnect, match="stop retry test"):
        asyncio.run(
            run_binance_trade_publisher(
                valid_producer_config(["BTCUSDT"]),
                RecordingPublisher(),
                connect=connect,
                sleep=sleep,
            )
        )

    assert delays == [5, 10, 20, 40, 60]
    assert all(context.exit_count == 1 for context in contexts)


def test_run_binance_trade_publisher_backoff_resets_after_first_successful_publish() -> None:
    class StopReconnect(Exception):
        pass

    event_message = combined_trade_message(
        symbol="BTCUSDT",
        trade_id=12345,
        price="68250.12",
        quantity="0.015",
        event_time_ms=1735689600123,
    )
    contexts = [
        FakeWebSocketContext(FakeWebSocket([], error=closed_error())),
        FakeWebSocketContext(FakeWebSocket([], error=closed_error())),
        FakeWebSocketContext(FakeWebSocket([event_message], error=closed_error())),
        FakeWebSocketContext(FakeWebSocket([], error=closed_error())),
    ]
    connect = SequenceWebSocketConnect(contexts)
    delays: list[float] = []

    async def sleep(delay: float) -> None:
        delays.append(delay)
        if len(delays) == 4:
            raise StopReconnect("stop retry test")

    with pytest.raises(StopReconnect, match="stop retry test"):
        asyncio.run(
            run_binance_trade_publisher(
                valid_producer_config(["BTCUSDT"]),
                RecordingPublisher(),
                connect=connect,
                sleep=sleep,
            )
        )

    assert delays == [5, 10, 5, 10]


def test_run_binance_trade_publisher_immediate_close_does_not_reset_backoff() -> None:
    class StopReconnect(Exception):
        pass

    contexts = [
        FakeWebSocketContext(FakeWebSocket([], error=closed_error()))
        for _ in range(3)
    ]
    connect = SequenceWebSocketConnect(contexts)
    delays: list[float] = []

    async def sleep(delay: float) -> None:
        delays.append(delay)
        if len(delays) == 3:
            raise StopReconnect("stop retry test")

    with pytest.raises(StopReconnect, match="stop retry test"):
        asyncio.run(
            run_binance_trade_publisher(
                valid_producer_config(["BTCUSDT"]),
                RecordingPublisher(),
                connect=connect,
                sleep=sleep,
            )
        )

    assert delays == [5, 10, 20]


def test_run_binance_trade_publisher_cancellation_during_backoff_propagates() -> None:
    started = asyncio.Event()
    context = FakeWebSocketContext(FakeWebSocket([], error=closed_error()))
    connect = SequenceWebSocketConnect([context])

    async def sleep(_delay: float) -> None:
        started.set()
        await asyncio.Event().wait()

    async def run_and_cancel() -> None:
        task = asyncio.create_task(
            run_binance_trade_publisher(
                valid_producer_config(["BTCUSDT"]),
                RecordingPublisher(),
                connect=connect,
                sleep=sleep,
            )
        )
        await started.wait()
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run_and_cancel())

    assert len(connect.urls) == 1
    assert context.exit_count == 1


def test_run_binance_trade_publisher_does_not_retry_malformed_message() -> None:
    context = FakeWebSocketContext(FakeWebSocket(["not-json"]))
    connect = SequenceWebSocketConnect([context])

    with pytest.raises(ValueError):
        asyncio.run(
            run_binance_trade_publisher(
                valid_producer_config(["BTCUSDT"]),
                RecordingPublisher(),
                connect=connect,
            )
        )

    assert len(connect.urls) == 1
    assert context.exit_count == 1


def test_run_binance_trade_publisher_does_not_retry_kafka_publisher_error() -> None:
    event_message = combined_trade_message(
        symbol="BTCUSDT",
        trade_id=12345,
        price="68250.12",
        quantity="0.015",
        event_time_ms=1735689600123,
    )

    class OSErrorKafkaClient(FakeKafkaClient):
        def send(self, topic: str, key: bytes, value: bytes) -> object:
            raise OSError("Kafka send failed")

    context = FakeWebSocketContext(FakeWebSocket([event_message]))
    connect = SequenceWebSocketConnect([context])
    publisher = KafkaPublisher(topic="market.trades.raw", client=OSErrorKafkaClient())

    with pytest.raises(OSError, match="Kafka send failed"):
        asyncio.run(
            run_binance_trade_publisher(
                valid_producer_config(["BTCUSDT"]),
                publisher,
                connect=connect,
            )
        )

    assert len(connect.urls) == 1
    assert context.exit_count == 1


def test_run_binance_trade_publisher_closes_context_before_next_connection() -> None:
    class StopRuntime(Exception):
        pass

    order: list[str] = []
    first = OrderedWebSocketContext(FakeWebSocket([], error=closed_error()), order)
    second = OrderedWebSocketContext(
        FakeWebSocket([], error=StopRuntime("stop")),
        order,
    )
    connect = SequenceWebSocketConnect([first, second], order)

    async def sleep(_delay: float) -> None:
        pass

    with pytest.raises(StopRuntime, match="stop"):
        asyncio.run(
            run_binance_trade_publisher(
                valid_producer_config(["BTCUSDT"]),
                RecordingPublisher(),
                connect=connect,
                sleep=sleep,
            )
        )

    assert order == ["connect", "exit", "connect", "exit"]
