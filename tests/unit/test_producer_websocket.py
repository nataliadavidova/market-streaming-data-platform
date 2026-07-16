"""Test receiving one raw WebSocket message without network access."""

import asyncio
import logging

import pytest

from jobs.producer import websocket as websocket_module
from jobs.producer.websocket import (
    ReceivedWebSocketMessage,
    open_websocket_message_receiver,
    receive_one_websocket_message,
)


class FakeWebSocket:
    def __init__(
        self,
        messages: str | list[str],
        events: list[str] | None = None,
    ) -> None:
        self._messages = [messages] if isinstance(messages, str) else messages
        self.recv_count = 0
        self.recv_decode_values: list[bool | None] = []
        self.events = events

    async def recv(self, decode: bool | None = None) -> str:
        message = self._messages[self.recv_count]
        self.recv_count += 1
        self.recv_decode_values.append(decode)
        if self.events is not None:
            self.events.append(f"recv:{self.recv_count}")
        return message


class FakeWebSocketContext:
    def __init__(
        self,
        websocket: FakeWebSocket,
        events: list[str] | None = None,
        exit_return: bool | None = None,
        exit_error: Exception | None = None,
    ) -> None:
        self.websocket = websocket
        self.events = events
        self.exit_return = exit_return
        self.exit_error = exit_error
        self.enter_count = 0
        self.exit_count = 0
        self.exit_exception: tuple[type[BaseException] | None, BaseException | None] | None = None

    async def __aenter__(self) -> FakeWebSocket:
        self.enter_count += 1
        if self.events is not None:
            self.events.append("enter")
        return self.websocket

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback,
    ) -> bool | None:
        self.exit_count += 1
        self.exit_exception = (exc_type, exc)
        if self.events is not None:
            self.events.append("exit")
        if self.exit_error is not None:
            raise self.exit_error
        return self.exit_return


class FakeWebSocketConnect:
    def __init__(self, context: FakeWebSocketContext) -> None:
        self.context = context
        self.urls: list[str] = []

    def __call__(self, url: str) -> FakeWebSocketContext:
        self.urls.append(url)
        return self.context


def test_open_websocket_message_receiver_passes_url_to_connection_factory() -> None:
    websocket = FakeWebSocket('{"event":"trade"}')
    context = FakeWebSocketContext(websocket)
    connect = FakeWebSocketConnect(context)

    async def run() -> None:
        async with open_websocket_message_receiver(
            "wss://stream.example.test/stream",
            connect=connect,
            clock=lambda: 1735689600456,
        ):
            pass

    asyncio.run(run())

    assert connect.urls == ["wss://stream.example.test/stream"]


def test_open_websocket_message_receiver_receives_multiple_messages_on_one_connection() -> None:
    events: list[str] = []
    websocket = FakeWebSocket(
        ['{"event":"first"}', '{"event":"second"}'],
        events=events,
    )
    context = FakeWebSocketContext(websocket, events=events)
    connect = FakeWebSocketConnect(context)
    clock_values = iter([1735689600456, 1735689600789])

    def clock() -> int:
        events.append(f"clock:{websocket.recv_count}")
        return next(clock_values)

    async def run() -> tuple[ReceivedWebSocketMessage, ReceivedWebSocketMessage]:
        async with open_websocket_message_receiver(
            "wss://stream.example.test/stream",
            connect=connect,
            clock=clock,
        ) as receiver:
            first = await receiver.receive()
            second = await receiver.receive()

            return first, second

    first, second = asyncio.run(run())

    assert connect.urls == ["wss://stream.example.test/stream"]
    assert context.enter_count == 1
    assert context.exit_count == 1
    assert context.exit_exception == (None, None)
    assert websocket.recv_count == 2
    assert websocket.recv_decode_values == [True, True]
    assert events == ["enter", "recv:1", "clock:1", "recv:2", "clock:2", "exit"]
    assert first == ReceivedWebSocketMessage(
        text='{"event":"first"}',
        received_at_ms=1735689600456,
    )
    assert second == ReceivedWebSocketMessage(
        text='{"event":"second"}',
        received_at_ms=1735689600789,
    )


def test_receive_one_websocket_message_passes_url_to_connection_factory() -> None:
    websocket = FakeWebSocket('{"event":"trade"}')
    context = FakeWebSocketContext(websocket)
    connect = FakeWebSocketConnect(context)

    asyncio.run(
        receive_one_websocket_message(
            "wss://stream.example.test/stream",
            connect=connect,
            clock=lambda: 1735689600456,
        )
    )

    assert connect.urls == ["wss://stream.example.test/stream"]


def test_open_websocket_message_receiver_forwards_context_exceptions() -> None:
    events: list[str] = []
    websocket = FakeWebSocket('{"event":"trade"}')
    context = FakeWebSocketContext(websocket, events=events)
    connect = FakeWebSocketConnect(context)

    async def run() -> None:
        async with open_websocket_message_receiver(
            "wss://stream.example.test/stream",
            connect=connect,
            clock=lambda: 1735689600456,
        ):
            raise RuntimeError("receive failed")

    with pytest.raises(RuntimeError, match="receive failed"):
        asyncio.run(run())

    assert context.enter_count == 1
    assert context.exit_count == 1
    assert context.exit_exception is not None
    exc_type, exc = context.exit_exception
    assert exc_type is RuntimeError
    assert isinstance(exc, RuntimeError)
    assert str(exc) == "receive failed"
    assert events == ["enter", "exit"]


def test_receive_one_websocket_message_awaits_exactly_one_recv_call() -> None:
    websocket = FakeWebSocket('{"event":"trade"}')
    context = FakeWebSocketContext(websocket)
    connect = FakeWebSocketConnect(context)

    asyncio.run(
        receive_one_websocket_message(
            "wss://stream.example.test/stream",
            connect=connect,
            clock=lambda: 1735689600456,
        )
    )

    assert websocket.recv_count == 1
    assert websocket.recv_decode_values == [True]


def test_receive_one_websocket_message_returns_received_message() -> None:
    websocket = FakeWebSocket('{"event":"trade","price":"1"}')
    context = FakeWebSocketContext(websocket)
    connect = FakeWebSocketConnect(context)

    received = asyncio.run(
        receive_one_websocket_message(
            "wss://stream.example.test/stream",
            connect=connect,
            clock=lambda: 1735689600456,
        )
    )

    assert isinstance(received, ReceivedWebSocketMessage)
    assert received.text == '{"event":"trade","price":"1"}'
    assert received.received_at_ms == 1735689600456


def test_receive_one_websocket_message_exits_connection_context() -> None:
    websocket = FakeWebSocket('{"event":"trade"}')
    context = FakeWebSocketContext(websocket)
    connect = FakeWebSocketConnect(context)

    asyncio.run(
        receive_one_websocket_message(
            "wss://stream.example.test/stream",
            connect=connect,
            clock=lambda: 1735689600456,
        )
    )

    assert context.enter_count == 1
    assert context.exit_count == 1
    assert context.exit_exception == (None, None)


def test_websocket_receiver_context_returns_delegated_exit_value(
    monkeypatch,
    caplog,
) -> None:
    times = iter([10.0, 10.125])
    websocket = FakeWebSocket('{"event":"trade"}')
    context = FakeWebSocketContext(websocket, exit_return=True)
    connect = FakeWebSocketConnect(context)
    receiver_context = open_websocket_message_receiver(
        "wss://stream.example.test/stream",
        connect=connect,
    )

    monkeypatch.setattr(websocket_module, "monotonic", lambda: next(times))

    async def run() -> bool | None:
        await receiver_context.__aenter__()
        return await receiver_context.__aexit__(None, None, None)

    with caplog.at_level(logging.INFO, logger=websocket_module.logger.name):
        result = asyncio.run(run())

    assert result is True
    assert context.exit_count == 1
    assert "WebSocket context exit duration" in caplog.text
    assert "0.125" in caplog.text


def test_websocket_receiver_context_logs_exit_duration_when_delegated_exit_raises(
    monkeypatch,
    caplog,
) -> None:
    class ExitFailure(Exception):
        pass

    times = iter([20.0, 20.25])
    error = ExitFailure("close failed")
    websocket = FakeWebSocket('{"event":"trade"}')
    context = FakeWebSocketContext(websocket, exit_error=error)
    connect = FakeWebSocketConnect(context)
    receiver_context = open_websocket_message_receiver(
        "wss://stream.example.test/stream",
        connect=connect,
    )

    monkeypatch.setattr(websocket_module, "monotonic", lambda: next(times))

    async def run() -> None:
        await receiver_context.__aenter__()
        await receiver_context.__aexit__(None, None, None)

    with caplog.at_level(logging.INFO, logger=websocket_module.logger.name):
        with pytest.raises(ExitFailure) as exc_info:
            asyncio.run(run())

    assert exc_info.value is error
    assert context.exit_count == 1
    assert "WebSocket context exit duration" in caplog.text
    assert "0.250" in caplog.text


def test_receive_one_websocket_message_captures_timestamp_after_recv_before_exit() -> None:
    events: list[str] = []
    websocket = FakeWebSocket('{"event":"trade"}', events=events)
    context = FakeWebSocketContext(websocket, events=events)
    connect = FakeWebSocketConnect(context)

    def clock() -> int:
        events.append("clock")
        return 1735689600456

    received = asyncio.run(
        receive_one_websocket_message(
            "wss://stream.example.test/stream",
            connect=connect,
            clock=clock,
        )
    )

    assert events == ["enter", "recv:1", "clock", "exit"]
    assert received.text == '{"event":"trade"}'
    assert received.received_at_ms == 1735689600456


def test_receive_one_websocket_message_evaluates_clock_once() -> None:
    clock_calls = 0
    websocket = FakeWebSocket('{"event":"trade"}')
    context = FakeWebSocketContext(websocket)
    connect = FakeWebSocketConnect(context)

    def clock() -> int:
        nonlocal clock_calls
        clock_calls += 1
        return 1735689600456

    asyncio.run(
        receive_one_websocket_message(
            "wss://stream.example.test/stream",
            connect=connect,
            clock=clock,
        )
    )

    assert websocket.recv_count == 1
    assert clock_calls == 1
