"""Test receiving one raw WebSocket message without network access."""

import asyncio

from jobs.producer.websocket import (
    ReceivedWebSocketMessage,
    receive_one_websocket_message,
)


class FakeWebSocket:
    def __init__(self, message: str, events: list[str] | None = None) -> None:
        self._message = message
        self.recv_count = 0
        self.recv_decode_values: list[bool | None] = []
        self.events = events

    async def recv(self, decode: bool | None = None) -> str:
        self.recv_count += 1
        self.recv_decode_values.append(decode)
        if self.events is not None:
            self.events.append("recv")
        return self._message


class FakeWebSocketContext:
    def __init__(
        self,
        websocket: FakeWebSocket,
        events: list[str] | None = None,
    ) -> None:
        self.websocket = websocket
        self.events = events
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
        if self.events is not None:
            self.events.append("exit")


class FakeWebSocketConnect:
    def __init__(self, context: FakeWebSocketContext) -> None:
        self.context = context
        self.urls: list[str] = []

    def __call__(self, url: str) -> FakeWebSocketContext:
        self.urls.append(url)
        return self.context


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

    assert events == ["recv", "clock", "exit"]
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
