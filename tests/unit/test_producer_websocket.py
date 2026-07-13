"""Test receiving one raw WebSocket message without network access."""

import asyncio

from jobs.producer.websocket import receive_one_websocket_message


class FakeWebSocket:
    def __init__(self, message: str) -> None:
        self._message = message
        self.recv_count = 0
        self.recv_decode_values: list[bool | None] = []

    async def recv(self, decode: bool | None = None) -> str:
        self.recv_count += 1
        self.recv_decode_values.append(decode)
        return self._message


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
        )
    )

    assert websocket.recv_count == 1
    assert websocket.recv_decode_values == [True]


def test_receive_one_websocket_message_returns_raw_text_message() -> None:
    websocket = FakeWebSocket('{"event":"trade","price":"1"}')
    context = FakeWebSocketContext(websocket)
    connect = FakeWebSocketConnect(context)

    message = asyncio.run(
        receive_one_websocket_message(
            "wss://stream.example.test/stream",
            connect=connect,
        )
    )

    assert message == '{"event":"trade","price":"1"}'


def test_receive_one_websocket_message_exits_connection_context() -> None:
    websocket = FakeWebSocket('{"event":"trade"}')
    context = FakeWebSocketContext(websocket)
    connect = FakeWebSocketConnect(context)

    asyncio.run(
        receive_one_websocket_message(
            "wss://stream.example.test/stream",
            connect=connect,
        )
    )

    assert context.enter_count == 1
    assert context.exit_count == 1
    assert context.exit_exception == (None, None)
