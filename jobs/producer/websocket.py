"""Receive raw messages from WebSocket connections."""

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from time import time_ns
from typing import Protocol


class WebSocketConnection(Protocol):
    async def recv(self, decode: bool | None = None) -> str:
        ...


WebSocketConnect = Callable[[str], AbstractAsyncContextManager[WebSocketConnection]]
Clock = Callable[[], int]


@dataclass(frozen=True)
class ReceivedWebSocketMessage:
    text: str
    received_at_ms: int


@dataclass(frozen=True)
class WebSocketMessageReceiver:
    websocket: WebSocketConnection
    clock: Clock

    async def receive(self) -> ReceivedWebSocketMessage:
        text = await self.websocket.recv(decode=True)
        received_at_ms = self.clock()

        return ReceivedWebSocketMessage(text=text, received_at_ms=received_at_ms)


def current_time_ms() -> int:
    return time_ns() // 1_000_000


def _default_websocket_connect() -> WebSocketConnect:
    try:
        from websockets import connect
    except ImportError as error:  # pragma: no cover - dependency packaging guard
        raise ImportError(
            "websockets is required to receive WebSocket messages"
        ) from error

    return connect


class WebSocketMessageReceiverContext:
    def __init__(
        self,
        url: str,
        *,
        connect: WebSocketConnect | None = None,
        clock: Clock = current_time_ms,
    ) -> None:
        websocket_connect = connect or _default_websocket_connect()

        self._connection_context = websocket_connect(url)
        self._clock = clock

    async def __aenter__(self) -> WebSocketMessageReceiver:
        websocket = await self._connection_context.__aenter__()

        return WebSocketMessageReceiver(websocket=websocket, clock=self._clock)

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback,
    ) -> bool | None:
        return await self._connection_context.__aexit__(exc_type, exc, traceback)


def open_websocket_message_receiver(
    url: str,
    *,
    connect: WebSocketConnect | None = None,
    clock: Clock = current_time_ms,
) -> WebSocketMessageReceiverContext:
    return WebSocketMessageReceiverContext(url, connect=connect, clock=clock)


async def receive_one_websocket_message(
    url: str,
    *,
    connect: WebSocketConnect | None = None,
    clock: Clock = current_time_ms,
) -> ReceivedWebSocketMessage:
    async with open_websocket_message_receiver(
        url,
        connect=connect,
        clock=clock,
    ) as receiver:
        return await receiver.receive()
