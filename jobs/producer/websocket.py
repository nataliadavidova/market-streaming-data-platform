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


async def receive_one_websocket_message(
    url: str,
    *,
    connect: WebSocketConnect | None = None,
    clock: Clock = current_time_ms,
) -> ReceivedWebSocketMessage:
    websocket_connect = connect or _default_websocket_connect()

    async with websocket_connect(url) as websocket:
        text = await websocket.recv(decode=True)
        received_at_ms = clock()

        return ReceivedWebSocketMessage(text=text, received_at_ms=received_at_ms)
