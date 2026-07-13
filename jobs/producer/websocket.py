"""Receive raw messages from WebSocket connections."""

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from typing import Protocol


class WebSocketConnection(Protocol):
    async def recv(self, decode: bool | None = None) -> str:
        ...


WebSocketConnect = Callable[[str], AbstractAsyncContextManager[WebSocketConnection]]


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
) -> str:
    websocket_connect = connect or _default_websocket_connect()

    async with websocket_connect(url) as websocket:
        return await websocket.recv(decode=True)
