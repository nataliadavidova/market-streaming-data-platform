"""Receive raw messages from WebSocket connections."""

import logging
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from time import monotonic, time_ns
from typing import Protocol

logger = logging.getLogger(__name__)

DEFAULT_WEBSOCKET_CLOSE_TIMEOUT_SECONDS = 2.0


class WebSocketConnection(Protocol):
    async def recv(self, decode: bool | None = None) -> str:
        ...


class WebSocketConnect(Protocol):
    def __call__(
        self,
        url: str,
        *,
        close_timeout: float | None,
    ) -> AbstractAsyncContextManager[WebSocketConnection]:
        ...


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
        close_timeout: float | None = DEFAULT_WEBSOCKET_CLOSE_TIMEOUT_SECONDS,
    ) -> None:
        websocket_connect = connect or _default_websocket_connect()

        self._connection_context = websocket_connect(
            url,
            close_timeout=close_timeout,
        )
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
        started_at = monotonic()
        try:
            return await self._connection_context.__aexit__(exc_type, exc, traceback)
        finally:
            logger.info(
                "WebSocket context exit duration %.3f seconds",
                monotonic() - started_at,
            )


def open_websocket_message_receiver(
    url: str,
    *,
    connect: WebSocketConnect | None = None,
    clock: Clock = current_time_ms,
    close_timeout: float | None = DEFAULT_WEBSOCKET_CLOSE_TIMEOUT_SECONDS,
) -> WebSocketMessageReceiverContext:
    return WebSocketMessageReceiverContext(
        url,
        connect=connect,
        clock=clock,
        close_timeout=close_timeout,
    )


async def receive_one_websocket_message(
    url: str,
    *,
    connect: WebSocketConnect | None = None,
    clock: Clock = current_time_ms,
    close_timeout: float | None = DEFAULT_WEBSOCKET_CLOSE_TIMEOUT_SECONDS,
) -> ReceivedWebSocketMessage:
    async with open_websocket_message_receiver(
        url,
        connect=connect,
        clock=clock,
        close_timeout=close_timeout,
    ) as receiver:
        return await receiver.receive()
