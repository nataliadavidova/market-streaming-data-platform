"""Test Binance trade event receiver sessions without network access."""

import asyncio
import json
from decimal import Decimal

from jobs.producer.binance import open_binance_trade_event_receiver
from jobs.producer.config import ProducerConfig
from jobs.producer.events import TradeEvent


class FakeWebSocket:
    def __init__(
        self,
        messages: list[str],
        events: list[str] | None = None,
    ) -> None:
        self._messages = messages
        self.events = events
        self.recv_count = 0
        self.recv_decode_values: list[bool | None] = []

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
    ) -> None:
        self.websocket = websocket
        self.events = events
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
    return json.dumps(
        {
            "stream": f"{symbol.lower()}@trade",
            "data": {
                "s": symbol,
                "t": trade_id,
                "p": price,
                "q": quantity,
                "T": event_time_ms,
            },
        }
    )


def test_open_binance_trade_event_receiver_receives_multiple_events_on_one_connection() -> None:
    events: list[str] = []
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
        events=events,
    )
    context = FakeWebSocketContext(websocket, events=events)
    connect = FakeWebSocketConnect(context)
    clock_values = iter([1735689600456, 1735689600789])

    def clock() -> int:
        events.append(f"clock:{websocket.recv_count}")
        return next(clock_values)

    async def run() -> tuple[TradeEvent, TradeEvent]:
        async with open_binance_trade_event_receiver(
            valid_producer_config(["BTCUSDT", "ETHUSDT", "SOLUSDT"]),
            connect=connect,
            clock=clock,
        ) as receiver:
            first = await receiver.receive()
            second = await receiver.receive()

            return first, second

    first, second = asyncio.run(run())

    assert connect.urls == [
        "wss://stream.binance.com:9443/stream?"
        "streams=btcusdt@trade/ethusdt@trade/solusdt@trade"
    ]
    assert context.enter_count == 1
    assert context.exit_count == 1
    assert context.exit_exception == (None, None)
    assert websocket.recv_count == 2
    assert websocket.recv_decode_values == [True, True]
    assert events == ["enter", "recv:1", "clock:1", "recv:2", "clock:2", "exit"]
    assert first == TradeEvent(
        exchange="binance",
        symbol="BTCUSDT",
        trade_id="12345",
        price=Decimal("68250.12"),
        quantity=Decimal("0.015"),
        event_time_ms=1735689600123,
        ingested_at_ms=1735689600456,
    )
    assert second == TradeEvent(
        exchange="binance",
        symbol="ETHUSDT",
        trade_id="67890",
        price=Decimal("3420.55"),
        quantity=Decimal("0.25"),
        event_time_ms=1735689600345,
        ingested_at_ms=1735689600789,
    )
