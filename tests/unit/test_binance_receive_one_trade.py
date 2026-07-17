"""Test one-shot Binance trade receive composition without network access."""

import asyncio
import json

from jobs.producer import binance
from jobs.producer.binance import receive_one_binance_trade_event
from jobs.producer.config import ProducerConfig
from jobs.producer.events import TradeEvent


class FakeWebSocket:
    def __init__(self, message: str) -> None:
        self._message = message
        self.recv_count = 0

    async def recv(self, decode: bool | None = None) -> str:
        self.recv_count += 1
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

    def __call__(
        self,
        url: str,
        *,
        close_timeout: float | None,
    ) -> FakeWebSocketContext:
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


def valid_combined_trade_message(symbol: str = "BTCUSDT") -> str:
    return json.dumps(
        {
            "stream": f"{symbol.lower()}@trade",
            "data": {
                "s": symbol,
                "t": 12345,
                "p": "68250.12",
                "q": "0.015",
                "T": 1735689600123,
            },
        }
    )


def test_receive_one_binance_trade_event_returns_trade_event() -> None:
    websocket = FakeWebSocket(valid_combined_trade_message())
    context = FakeWebSocketContext(websocket)
    connect = FakeWebSocketConnect(context)

    event = asyncio.run(
        receive_one_binance_trade_event(
            valid_producer_config(["BTCUSDT"]),
            connect=connect,
            clock=lambda: 1735689600456,
        )
    )

    assert event.exchange == "binance"
    assert event.symbol == "BTCUSDT"
    assert event.trade_id == "12345"
    assert event.ingested_at_ms == 1735689600456


def test_receive_one_binance_trade_event_uses_configured_symbol_order_in_url() -> None:
    websocket = FakeWebSocket(valid_combined_trade_message("SOLUSDT"))
    context = FakeWebSocketContext(websocket)
    connect = FakeWebSocketConnect(context)

    asyncio.run(
        receive_one_binance_trade_event(
            valid_producer_config(["SOLUSDT", "BTCUSDT", "ETHUSDT"]),
            connect=connect,
            clock=lambda: 1735689600456,
        )
    )

    assert connect.urls == [
        "wss://stream.binance.com:9443/stream?"
        "streams=solusdt@trade/btcusdt@trade/ethusdt@trade"
    ]


def test_receive_one_binance_trade_event_receives_exactly_one_message() -> None:
    websocket = FakeWebSocket(valid_combined_trade_message())
    context = FakeWebSocketContext(websocket)
    connect = FakeWebSocketConnect(context)

    asyncio.run(
        receive_one_binance_trade_event(
            valid_producer_config(["BTCUSDT"]),
            connect=connect,
            clock=lambda: 1735689600456,
        )
    )

    assert websocket.recv_count == 1


def test_receive_one_binance_trade_event_exits_connection_context() -> None:
    websocket = FakeWebSocket(valid_combined_trade_message())
    context = FakeWebSocketContext(websocket)
    connect = FakeWebSocketConnect(context)

    asyncio.run(
        receive_one_binance_trade_event(
            valid_producer_config(["BTCUSDT"]),
            connect=connect,
            clock=lambda: 1735689600456,
        )
    )

    assert context.enter_count == 1
    assert context.exit_count == 1
    assert context.exit_exception == (None, None)


def test_receive_one_binance_trade_event_delegates_parsing_with_received_timestamp(
    monkeypatch,
) -> None:
    websocket = FakeWebSocket(valid_combined_trade_message())
    context = FakeWebSocketContext(websocket)
    connect = FakeWebSocketConnect(context)
    called_with_message = None
    called_with_ingested_at_ms = None

    def fake_parse_combined_message(
        raw_message: str,
        ingested_at_ms: int,
    ) -> TradeEvent:
        nonlocal called_with_message, called_with_ingested_at_ms
        called_with_message = raw_message
        called_with_ingested_at_ms = ingested_at_ms
        return TradeEvent(
            exchange="binance",
            symbol="BTCUSDT",
            trade_id="12345",
            price="68250.12",
            quantity="0.015",
            event_time_ms=1735689600123,
            ingested_at_ms=ingested_at_ms,
        )

    monkeypatch.setattr(
        binance,
        "parse_binance_combined_trade_message",
        fake_parse_combined_message,
    )

    event = asyncio.run(
        receive_one_binance_trade_event(
            valid_producer_config(["BTCUSDT"]),
            connect=connect,
            clock=lambda: 1735689600789,
        )
    )

    assert called_with_message == valid_combined_trade_message()
    assert called_with_ingested_at_ms == 1735689600789
    assert event.ingested_at_ms == 1735689600789
