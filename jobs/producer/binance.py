"""Helpers for Binance trade stream messages and stream URLs."""

import json

from jobs.producer.config import ProducerConfig
from jobs.producer.events import TradeEvent
from jobs.producer.websocket import WebSocketConnect, receive_one_websocket_message


BINANCE_COMBINED_STREAM_BASE_URL = "wss://stream.binance.com:9443/stream"


def build_binance_combined_trade_stream_url(symbols: list[str]) -> str:
    if not symbols:
        raise ValueError("symbols must contain at least one symbol")

    streams = "/".join(f"{symbol.lower()}@trade" for symbol in symbols)
    return f"{BINANCE_COMBINED_STREAM_BASE_URL}?streams={streams}"


def build_binance_combined_trade_stream_url_from_config(
    config: ProducerConfig,
) -> str:
    if not config.stream.symbols:
        raise ValueError("config.stream.symbols must contain at least one symbol")

    return build_binance_combined_trade_stream_url(config.stream.symbols)


def parse_binance_trade_message(
    raw_message: dict[str, object],
    ingested_at_ms: int,
) -> TradeEvent:
    return TradeEvent(
        exchange="binance",
        symbol=raw_message["s"],
        trade_id=str(raw_message["t"]),
        price=raw_message["p"],
        quantity=raw_message["q"],
        event_time_ms=raw_message["T"],
        ingested_at_ms=ingested_at_ms,
    )


def parse_binance_combined_trade_message(
    raw_message: str,
    ingested_at_ms: int,
) -> TradeEvent:
    decoded_message = json.loads(raw_message)

    if not isinstance(decoded_message, dict):
        raise TypeError("Binance combined message must be a JSON object")

    if "data" not in decoded_message:
        raise ValueError('Binance combined message must contain "data"')

    data = decoded_message["data"]
    if not isinstance(data, dict):
        raise TypeError('Binance combined message "data" must be a JSON object')

    return parse_binance_trade_message(data, ingested_at_ms)


async def receive_one_binance_trade_event(
    config: ProducerConfig,
    ingested_at_ms: int,
    *,
    connect: WebSocketConnect | None = None,
) -> TradeEvent:
    url = build_binance_combined_trade_stream_url_from_config(config)
    raw_message = await receive_one_websocket_message(url, connect=connect)

    return parse_binance_combined_trade_message(raw_message, ingested_at_ms)
