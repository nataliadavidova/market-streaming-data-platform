"""Helpers for Binance trade stream messages and stream URLs."""

from jobs.producer.events import TradeEvent


BINANCE_COMBINED_STREAM_BASE_URL = "wss://stream.binance.com:9443/stream"


def build_binance_combined_trade_stream_url(symbols: list[str]) -> str:
    if not symbols:
        raise ValueError("symbols must contain at least one symbol")

    streams = "/".join(f"{symbol.lower()}@trade" for symbol in symbols)
    return f"{BINANCE_COMBINED_STREAM_BASE_URL}?streams={streams}"


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
