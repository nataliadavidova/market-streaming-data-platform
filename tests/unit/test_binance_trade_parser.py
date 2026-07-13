from decimal import Decimal

import pytest
from pydantic import ValidationError

from jobs.producer.binance import (
    build_binance_combined_trade_stream_url,
    parse_binance_trade_message,
)
from jobs.producer.events import TradeEvent


def valid_binance_trade_message() -> dict[str, object]:
    return {
        "s": "BTCUSDT",
        "t": 12345,
        "p": "68250.12",
        "q": "0.015",
        "T": 1735689600123,
    }


def test_build_binance_combined_trade_stream_url_for_configured_symbols() -> None:
    url = build_binance_combined_trade_stream_url(["BTCUSDT", "ETHUSDT", "SOLUSDT"])

    assert (
        url
        == "wss://stream.binance.com:9443/stream?"
        "streams=btcusdt@trade/ethusdt@trade/solusdt@trade"
    )


def test_build_binance_combined_trade_stream_url_lowercases_symbols() -> None:
    url = build_binance_combined_trade_stream_url(["BtcUsdt", "EthUsdt"])

    assert url.endswith("streams=btcusdt@trade/ethusdt@trade")


def test_build_binance_combined_trade_stream_url_preserves_symbol_order() -> None:
    url = build_binance_combined_trade_stream_url(["SOLUSDT", "BTCUSDT", "ETHUSDT"])

    assert url.endswith("streams=solusdt@trade/btcusdt@trade/ethusdt@trade")


def test_build_binance_combined_trade_stream_url_rejects_empty_symbols() -> None:
    with pytest.raises(ValueError, match="symbols must contain at least one symbol"):
        build_binance_combined_trade_stream_url([])


def test_parse_binance_trade_message_maps_trade_event_fields() -> None:
    event = parse_binance_trade_message(
        valid_binance_trade_message(),
        ingested_at_ms=1735689600456,
    )

    assert isinstance(event, TradeEvent)
    assert event.exchange == "binance"
    assert event.symbol == "BTCUSDT"
    assert event.trade_id == "12345"
    assert event.price == Decimal("68250.12")
    assert event.quantity == Decimal("0.015")
    assert event.event_time_ms == 1735689600123
    assert event.ingested_at_ms == 1735689600456


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("p", "0"),
        ("q", "0"),
    ],
)
def test_parse_binance_trade_message_raises_validation_error_for_invalid_event(
    field: str, value: str
) -> None:
    raw_message = valid_binance_trade_message()
    raw_message[field] = value

    with pytest.raises(ValidationError):
        parse_binance_trade_message(raw_message, ingested_at_ms=1735689600456)


@pytest.mark.parametrize("field", ["s", "t", "p", "q", "T"])
def test_parse_binance_trade_message_raises_key_error_for_missing_field(
    field: str,
) -> None:
    raw_message = valid_binance_trade_message()
    del raw_message[field]

    with pytest.raises(KeyError):
        parse_binance_trade_message(raw_message, ingested_at_ms=1735689600456)
