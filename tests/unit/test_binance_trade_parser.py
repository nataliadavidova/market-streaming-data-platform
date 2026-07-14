import json
from decimal import Decimal

import pytest
from pydantic import ValidationError

from jobs.producer import binance
from jobs.producer.binance import (
    build_binance_combined_trade_stream_url,
    build_binance_combined_trade_stream_url_from_config,
    parse_binance_combined_trade_message,
    parse_binance_trade_message,
)
from jobs.producer.config import ProducerConfig
from jobs.producer.events import TradeEvent


def valid_binance_trade_message() -> dict[str, object]:
    return {
        "s": "BTCUSDT",
        "t": 12345,
        "p": "68250.12",
        "q": "0.015",
        "T": 1735689600123,
    }


def valid_binance_combined_trade_message() -> str:
    return json.dumps(
        {
            "stream": "btcusdt@trade",
            "data": valid_binance_trade_message(),
        }
    )


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


def test_build_binance_combined_trade_stream_url_from_config_shape() -> None:
    config = valid_producer_config(["BTCUSDT", "ETHUSDT", "SOLUSDT"])

    url = build_binance_combined_trade_stream_url_from_config(config)

    assert (
        url
        == "wss://stream.binance.com:9443/stream?"
        "streams=btcusdt@trade/ethusdt@trade/solusdt@trade"
    )


def test_build_binance_combined_trade_stream_url_from_config_preserves_order() -> None:
    config = valid_producer_config(["SOLUSDT", "BTCUSDT", "ETHUSDT"])

    url = build_binance_combined_trade_stream_url_from_config(config)

    assert url.endswith("streams=solusdt@trade/btcusdt@trade/ethusdt@trade")


def test_build_binance_combined_trade_stream_url_from_config_rejects_empty_symbols() -> None:
    config = valid_producer_config([])

    with pytest.raises(
        ValueError,
        match="config.stream.symbols must contain at least one symbol",
    ):
        build_binance_combined_trade_stream_url_from_config(config)


def test_producer_config_rejects_missing_stream_symbols() -> None:
    with pytest.raises(ValidationError):
        ProducerConfig.model_validate(
            {
                "exchange": "binance",
                "stream": {
                    "type": "trades",
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


def test_build_binance_combined_trade_stream_url_from_config_delegates_url_building(
    monkeypatch,
) -> None:
    called_with_symbols = None

    def fake_build_url(symbols: list[str]) -> str:
        nonlocal called_with_symbols
        called_with_symbols = symbols
        return "wss://example.test/stream?streams=fake@trade"

    monkeypatch.setattr(
        binance,
        "build_binance_combined_trade_stream_url",
        fake_build_url,
    )
    config = valid_producer_config(["BTCUSDT", "ETHUSDT"])

    url = build_binance_combined_trade_stream_url_from_config(config)

    assert called_with_symbols == ["BTCUSDT", "ETHUSDT"]
    assert url == "wss://example.test/stream?streams=fake@trade"


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


def test_parse_binance_combined_trade_message_returns_trade_event() -> None:
    event = parse_binance_combined_trade_message(
        valid_binance_combined_trade_message(),
        ingested_at_ms=1735689600456,
    )

    assert isinstance(event, TradeEvent)
    assert event.symbol == "BTCUSDT"
    assert event.trade_id == "12345"
    assert event.event_time_ms == 1735689600123
    assert event.ingested_at_ms == 1735689600456


def test_parse_binance_combined_trade_message_delegates_inner_payload(
    monkeypatch,
) -> None:
    called_with_message = None
    called_with_ingested_at_ms = None

    def fake_parse_trade_message(
        raw_message: dict[str, object],
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
        "parse_binance_trade_message",
        fake_parse_trade_message,
    )
    inner_payload = valid_binance_trade_message()
    raw_message = json.dumps({"stream": "btcusdt@trade", "data": inner_payload})

    event = parse_binance_combined_trade_message(
        raw_message,
        ingested_at_ms=1735689600456,
    )

    assert called_with_message == inner_payload
    assert called_with_ingested_at_ms == 1735689600456
    assert event.trade_id == "12345"


def test_parse_binance_combined_trade_message_propagates_invalid_json() -> None:
    with pytest.raises(json.JSONDecodeError):
        parse_binance_combined_trade_message("{", ingested_at_ms=1735689600456)


def test_parse_binance_combined_trade_message_rejects_non_object_message() -> None:
    with pytest.raises(
        TypeError,
        match="Binance combined message must be a JSON object",
    ):
        parse_binance_combined_trade_message("[]", ingested_at_ms=1735689600456)


def test_parse_binance_combined_trade_message_rejects_missing_data() -> None:
    raw_message = json.dumps({"stream": "btcusdt@trade"})

    with pytest.raises(
        ValueError,
        match='Binance combined message must contain "data"',
    ):
        parse_binance_combined_trade_message(
            raw_message,
            ingested_at_ms=1735689600456,
        )


def test_parse_binance_combined_trade_message_rejects_non_object_data() -> None:
    raw_message = json.dumps({"stream": "btcusdt@trade", "data": "not-object"})

    with pytest.raises(
        TypeError,
        match='Binance combined message "data" must be a JSON object',
    ):
        parse_binance_combined_trade_message(
            raw_message,
            ingested_at_ms=1735689600456,
        )
