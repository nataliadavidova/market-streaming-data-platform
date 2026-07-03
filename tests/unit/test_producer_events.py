from decimal import Decimal
import json

import pytest
from pydantic import ValidationError

from jobs.producer.events import TradeEvent


def valid_trade_event_data() -> dict[str, object]:
    return {
        "exchange": "binance",
        "symbol": "BTCUSDT",
        "trade_id": "12345",
        "price": "68250.12",
        "quantity": "0.015",
        "event_time_ms": 1735689600123,
        "ingested_at_ms": 1735689600456,
    }


def test_trade_event_accepts_string_decimal_inputs() -> None:
    event = TradeEvent.model_validate(valid_trade_event_data())

    assert event.price == Decimal("68250.12")
    assert event.quantity == Decimal("0.015")


def test_trade_event_serializes_to_deterministic_json_message() -> None:
    event = TradeEvent.model_validate(valid_trade_event_data())

    message = event.to_json_message()

    assert (
        message
        == '{"exchange":"binance","symbol":"BTCUSDT","trade_id":"12345",'
        '"price":"68250.12","quantity":"0.015","event_time_ms":1735689600123,'
        '"ingested_at_ms":1735689600456}'
    )


def test_trade_event_json_message_preserves_decimals_as_strings() -> None:
    event = TradeEvent.model_validate(valid_trade_event_data())

    message_data = json.loads(event.to_json_message())

    assert message_data["price"] == "68250.12"
    assert message_data["quantity"] == "0.015"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("price", "0"),
        ("price", "-1.25"),
        ("quantity", "0"),
        ("quantity", "-0.015"),
    ],
)
def test_trade_event_rejects_non_positive_price_or_quantity(
    field: str, value: str
) -> None:
    data = valid_trade_event_data()
    data[field] = value

    with pytest.raises(ValidationError):
        TradeEvent.model_validate(data)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("event_time_ms", 0),
        ("event_time_ms", -1),
        ("ingested_at_ms", 0),
        ("ingested_at_ms", -1),
    ],
)
def test_trade_event_rejects_non_positive_timestamps(
    field: str, value: int
) -> None:
    data = valid_trade_event_data()
    data[field] = value

    with pytest.raises(ValidationError):
        TradeEvent.model_validate(data)


@pytest.mark.parametrize("field", ["exchange", "symbol", "trade_id"])
def test_trade_event_rejects_blank_required_string(field: str) -> None:
    data = valid_trade_event_data()
    data[field] = "   "

    with pytest.raises(ValidationError):
        TradeEvent.model_validate(data)
