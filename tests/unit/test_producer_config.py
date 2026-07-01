import pytest
from pydantic import ValidationError

from jobs.producer.config import load_config, load_producer_config


def test_load_config_reads_market_symbols_config() -> None:
    config = load_config("config/market_symbols.yaml")

    assert config["exchange"] == "binance"
    assert {"BTCUSDT", "ETHUSDT", "SOLUSDT"}.issubset(config["stream"]["symbols"])
    assert config["kafka"]["raw_topic"] == "market.trades.raw"


def test_load_producer_config_validates_market_symbols_config() -> None:
    config = load_producer_config("config/market_symbols.yaml")

    assert config.exchange == "binance"
    assert {"BTCUSDT", "ETHUSDT", "SOLUSDT"}.issubset(config.stream.symbols)
    assert config.kafka.raw_topic == "market.trades.raw"
    assert config.producer.reconnect_delay_seconds == 5


def test_load_producer_config_rejects_scalar_symbols(tmp_path) -> None:
    config_path = tmp_path / "invalid_market_symbols.yaml"
    config_path.write_text(
        """
exchange: binance
stream:
  type: trades
  symbols: BTCUSDT
kafka:
  raw_topic: market.trades.raw
producer:
  reconnect_delay_seconds: 5
  max_reconnect_delay_seconds: 60
""",
        encoding="utf-8",
    )

    with pytest.raises(ValidationError):
        load_producer_config(config_path)
