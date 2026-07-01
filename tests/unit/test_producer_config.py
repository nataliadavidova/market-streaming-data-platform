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
