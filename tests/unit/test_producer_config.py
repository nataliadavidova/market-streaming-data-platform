from jobs.producer.config import load_config


def test_load_config_reads_market_symbols_config() -> None:
    config = load_config("config/market_symbols.yaml")

    assert config["exchange"] == "binance"
    assert {"BTCUSDT", "ETHUSDT", "SOLUSDT"}.issubset(config["stream"]["symbols"])
    assert config["kafka"]["raw_topic"] == "market.trades.raw"
