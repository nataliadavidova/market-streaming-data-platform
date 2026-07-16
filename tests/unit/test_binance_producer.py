"""Test executable Binance producer assembly without external services."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from jobs.producer import binance_producer
from jobs.producer.config import ProducerConfig


def valid_producer_config() -> ProducerConfig:
    return ProducerConfig.model_validate(
        {
            "exchange": "binance",
            "stream": {
                "type": "trades",
                "symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
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


def test_run_configured_binance_producer_assembles_runtime(monkeypatch) -> None:
    config = valid_producer_config()
    client = object()
    publisher = object()
    loaded_config_path = None
    built_bootstrap_servers = None
    publisher_arguments = None
    run_binance_trade_publisher = AsyncMock(return_value=None)

    def fake_load_producer_config(config_path: str) -> ProducerConfig:
        nonlocal loaded_config_path
        loaded_config_path = config_path
        return config

    def fake_build_kafka_client(bootstrap_servers: str) -> object:
        nonlocal built_bootstrap_servers
        built_bootstrap_servers = bootstrap_servers
        return client

    def fake_kafka_publisher(*, topic: str, client: object) -> object:
        nonlocal publisher_arguments
        publisher_arguments = {
            "topic": topic,
            "client": client,
        }
        return publisher

    monkeypatch.setattr(
        binance_producer,
        "load_producer_config",
        fake_load_producer_config,
    )
    monkeypatch.setattr(
        binance_producer,
        "build_kafka_client",
        fake_build_kafka_client,
    )
    monkeypatch.setattr(
        binance_producer,
        "KafkaPublisher",
        fake_kafka_publisher,
    )
    monkeypatch.setattr(
        binance_producer,
        "run_binance_trade_publisher",
        run_binance_trade_publisher,
    )

    asyncio.run(
        binance_producer.run_configured_binance_producer(
            "custom/config.yaml",
            "broker.example:19092",
        )
    )

    assert loaded_config_path == "custom/config.yaml"
    assert built_bootstrap_servers == "broker.example:19092"
    assert publisher_arguments == {
        "topic": "market.trades.raw",
        "client": client,
    }
    run_binance_trade_publisher.assert_awaited_once_with(config, publisher)


def test_run_configured_binance_producer_propagates_client_build_error(
    monkeypatch,
) -> None:
    class ClientBuildError(Exception):
        pass

    config = valid_producer_config()
    error = ClientBuildError("client build failed")
    kafka_publisher_called = False
    run_binance_trade_publisher = AsyncMock(return_value=None)

    def fake_build_kafka_client(bootstrap_servers: str) -> object:
        raise error

    def fake_kafka_publisher(*, topic: str, client: object) -> object:
        nonlocal kafka_publisher_called
        kafka_publisher_called = True
        return object()

    monkeypatch.setattr(
        binance_producer,
        "load_producer_config",
        lambda config_path: config,
    )
    monkeypatch.setattr(
        binance_producer,
        "build_kafka_client",
        fake_build_kafka_client,
    )
    monkeypatch.setattr(
        binance_producer,
        "KafkaPublisher",
        fake_kafka_publisher,
    )
    monkeypatch.setattr(
        binance_producer,
        "run_binance_trade_publisher",
        run_binance_trade_publisher,
    )

    with pytest.raises(ClientBuildError) as exc_info:
        asyncio.run(
            binance_producer.run_configured_binance_producer(
                "custom/config.yaml",
                "broker.example:19092",
            )
        )

    assert exc_info.value is error
    assert kafka_publisher_called is False
    run_binance_trade_publisher.assert_not_awaited()


def test_main_uses_default_bootstrap_servers(monkeypatch) -> None:
    coroutine = object()
    run_arguments = None
    asyncio_run_arguments = None

    def fake_run_configured_binance_producer(
        config_path: str,
        bootstrap_servers: str,
    ) -> object:
        nonlocal run_arguments
        run_arguments = {
            "config_path": config_path,
            "bootstrap_servers": bootstrap_servers,
        }
        return coroutine

    def fake_asyncio_run(awaitable: object) -> None:
        nonlocal asyncio_run_arguments
        asyncio_run_arguments = awaitable

    monkeypatch.delenv(binance_producer.KAFKA_BOOTSTRAP_SERVERS_ENV, raising=False)
    monkeypatch.setattr(
        binance_producer,
        "run_configured_binance_producer",
        fake_run_configured_binance_producer,
    )
    monkeypatch.setattr(binance_producer.asyncio, "run", fake_asyncio_run)

    binance_producer.main()

    assert run_arguments == {
        "config_path": binance_producer.DEFAULT_CONFIG_PATH,
        "bootstrap_servers": "localhost:9092",
    }
    assert asyncio_run_arguments is coroutine


def test_main_forwards_configured_bootstrap_servers(monkeypatch) -> None:
    coroutine = object()
    run_arguments = None

    def fake_run_configured_binance_producer(
        config_path: str,
        bootstrap_servers: str,
    ) -> object:
        nonlocal run_arguments
        run_arguments = {
            "config_path": config_path,
            "bootstrap_servers": bootstrap_servers,
        }
        return coroutine

    monkeypatch.setenv(
        binance_producer.KAFKA_BOOTSTRAP_SERVERS_ENV,
        "custom-kafka:19092",
    )
    monkeypatch.setattr(
        binance_producer,
        "run_configured_binance_producer",
        fake_run_configured_binance_producer,
    )
    monkeypatch.setattr(binance_producer.asyncio, "run", lambda awaitable: None)

    binance_producer.main()

    assert run_arguments == {
        "config_path": binance_producer.DEFAULT_CONFIG_PATH,
        "bootstrap_servers": "custom-kafka:19092",
    }


def test_main_preserves_empty_bootstrap_servers(monkeypatch) -> None:
    run_bootstrap_servers = None

    def fake_run_configured_binance_producer(
        config_path: str,
        bootstrap_servers: str,
    ) -> object:
        nonlocal run_bootstrap_servers
        run_bootstrap_servers = bootstrap_servers
        return object()

    monkeypatch.setenv(binance_producer.KAFKA_BOOTSTRAP_SERVERS_ENV, "")
    monkeypatch.setattr(
        binance_producer,
        "run_configured_binance_producer",
        fake_run_configured_binance_producer,
    )
    monkeypatch.setattr(binance_producer.asyncio, "run", lambda awaitable: None)

    binance_producer.main()

    assert run_bootstrap_servers == ""
