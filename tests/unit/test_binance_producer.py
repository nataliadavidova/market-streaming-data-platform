"""Test executable Binance producer assembly without external services."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from jobs.producer import binance_producer
from jobs.producer.config import ProducerConfig


class FakeKafkaClient:
    def __init__(
        self,
        events: list[str] | None = None,
        remaining_messages: int = 0,
    ) -> None:
        self.flush_count = 0
        self.events = events
        self.flush_timeouts: list[float | None] = []
        self.remaining_messages = remaining_messages

    def flush(self, timeout: float | None = None) -> int:
        self.flush_count += 1
        self.flush_timeouts.append(timeout)
        if self.events is not None:
            self.events.append("client flush")
        return self.remaining_messages


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
    events: list[str] = []
    client = FakeKafkaClient(events)
    publisher = object()
    loaded_config_path = None
    built_bootstrap_servers = None
    publisher_arguments = None

    async def fake_run_binance_trade_publisher(
        received_config: ProducerConfig,
        received_publisher: object,
    ) -> None:
        events.append("runner start")
        assert received_config is config
        assert received_publisher is publisher
        events.append("runner finish")

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
        fake_run_binance_trade_publisher,
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
    assert events == ["runner start", "runner finish", "client flush"]
    assert client.flush_count == 1
    assert client.flush_timeouts == [
        binance_producer.FINAL_KAFKA_FLUSH_TIMEOUT_SECONDS
    ]


def test_run_configured_binance_producer_flushes_after_runtime_failure(
    monkeypatch,
) -> None:
    class RuntimeFailure(Exception):
        pass

    config = valid_producer_config()
    events: list[str] = []
    client = FakeKafkaClient(events)
    publisher = object()
    error = RuntimeFailure("runtime failed")
    runner_call_count = 0

    async def fake_run_binance_trade_publisher(
        received_config: ProducerConfig,
        received_publisher: object,
    ) -> None:
        nonlocal runner_call_count
        runner_call_count += 1
        events.append("runner start")
        assert received_config is config
        assert received_publisher is publisher
        events.append("runner raise")
        raise error

    monkeypatch.setattr(
        binance_producer,
        "load_producer_config",
        lambda config_path: config,
    )
    monkeypatch.setattr(
        binance_producer,
        "build_kafka_client",
        lambda bootstrap_servers: client,
    )
    monkeypatch.setattr(
        binance_producer,
        "KafkaPublisher",
        lambda *, topic, client: publisher,
    )
    monkeypatch.setattr(
        binance_producer,
        "run_binance_trade_publisher",
        fake_run_binance_trade_publisher,
    )

    with pytest.raises(RuntimeFailure) as exc_info:
        asyncio.run(
            binance_producer.run_configured_binance_producer(
                "custom/config.yaml",
                "broker.example:19092",
            )
        )

    assert exc_info.value is error
    assert runner_call_count == 1
    assert events == ["runner start", "runner raise", "client flush"]
    assert client.flush_count == 1
    assert client.flush_timeouts == [
        binance_producer.FINAL_KAFKA_FLUSH_TIMEOUT_SECONDS
    ]


def test_run_configured_binance_producer_raises_when_final_flush_leaves_messages(
    monkeypatch,
) -> None:
    config = valid_producer_config()
    events: list[str] = []
    client = FakeKafkaClient(events, remaining_messages=2)
    publisher = object()

    async def fake_run_binance_trade_publisher(
        received_config: ProducerConfig,
        received_publisher: object,
    ) -> None:
        events.append("runner start")
        assert received_config is config
        assert received_publisher is publisher
        events.append("runner finish")

    monkeypatch.setattr(
        binance_producer,
        "load_producer_config",
        lambda config_path: config,
    )
    monkeypatch.setattr(
        binance_producer,
        "build_kafka_client",
        lambda bootstrap_servers: client,
    )
    monkeypatch.setattr(
        binance_producer,
        "KafkaPublisher",
        lambda *, topic, client: publisher,
    )
    monkeypatch.setattr(
        binance_producer,
        "run_binance_trade_publisher",
        fake_run_binance_trade_publisher,
    )

    with pytest.raises(
        binance_producer.KafkaFinalizationError,
        match="2 message\\(s\\) still queued",
    ):
        asyncio.run(
            binance_producer.run_configured_binance_producer(
                "custom/config.yaml",
                "broker.example:19092",
            )
        )

    assert events == ["runner start", "runner finish", "client flush"]
    assert client.flush_count == 1
    assert client.flush_timeouts == [
        binance_producer.FINAL_KAFKA_FLUSH_TIMEOUT_SECONDS
    ]


def test_run_configured_binance_producer_finalization_error_preserves_runtime_context(
    monkeypatch,
) -> None:
    class RuntimeFailure(Exception):
        pass

    config = valid_producer_config()
    events: list[str] = []
    client = FakeKafkaClient(events, remaining_messages=3)
    publisher = object()
    error = RuntimeFailure("runtime failed")

    async def fake_run_binance_trade_publisher(
        received_config: ProducerConfig,
        received_publisher: object,
    ) -> None:
        events.append("runner start")
        assert received_config is config
        assert received_publisher is publisher
        events.append("runner raise")
        raise error

    monkeypatch.setattr(
        binance_producer,
        "load_producer_config",
        lambda config_path: config,
    )
    monkeypatch.setattr(
        binance_producer,
        "build_kafka_client",
        lambda bootstrap_servers: client,
    )
    monkeypatch.setattr(
        binance_producer,
        "KafkaPublisher",
        lambda *, topic, client: publisher,
    )
    monkeypatch.setattr(
        binance_producer,
        "run_binance_trade_publisher",
        fake_run_binance_trade_publisher,
    )

    with pytest.raises(binance_producer.KafkaFinalizationError) as exc_info:
        asyncio.run(
            binance_producer.run_configured_binance_producer(
                "custom/config.yaml",
                "broker.example:19092",
            )
        )

    assert "3 message(s) still queued" in str(exc_info.value)
    assert exc_info.value.__context__ is error
    assert events == ["runner start", "runner raise", "client flush"]
    assert client.flush_count == 1
    assert client.flush_timeouts == [
        binance_producer.FINAL_KAFKA_FLUSH_TIMEOUT_SECONDS
    ]


def test_run_configured_binance_producer_flushes_after_publisher_construction_failure(
    monkeypatch,
) -> None:
    class PublisherBuildError(Exception):
        pass

    config = valid_producer_config()
    client = FakeKafkaClient()
    error = PublisherBuildError("publisher build failed")
    run_binance_trade_publisher = AsyncMock(return_value=None)

    def fake_kafka_publisher(*, topic: str, client: object) -> object:
        raise error

    monkeypatch.setattr(
        binance_producer,
        "load_producer_config",
        lambda config_path: config,
    )
    monkeypatch.setattr(
        binance_producer,
        "build_kafka_client",
        lambda bootstrap_servers: client,
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

    with pytest.raises(PublisherBuildError) as exc_info:
        asyncio.run(
            binance_producer.run_configured_binance_producer(
                "custom/config.yaml",
                "broker.example:19092",
            )
        )

    assert exc_info.value is error
    assert client.flush_count == 1
    assert client.flush_timeouts == [
        binance_producer.FINAL_KAFKA_FLUSH_TIMEOUT_SECONDS
    ]
    run_binance_trade_publisher.assert_not_awaited()


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


def test_main_treats_keyboard_interrupt_as_expected_shutdown(monkeypatch) -> None:
    coroutine = object()
    run_arguments = None
    asyncio_run_count = 0

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
        nonlocal asyncio_run_count
        asyncio_run_count += 1
        assert awaitable is coroutine
        raise KeyboardInterrupt

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
    assert asyncio_run_count == 1


def test_main_does_not_suppress_unexpected_runtime_error(monkeypatch) -> None:
    error = RuntimeError("unexpected")

    monkeypatch.setattr(
        binance_producer,
        "run_configured_binance_producer",
        lambda config_path, bootstrap_servers: object(),
    )
    monkeypatch.setattr(
        binance_producer.asyncio,
        "run",
        lambda awaitable: (_ for _ in ()).throw(error),
    )

    with pytest.raises(RuntimeError) as exc_info:
        binance_producer.main()

    assert exc_info.value is error


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
