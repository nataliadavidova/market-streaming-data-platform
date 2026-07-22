"""Unit tests for Kafka-to-Iceberg streaming job orchestration."""

import argparse

import pytest

from jobs.streaming import iceberg_trade_streaming_job


class RecordingSparkBuilder:
    def __init__(self, spark: object, events: list[str]) -> None:
        self.spark = spark
        self.events = events
        self.app_names: list[str] = []
        self.configs: list[tuple[str, str]] = []
        self.get_or_create_count = 0

    def appName(self, app_name: str) -> "RecordingSparkBuilder":
        self.events.append("appName")
        self.app_names.append(app_name)
        return self

    def config(self, key: str, value: str) -> "RecordingSparkBuilder":
        self.events.append("UTC config")
        self.configs.append((key, value))
        return self

    def getOrCreate(self) -> object:
        self.events.append("getOrCreate")
        self.get_or_create_count += 1
        return self.spark


class FakeSpark:
    def __init__(
        self,
        events: list[str] | None = None,
        *,
        sql_error: Exception | None = None,
    ) -> None:
        self.events = events
        self.sql_error = sql_error
        self.sql_calls: list[str] = []
        self.stop_count = 0

    def sql(self, query: str) -> object:
        if self.events is not None:
            self.events.append("table check")
        self.sql_calls.append(query)
        if self.sql_error is not None:
            raise self.sql_error
        return object()

    def stop(self) -> None:
        if self.events is not None:
            self.events.append("spark.stop")
        self.stop_count += 1


class FakeQuery:
    def __init__(
        self,
        events: list[str] | None = None,
        *,
        active: bool = True,
        await_error: Exception | None = None,
        stop_error: Exception | None = None,
    ) -> None:
        self.events = events
        self.isActive = active
        self.await_error = await_error
        self.stop_error = stop_error
        self.await_count = 0
        self.stop_count = 0

    def awaitTermination(self) -> None:
        if self.events is not None:
            self.events.append("awaitTermination")
        self.await_count += 1
        if self.await_error is not None:
            raise self.await_error

    def stop(self) -> None:
        if self.events is not None:
            self.events.append("query.stop")
        self.stop_count += 1
        if self.stop_error is not None:
            raise self.stop_error
        self.isActive = False


BUILD_ARGUMENTS = {
    "app_name": "market-iceberg-trades",
    "catalog_name": "market_catalog",
    "catalog_uri": "http://localhost:8181",
    "warehouse": "s3://market-lake/warehouse",
    "s3_endpoint": "http://localhost:9000",
    "s3_region": "us-east-1",
    "s3_access_key": "access",
    "s3_secret_key": "secret",
    "s3_path_style_access": False,
    "s3a_ssl_enabled": True,
}

RUN_ARGUMENTS = {
    "bootstrap_servers": "localhost:9092",
    "topic": "market.trades.raw",
    **BUILD_ARGUMENTS,
    "table_name": "market_catalog.market.bronze_trades",
    "checkpoint_location": "s3a://market-lake/checkpoints/bronze-trades",
    "query_name": "market-bronze-trades",
    "processing_time": "2 seconds",
}


def test_build_session_orders_configuration_and_forwards_exact_values(
    monkeypatch,
) -> None:
    events: list[str] = []
    spark = FakeSpark()
    builder = RecordingSparkBuilder(spark, events)
    iceberg_call = None
    s3a_call = None

    def fake_configure_iceberg(received_builder, **kwargs):
        nonlocal iceberg_call
        events.append("Iceberg config")
        iceberg_call = (received_builder, kwargs)
        return received_builder

    def fake_configure_s3a(received_builder, **kwargs):
        nonlocal s3a_call
        events.append("S3A config")
        s3a_call = (received_builder, kwargs)
        return received_builder

    monkeypatch.setattr(
        iceberg_trade_streaming_job,
        "configure_iceberg_rest_catalog",
        fake_configure_iceberg,
    )
    monkeypatch.setattr(
        iceberg_trade_streaming_job,
        "configure_s3a_checkpoint_storage",
        fake_configure_s3a,
    )

    result = iceberg_trade_streaming_job.build_iceberg_trade_spark_session(
        **BUILD_ARGUMENTS,
        builder=builder,
    )

    assert result is spark
    assert events == [
        "appName",
        "UTC config",
        "Iceberg config",
        "S3A config",
        "getOrCreate",
    ]
    assert builder.app_names == ["market-iceberg-trades"]
    assert builder.configs == [("spark.sql.session.timeZone", "UTC")]
    assert builder.get_or_create_count == 1
    assert iceberg_call == (
        builder,
        {
            "catalog_name": "market_catalog",
            "catalog_uri": "http://localhost:8181",
            "warehouse": "s3://market-lake/warehouse",
            "s3_endpoint": "http://localhost:9000",
            "s3_region": "us-east-1",
            "s3_access_key": "access",
            "s3_secret_key": "secret",
            "s3_path_style_access": False,
        },
    )
    assert s3a_call == (
        builder,
        {
            "endpoint": "http://localhost:9000",
            "region": "us-east-1",
            "access_key": "access",
            "secret_key": "secret",
            "path_style_access": False,
            "ssl_enabled": True,
        },
    )


def test_build_session_uses_spark_session_builder_when_not_injected(
    monkeypatch,
) -> None:
    events: list[str] = []
    spark = FakeSpark()
    builder = RecordingSparkBuilder(spark, events)

    class FakeSparkSession:
        pass

    FakeSparkSession.builder = builder
    monkeypatch.setattr(iceberg_trade_streaming_job, "SparkSession", FakeSparkSession)
    monkeypatch.setattr(
        iceberg_trade_streaming_job,
        "configure_iceberg_rest_catalog",
        lambda received_builder, **kwargs: received_builder,
    )
    monkeypatch.setattr(
        iceberg_trade_streaming_job,
        "configure_s3a_checkpoint_storage",
        lambda received_builder, **kwargs: received_builder,
    )

    result = iceberg_trade_streaming_job.build_iceberg_trade_spark_session(
        **BUILD_ARGUMENTS
    )

    assert result is spark
    assert builder.get_or_create_count == 1


def test_verify_iceberg_table_exists_executes_exact_describe() -> None:
    spark = FakeSpark()

    result = iceberg_trade_streaming_job.verify_iceberg_table_exists(
        spark,
        table_name="market_catalog.market.bronze_trades",
    )

    assert result is None
    assert spark.sql_calls == [
        "DESCRIBE TABLE EXTENDED market_catalog.market.bronze_trades"
    ]


def install_orchestration_fakes(
    monkeypatch,
    *,
    spark: FakeSpark,
    query: FakeQuery | None,
    events: list[str],
    sink_error: Exception | None = None,
) -> tuple[object, object, dict[str, object]]:
    raw_stream = object()
    parsed_stream = object()
    calls: dict[str, object] = {}

    def fake_build(**kwargs):
        events.append("build session")
        calls["build"] = kwargs
        return spark

    def fake_source(received_spark, **kwargs):
        events.append("Kafka source")
        calls["source"] = (received_spark, kwargs)
        return raw_stream

    def fake_parser(received_stream):
        events.append("parser")
        calls["parser"] = received_stream
        return parsed_stream

    def fake_sink(received_stream, **kwargs):
        events.append("Iceberg sink")
        calls["sink"] = (received_stream, kwargs)
        if sink_error is not None:
            raise sink_error
        return query

    monkeypatch.setattr(
        iceberg_trade_streaming_job,
        "build_iceberg_trade_spark_session",
        fake_build,
    )
    monkeypatch.setattr(
        iceberg_trade_streaming_job,
        "read_raw_trade_kafka_stream",
        fake_source,
    )
    monkeypatch.setattr(
        iceberg_trade_streaming_job,
        "parse_raw_trade_kafka_messages",
        fake_parser,
    )
    monkeypatch.setattr(
        iceberg_trade_streaming_job,
        "start_bronze_trade_stream",
        fake_sink,
    )
    return raw_stream, parsed_stream, calls


def test_run_stream_composes_dependencies_and_stops_active_resources(
    monkeypatch,
) -> None:
    events: list[str] = []
    spark = FakeSpark(events)
    query = FakeQuery(events, active=True)
    raw_stream, parsed_stream, calls = install_orchestration_fakes(
        monkeypatch,
        spark=spark,
        query=query,
        events=events,
    )

    result = iceberg_trade_streaming_job.run_iceberg_trade_stream(**RUN_ARGUMENTS)

    assert result is None
    assert events == [
        "build session",
        "table check",
        "Kafka source",
        "parser",
        "Iceberg sink",
        "awaitTermination",
        "query.stop",
        "spark.stop",
    ]
    assert calls["source"] == (
        spark,
        {
            "bootstrap_servers": "localhost:9092",
            "topic": "market.trades.raw",
        },
    )
    assert calls["parser"] is raw_stream
    assert calls["sink"] == (
        parsed_stream,
        {
            "table_name": "market_catalog.market.bronze_trades",
            "checkpoint_location": "s3a://market-lake/checkpoints/bronze-trades",
            "query_name": "market-bronze-trades",
            "processing_time": "2 seconds",
        },
    )
    assert query.await_count == 1
    assert query.stop_count == 1
    assert spark.stop_count == 1


def test_run_stream_does_not_stop_inactive_query(monkeypatch) -> None:
    events: list[str] = []
    spark = FakeSpark(events)
    query = FakeQuery(events, active=False)
    install_orchestration_fakes(
        monkeypatch,
        spark=spark,
        query=query,
        events=events,
    )

    iceberg_trade_streaming_job.run_iceberg_trade_stream(**RUN_ARGUMENTS)

    assert query.await_count == 1
    assert query.stop_count == 0
    assert spark.stop_count == 1


def test_run_stream_stops_spark_after_table_check_failure(monkeypatch) -> None:
    error = RuntimeError("table missing")
    events: list[str] = []
    spark = FakeSpark(events, sql_error=error)
    query = FakeQuery(events)
    install_orchestration_fakes(
        monkeypatch,
        spark=spark,
        query=query,
        events=events,
    )

    with pytest.raises(RuntimeError) as exc_info:
        iceberg_trade_streaming_job.run_iceberg_trade_stream(**RUN_ARGUMENTS)

    assert exc_info.value is error
    assert events == ["build session", "table check", "spark.stop"]
    assert query.await_count == 0
    assert query.stop_count == 0
    assert spark.stop_count == 1


def test_run_stream_stops_spark_after_sink_creation_failure(monkeypatch) -> None:
    error = RuntimeError("sink failed")
    events: list[str] = []
    spark = FakeSpark(events)
    install_orchestration_fakes(
        monkeypatch,
        spark=spark,
        query=None,
        events=events,
        sink_error=error,
    )

    with pytest.raises(RuntimeError) as exc_info:
        iceberg_trade_streaming_job.run_iceberg_trade_stream(**RUN_ARGUMENTS)

    assert exc_info.value is error
    assert events == [
        "build session",
        "table check",
        "Kafka source",
        "parser",
        "Iceberg sink",
        "spark.stop",
    ]
    assert spark.stop_count == 1


def test_run_stream_stops_resources_after_await_failure(monkeypatch) -> None:
    error = RuntimeError("await failed")
    events: list[str] = []
    spark = FakeSpark(events)
    query = FakeQuery(events, active=True, await_error=error)
    install_orchestration_fakes(
        monkeypatch,
        spark=spark,
        query=query,
        events=events,
    )

    with pytest.raises(RuntimeError) as exc_info:
        iceberg_trade_streaming_job.run_iceberg_trade_stream(**RUN_ARGUMENTS)

    assert exc_info.value is error
    assert query.await_count == 1
    assert query.stop_count == 1
    assert spark.stop_count == 1


def test_run_stream_stops_spark_when_query_stop_fails(monkeypatch) -> None:
    error = RuntimeError("stop failed")
    events: list[str] = []
    spark = FakeSpark(events)
    query = FakeQuery(events, active=True, stop_error=error)
    install_orchestration_fakes(
        monkeypatch,
        spark=spark,
        query=query,
        events=events,
    )

    with pytest.raises(RuntimeError) as exc_info:
        iceberg_trade_streaming_job.run_iceberg_trade_stream(**RUN_ARGUMENTS)

    assert exc_info.value is error
    assert query.await_count == 1
    assert query.stop_count == 1
    assert spark.stop_count == 1


@pytest.mark.parametrize("value", ["true", "TRUE", " yes ", "1", "on"])
def test_parse_boolean_accepts_true_values(value: str) -> None:
    assert iceberg_trade_streaming_job._parse_boolean(value) is True


@pytest.mark.parametrize("value", ["false", "FALSE", " no ", "0", "off"])
def test_parse_boolean_accepts_false_values(value: str) -> None:
    assert iceberg_trade_streaming_job._parse_boolean(value) is False


def test_parse_boolean_rejects_unknown_value() -> None:
    with pytest.raises(argparse.ArgumentTypeError, match="unexpected"):
        iceberg_trade_streaming_job._parse_boolean("unexpected")


def test_parse_args_uses_local_fallbacks() -> None:
    args = iceberg_trade_streaming_job.parse_args([], environ={})

    assert vars(args) == {
        "bootstrap_servers": "localhost:9092",
        "topic": "market.trades.raw",
        "app_name": "market-iceberg-trade-streaming",
        "catalog_name": "market_catalog",
        "catalog_uri": "http://localhost:8181",
        "warehouse": "s3://market-lake/warehouse",
        "table_name": "market_catalog.market.bronze_trades",
        "s3_endpoint": "http://localhost:9000",
        "s3_region": "us-east-1",
        "s3_access_key": "minioadmin",
        "s3_secret_key": "minioadmin",
        "checkpoint_location": (
            "s3a://market-lake/checkpoints/market/bronze-trades"
        ),
        "query_name": "market-iceberg-bronze-trades",
        "processing_time": None,
        "s3_path_style_access": True,
        "s3a_ssl_enabled": False,
    }


def test_parse_args_uses_environment_defaults_without_mutating_mapping() -> None:
    environ = {
        "KAFKA_BOOTSTRAP_SERVERS": "env-kafka:19092",
        "KAFKA_TOPIC_TRADES_RAW": "env.trades.raw",
        "ICEBERG_TRADE_APP_NAME": "env-app",
        "ICEBERG_CATALOG_NAME": "env_catalog",
        "ICEBERG_REST_HOST_URI": "http://env-rest:8181",
        "ICEBERG_WAREHOUSE": "s3://env-lake/warehouse",
        "ICEBERG_BRONZE_TABLE": "env_catalog.market.bronze_trades",
        "S3_HOST_ENDPOINT": "http://env-minio:9000",
        "S3_REGION": "eu-west-1",
        "S3_ACCESS_KEY": "env-access",
        "S3_SECRET_KEY": "env-secret",
        "ICEBERG_TRADE_CHECKPOINT_LOCATION": "s3a://env/checkpoint",
        "ICEBERG_TRADE_QUERY_NAME": "env-query",
        "ICEBERG_TRADE_PROCESSING_TIME": "5 seconds",
        "S3_PATH_STYLE_ACCESS": "false",
        "S3A_SSL_ENABLED": "true",
    }
    original_environ = environ.copy()

    args = iceberg_trade_streaming_job.parse_args([], environ=environ)

    assert vars(args) == {
        "bootstrap_servers": "env-kafka:19092",
        "topic": "env.trades.raw",
        "app_name": "env-app",
        "catalog_name": "env_catalog",
        "catalog_uri": "http://env-rest:8181",
        "warehouse": "s3://env-lake/warehouse",
        "table_name": "env_catalog.market.bronze_trades",
        "s3_endpoint": "http://env-minio:9000",
        "s3_region": "eu-west-1",
        "s3_access_key": "env-access",
        "s3_secret_key": "env-secret",
        "checkpoint_location": "s3a://env/checkpoint",
        "query_name": "env-query",
        "processing_time": "5 seconds",
        "s3_path_style_access": False,
        "s3a_ssl_enabled": True,
    }
    assert environ == original_environ


def test_parse_args_cli_values_override_environment_defaults() -> None:
    environ = {
        "KAFKA_BOOTSTRAP_SERVERS": "env-kafka:19092",
        "KAFKA_TOPIC_TRADES_RAW": "env.topic",
        "ICEBERG_TRADE_APP_NAME": "env-app",
        "ICEBERG_CATALOG_NAME": "env_catalog",
        "ICEBERG_REST_HOST_URI": "http://env-rest:8181",
        "ICEBERG_WAREHOUSE": "s3://env/warehouse",
        "ICEBERG_BRONZE_TABLE": "env_catalog.market.trades",
        "S3_HOST_ENDPOINT": "http://env-s3:9000",
        "S3_REGION": "env-region",
        "S3_ACCESS_KEY": "env-access",
        "S3_SECRET_KEY": "env-secret",
        "ICEBERG_TRADE_CHECKPOINT_LOCATION": "s3a://env/checkpoint",
        "ICEBERG_TRADE_QUERY_NAME": "env-query",
        "ICEBERG_TRADE_PROCESSING_TIME": "10 seconds",
        "S3_PATH_STYLE_ACCESS": "true",
        "S3A_SSL_ENABLED": "false",
    }
    argv = [
        "--bootstrap-servers",
        "cli-kafka:29092",
        "--topic",
        "cli.topic",
        "--app-name",
        "cli-app",
        "--catalog-name",
        "cli_catalog",
        "--catalog-uri",
        "http://cli-rest:8181",
        "--warehouse",
        "s3://cli/warehouse",
        "--table-name",
        "cli_catalog.market.trades",
        "--s3-endpoint",
        "http://cli-s3:9000",
        "--s3-region",
        "cli-region",
        "--s3-access-key",
        "cli-access",
        "--s3-secret-key",
        "cli-secret",
        "--checkpoint-location",
        "s3a://cli/checkpoint",
        "--query-name",
        "cli-query",
        "--processing-time",
        "2 seconds",
        "--no-s3-path-style-access",
        "--s3a-ssl-enabled",
    ]

    args = iceberg_trade_streaming_job.parse_args(argv, environ=environ)

    assert vars(args) == {
        "bootstrap_servers": "cli-kafka:29092",
        "topic": "cli.topic",
        "app_name": "cli-app",
        "catalog_name": "cli_catalog",
        "catalog_uri": "http://cli-rest:8181",
        "warehouse": "s3://cli/warehouse",
        "table_name": "cli_catalog.market.trades",
        "s3_endpoint": "http://cli-s3:9000",
        "s3_region": "cli-region",
        "s3_access_key": "cli-access",
        "s3_secret_key": "cli-secret",
        "checkpoint_location": "s3a://cli/checkpoint",
        "query_name": "cli-query",
        "processing_time": "2 seconds",
        "s3_path_style_access": False,
        "s3a_ssl_enabled": True,
    }


@pytest.mark.parametrize("value", ["", "   "])
def test_parse_args_treats_empty_environment_processing_time_as_none(
    value: str,
) -> None:
    args = iceberg_trade_streaming_job.parse_args(
        [],
        environ={"ICEBERG_TRADE_PROCESSING_TIME": value},
    )

    assert args.processing_time is None


@pytest.mark.parametrize(
    ("environment_name", "value"),
    [
        ("S3_PATH_STYLE_ACCESS", "maybe"),
        ("S3A_SSL_ENABLED", "perhaps"),
    ],
)
def test_parse_args_rejects_invalid_boolean_environment_defaults(
    environment_name: str,
    value: str,
) -> None:
    with pytest.raises(argparse.ArgumentTypeError, match=value):
        iceberg_trade_streaming_job.parse_args(
            [],
            environ={environment_name: value},
        )


def test_main_forwards_parsed_arguments_without_changes(monkeypatch) -> None:
    argv = ["--sentinel"]
    namespace = argparse.Namespace(
        bootstrap_servers="main-kafka",
        topic="main-topic",
        app_name="main-app",
        catalog_name="main-catalog",
        catalog_uri="main-uri",
        warehouse="main-warehouse",
        table_name="main-table",
        s3_endpoint="main-endpoint",
        s3_region="main-region",
        s3_access_key="main-access",
        s3_secret_key="main-secret",
        checkpoint_location="main-checkpoint",
        query_name="main-query",
        processing_time="main-processing-time",
        s3_path_style_access=False,
        s3a_ssl_enabled=True,
    )
    parse_calls: list[object] = []
    run_calls: list[dict[str, object]] = []

    def fake_parse_args(received_argv):
        parse_calls.append(received_argv)
        return namespace

    def fake_run(**kwargs):
        run_calls.append(kwargs)

    monkeypatch.setattr(
        iceberg_trade_streaming_job,
        "parse_args",
        fake_parse_args,
    )
    monkeypatch.setattr(
        iceberg_trade_streaming_job,
        "run_iceberg_trade_stream",
        fake_run,
    )

    result = iceberg_trade_streaming_job.main(argv)

    assert result is None
    assert parse_calls == [argv]
    assert run_calls == [vars(namespace)]
