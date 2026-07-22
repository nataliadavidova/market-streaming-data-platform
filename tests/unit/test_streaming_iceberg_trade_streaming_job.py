"""Unit tests for Kafka-to-Iceberg streaming job orchestration."""

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
