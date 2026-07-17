"""Unit tests for the executable Spark trade streaming job assembly."""

from typing import cast

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

from jobs.streaming import trade_streaming_job


class RecordingSparkBuilder:
    def __init__(self, result: object) -> None:
        self.result = result
        self.app_names: list[str] = []
        self.configs: list[tuple[str, str]] = []
        self.get_or_create_count = 0

    def appName(self, app_name: str) -> "RecordingSparkBuilder":
        self.app_names.append(app_name)
        return self

    def config(self, key: str, value: str) -> "RecordingSparkBuilder":
        self.configs.append((key, value))
        return self

    def getOrCreate(self) -> object:
        self.get_or_create_count += 1
        return self.result


class FakeSparkSessionType:
    builder: RecordingSparkBuilder


class RecordingWriteStream:
    def __init__(self, result: object) -> None:
        self.result = result
        self.formats: list[str] = []
        self.output_modes: list[str] = []
        self.options: list[tuple[str, str]] = []
        self.start_count = 0

    def format(self, sink: str) -> "RecordingWriteStream":
        self.formats.append(sink)
        return self

    def outputMode(self, mode: str) -> "RecordingWriteStream":
        self.output_modes.append(mode)
        return self

    def option(self, key: str, value: str) -> "RecordingWriteStream":
        self.options.append((key, value))
        return self

    def start(self) -> object:
        self.start_count += 1
        return self.result


class ParsedTrades:
    def __init__(self, write_stream: RecordingWriteStream) -> None:
        self.writeStream = write_stream


class FakeSpark:
    def __init__(self) -> None:
        self.stop_count = 0

    def stop(self) -> None:
        self.stop_count += 1


class FakeQuery:
    def __init__(
        self,
        *,
        active: bool = True,
        await_error: Exception | None = None,
        stop_error: Exception | None = None,
    ) -> None:
        self.isActive = active
        self.await_error = await_error
        self.stop_error = stop_error
        self.await_count = 0
        self.stop_count = 0

    def awaitTermination(self) -> None:
        self.await_count += 1
        if self.await_error is not None:
            raise self.await_error

    def stop(self) -> None:
        self.stop_count += 1
        if self.stop_error is not None:
            raise self.stop_error
        self.isActive = False


def test_build_spark_session_sets_app_name_and_utc_timezone(monkeypatch) -> None:
    spark = object()
    builder = RecordingSparkBuilder(spark)
    FakeSparkSessionType.builder = builder
    monkeypatch.setattr(
        trade_streaming_job,
        "SparkSession",
        FakeSparkSessionType,
    )

    session = trade_streaming_job.build_spark_session("custom-app")

    assert session is cast(SparkSession, spark)
    assert builder.app_names == ["custom-app"]
    assert builder.configs == [("spark.sql.session.timeZone", "UTC")]
    assert builder.get_or_create_count == 1


def test_start_console_trade_query_composes_source_parser_and_console_sink(
    monkeypatch,
) -> None:
    spark = object()
    raw_stream = object()
    query = object()
    write_stream = RecordingWriteStream(query)
    parsed_trades = ParsedTrades(write_stream)
    source_arguments = None
    parser_arguments = None

    def fake_read_raw_trade_kafka_stream(
        received_spark: object,
        *,
        bootstrap_servers: str,
        topic: str,
    ) -> object:
        nonlocal source_arguments
        source_arguments = {
            "spark": received_spark,
            "bootstrap_servers": bootstrap_servers,
            "topic": topic,
        }
        return raw_stream

    def fake_parse_raw_trade_kafka_messages(kafka_df: object) -> ParsedTrades:
        nonlocal parser_arguments
        parser_arguments = kafka_df
        return parsed_trades

    monkeypatch.setattr(
        trade_streaming_job,
        "read_raw_trade_kafka_stream",
        fake_read_raw_trade_kafka_stream,
    )
    monkeypatch.setattr(
        trade_streaming_job,
        "parse_raw_trade_kafka_messages",
        fake_parse_raw_trade_kafka_messages,
    )

    returned_query = trade_streaming_job.start_console_trade_query(
        cast(SparkSession, spark),
        bootstrap_servers="localhost:9092",
        topic="market.trades.raw",
    )

    assert returned_query is cast(StreamingQuery, query)
    assert source_arguments == {
        "spark": spark,
        "bootstrap_servers": "localhost:9092",
        "topic": "market.trades.raw",
    }
    assert parser_arguments is raw_stream
    assert write_stream.formats == ["console"]
    assert write_stream.output_modes == ["append"]
    assert write_stream.options == [("truncate", "false")]
    assert write_stream.start_count == 1


def test_run_trade_stream_awaits_query_and_stops_active_resources(
    monkeypatch,
) -> None:
    spark = FakeSpark()
    query = FakeQuery(active=True)
    start_arguments = None

    def fake_start_console_trade_query(
        received_spark: SparkSession,
        *,
        bootstrap_servers: str,
        topic: str,
    ) -> FakeQuery:
        nonlocal start_arguments
        start_arguments = {
            "spark": received_spark,
            "bootstrap_servers": bootstrap_servers,
            "topic": topic,
        }
        return query

    monkeypatch.setattr(
        trade_streaming_job,
        "build_spark_session",
        lambda app_name: spark,
    )
    monkeypatch.setattr(
        trade_streaming_job,
        "start_console_trade_query",
        fake_start_console_trade_query,
    )

    trade_streaming_job.run_trade_stream(
        bootstrap_servers="localhost:9092",
        topic="market.trades.raw",
        app_name="market-trade-streaming",
    )

    assert start_arguments == {
        "spark": spark,
        "bootstrap_servers": "localhost:9092",
        "topic": "market.trades.raw",
    }
    assert query.await_count == 1
    assert query.stop_count == 1
    assert spark.stop_count == 1


def test_run_trade_stream_stops_resources_after_await_error(monkeypatch) -> None:
    error = RuntimeError("stream failed")
    spark = FakeSpark()
    query = FakeQuery(active=True, await_error=error)

    monkeypatch.setattr(
        trade_streaming_job,
        "build_spark_session",
        lambda app_name: spark,
    )
    monkeypatch.setattr(
        trade_streaming_job,
        "start_console_trade_query",
        lambda spark, *, bootstrap_servers, topic: query,
    )

    with pytest.raises(RuntimeError) as exc_info:
        trade_streaming_job.run_trade_stream(
            bootstrap_servers="localhost:9092",
            topic="market.trades.raw",
            app_name="market-trade-streaming",
        )

    assert exc_info.value is error
    assert query.await_count == 1
    assert query.stop_count == 1
    assert spark.stop_count == 1


def test_run_trade_stream_stops_spark_when_query_stop_fails(monkeypatch) -> None:
    error = RuntimeError("stop failed")
    spark = FakeSpark()
    query = FakeQuery(active=True, stop_error=error)

    monkeypatch.setattr(
        trade_streaming_job,
        "build_spark_session",
        lambda app_name: spark,
    )
    monkeypatch.setattr(
        trade_streaming_job,
        "start_console_trade_query",
        lambda spark, *, bootstrap_servers, topic: query,
    )

    with pytest.raises(RuntimeError) as exc_info:
        trade_streaming_job.run_trade_stream(
            bootstrap_servers="localhost:9092",
            topic="market.trades.raw",
            app_name="market-trade-streaming",
        )

    assert exc_info.value is error
    assert query.await_count == 1
    assert query.stop_count == 1
    assert spark.stop_count == 1


def test_run_trade_stream_stops_spark_after_query_start_error(monkeypatch) -> None:
    error = RuntimeError("start failed")
    spark = FakeSpark()
    start_call_count = 0

    def fake_start_console_trade_query(
        spark: SparkSession,
        *,
        bootstrap_servers: str,
        topic: str,
    ) -> StreamingQuery:
        nonlocal start_call_count
        start_call_count += 1
        raise error

    monkeypatch.setattr(
        trade_streaming_job,
        "build_spark_session",
        lambda app_name: spark,
    )
    monkeypatch.setattr(
        trade_streaming_job,
        "start_console_trade_query",
        fake_start_console_trade_query,
    )

    with pytest.raises(RuntimeError) as exc_info:
        trade_streaming_job.run_trade_stream(
            bootstrap_servers="localhost:9092",
            topic="market.trades.raw",
            app_name="market-trade-streaming",
        )

    assert exc_info.value is error
    assert start_call_count == 1
    assert spark.stop_count == 1


def test_parse_args_uses_defaults() -> None:
    args = trade_streaming_job.parse_args([])

    assert args.bootstrap_servers == trade_streaming_job.DEFAULT_BOOTSTRAP_SERVERS
    assert args.topic == trade_streaming_job.DEFAULT_TOPIC
    assert args.app_name == trade_streaming_job.DEFAULT_APP_NAME


def test_parse_args_uses_explicit_overrides() -> None:
    args = trade_streaming_job.parse_args(
        [
            "--bootstrap-servers",
            "custom-kafka:19092",
            "--topic",
            "custom.topic",
            "--app-name",
            "custom-app",
        ]
    )

    assert args.bootstrap_servers == "custom-kafka:19092"
    assert args.topic == "custom.topic"
    assert args.app_name == "custom-app"
