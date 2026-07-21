"""Unit tests for Iceberg Structured Streaming sink construction."""

import pytest

from jobs.streaming.iceberg_sink import start_bronze_trade_stream


class QuerySentinel:
    def __init__(self) -> None:
        self.await_termination_count = 0
        self.stop_count = 0

    def awaitTermination(self) -> None:
        self.await_termination_count += 1

    def stop(self) -> None:
        self.stop_count += 1


class RecordingWriter:
    def __init__(
        self,
        query: QuerySentinel,
        *,
        to_table_error: Exception | None = None,
    ) -> None:
        self.query = query
        self.to_table_error = to_table_error
        self.calls: list[tuple[object, ...]] = []

    def format(self, source: str) -> "RecordingWriter":
        self.calls.append(("format", source))
        return self

    def outputMode(self, output_mode: str) -> "RecordingWriter":
        self.calls.append(("outputMode", output_mode))
        return self

    def option(self, key: str, value: str) -> "RecordingWriter":
        self.calls.append(("option", key, value))
        return self

    def queryName(self, query_name: str) -> "RecordingWriter":
        self.calls.append(("queryName", query_name))
        return self

    def trigger(self, *, processingTime: str) -> "RecordingWriter":
        self.calls.append(("trigger", {"processingTime": processingTime}))
        return self

    def toTable(self, table_name: str) -> QuerySentinel:
        self.calls.append(("toTable", table_name))
        if self.to_table_error is not None:
            raise self.to_table_error
        return self.query


class RecordingDataFrame:
    def __init__(self, writer: RecordingWriter) -> None:
        self.writer = writer

    @property
    def writeStream(self) -> RecordingWriter:
        return self.writer


def test_start_bronze_trade_stream_builds_required_writer_chain() -> None:
    query = QuerySentinel()
    writer = RecordingWriter(query)
    dataframe = RecordingDataFrame(writer)

    returned_query = start_bronze_trade_stream(
        dataframe,
        table_name="market_catalog.market.bronze_trades",
        checkpoint_location="/tmp/bronze-checkpoint",
    )

    assert returned_query is query
    assert writer.calls == [
        ("format", "iceberg"),
        ("outputMode", "append"),
        ("option", "checkpointLocation", "/tmp/bronze-checkpoint"),
        ("toTable", "market_catalog.market.bronze_trades"),
    ]
    assert query.await_termination_count == 0
    assert query.stop_count == 0


def test_start_bronze_trade_stream_can_set_query_name() -> None:
    query = QuerySentinel()
    writer = RecordingWriter(query)
    dataframe = RecordingDataFrame(writer)

    start_bronze_trade_stream(
        dataframe,
        table_name="market_catalog.market.bronze_trades",
        checkpoint_location="/tmp/bronze-checkpoint",
        query_name="market-bronze-trades",
    )

    assert writer.calls == [
        ("format", "iceberg"),
        ("outputMode", "append"),
        ("option", "checkpointLocation", "/tmp/bronze-checkpoint"),
        ("queryName", "market-bronze-trades"),
        ("toTable", "market_catalog.market.bronze_trades"),
    ]


def test_start_bronze_trade_stream_can_set_processing_trigger() -> None:
    query = QuerySentinel()
    writer = RecordingWriter(query)
    dataframe = RecordingDataFrame(writer)

    start_bronze_trade_stream(
        dataframe,
        table_name="market_catalog.market.bronze_trades",
        checkpoint_location="/tmp/bronze-checkpoint",
        processing_time="1 minute",
    )

    assert writer.calls == [
        ("format", "iceberg"),
        ("outputMode", "append"),
        ("option", "checkpointLocation", "/tmp/bronze-checkpoint"),
        ("trigger", {"processingTime": "1 minute"}),
        ("toTable", "market_catalog.market.bronze_trades"),
    ]


def test_start_bronze_trade_stream_can_set_query_name_and_processing_trigger() -> None:
    query = QuerySentinel()
    writer = RecordingWriter(query)
    dataframe = RecordingDataFrame(writer)

    start_bronze_trade_stream(
        dataframe,
        table_name="market_catalog.market.bronze_trades",
        checkpoint_location="/tmp/bronze-checkpoint",
        query_name="market-bronze-trades",
        processing_time="1 minute",
    )

    assert writer.calls == [
        ("format", "iceberg"),
        ("outputMode", "append"),
        ("option", "checkpointLocation", "/tmp/bronze-checkpoint"),
        ("queryName", "market-bronze-trades"),
        ("trigger", {"processingTime": "1 minute"}),
        ("toTable", "market_catalog.market.bronze_trades"),
    ]


def test_start_bronze_trade_stream_propagates_to_table_errors() -> None:
    error = RuntimeError("toTable failed")
    query = QuerySentinel()
    writer = RecordingWriter(query, to_table_error=error)
    dataframe = RecordingDataFrame(writer)

    with pytest.raises(RuntimeError) as exc_info:
        start_bronze_trade_stream(
            dataframe,
            table_name="market_catalog.market.bronze_trades",
            checkpoint_location="/tmp/bronze-checkpoint",
        )

    assert exc_info.value is error
    assert writer.calls == [
        ("format", "iceberg"),
        ("outputMode", "append"),
        ("option", "checkpointLocation", "/tmp/bronze-checkpoint"),
        ("toTable", "market_catalog.market.bronze_trades"),
    ]
    assert query.await_termination_count == 0
    assert query.stop_count == 0
