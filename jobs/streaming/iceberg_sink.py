"""Start Iceberg Structured Streaming sinks for parsed market trades."""

from typing import Protocol


class StreamingQueryLike(Protocol):
    pass


class DataStreamWriterLike(Protocol):
    def format(self, source: str) -> "DataStreamWriterLike":
        ...

    def outputMode(self, output_mode: str) -> "DataStreamWriterLike":
        ...

    def option(self, key: str, value: str) -> "DataStreamWriterLike":
        ...

    def queryName(self, query_name: str) -> "DataStreamWriterLike":
        ...

    def trigger(self, *, processingTime: str) -> "DataStreamWriterLike":
        ...

    def toTable(self, table_name: str) -> StreamingQueryLike:
        ...


class StreamingDataFrameLike(Protocol):
    @property
    def writeStream(self) -> DataStreamWriterLike:
        ...


def start_bronze_trade_stream(
    parsed_trades: StreamingDataFrameLike,
    *,
    table_name: str,
    checkpoint_location: str,
    query_name: str | None = None,
    processing_time: str | None = None,
) -> StreamingQueryLike:
    """Start appending parsed trades to an existing Bronze Iceberg table."""
    writer = (
        parsed_trades.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_location)
    )

    if query_name is not None:
        writer = writer.queryName(query_name)

    if processing_time is not None:
        writer = writer.trigger(processingTime=processing_time)

    return writer.toTable(table_name)
