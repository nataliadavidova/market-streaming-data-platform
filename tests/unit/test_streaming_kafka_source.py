"""Unit tests for Spark Kafka streaming source construction."""

from typing import cast

from pyspark.sql import DataFrame, SparkSession

from jobs.streaming.kafka_source import read_raw_trade_kafka_stream


class RecordingDataStreamReader:
    def __init__(self, result: object) -> None:
        self.result = result
        self.formats: list[str] = []
        self.options: list[tuple[str, str]] = []
        self.load_calls = 0

    def format(self, source: str) -> "RecordingDataStreamReader":
        self.formats.append(source)
        return self

    def option(self, key: str, value: str) -> "RecordingDataStreamReader":
        self.options.append((key, value))
        return self

    def load(self) -> object:
        self.load_calls += 1
        return self.result


class RecordingSparkSession:
    def __init__(self, read_stream: RecordingDataStreamReader) -> None:
        self.readStream = read_stream


def test_read_raw_trade_kafka_stream_builds_required_kafka_source() -> None:
    result = object()
    read_stream = RecordingDataStreamReader(result)
    spark = RecordingSparkSession(read_stream)

    stream = read_raw_trade_kafka_stream(
        cast(SparkSession, spark),
        bootstrap_servers="localhost:9092",
        topic="market.trades.raw",
    )

    assert stream is cast(DataFrame, result)
    assert read_stream.formats == ["kafka"]
    assert read_stream.options == [
        ("kafka.bootstrap.servers", "localhost:9092"),
        ("subscribe", "market.trades.raw"),
    ]
    assert read_stream.load_calls == 1
