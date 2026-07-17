"""Test Spark parsing for raw trade Kafka messages."""

from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from jobs.streaming.trades import (
    TRADE_DECIMAL_TYPE,
    parse_raw_trade_kafka_messages,
)


RAW_JSON = (
    '{"exchange":"binance","symbol":"ETHUSDT","trade_id":"4217272724",'
    '"price":"1847.50000000","quantity":"0.01010000",'
    '"event_time_ms":1784256121613,"ingested_at_ms":1784256121551}'
)

EXPECTED_COLUMNS = [
    "exchange",
    "symbol",
    "trade_id",
    "price",
    "quantity",
    "event_time_ms",
    "ingested_at_ms",
    "kafka_key",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "raw_json",
]


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("market-streaming-trade-parser-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        yield session
    finally:
        session.stop()


def kafka_source_schema() -> StructType:
    return StructType(
        [
            StructField("key", BinaryType(), nullable=True),
            StructField("value", BinaryType(), nullable=True),
            StructField("topic", StringType(), nullable=True),
            StructField("partition", IntegerType(), nullable=True),
            StructField("offset", LongType(), nullable=True),
            StructField("timestamp", TimestampType(), nullable=True),
        ]
    )


def kafka_source_df(spark: SparkSession):
    kafka_timestamp = datetime(2026, 7, 17, 2, 42, 4)
    return spark.createDataFrame(
        [
            (
                b"binance:ETHUSDT",
                RAW_JSON.encode("utf-8"),
                "market.trades.raw",
                0,
                42,
                kafka_timestamp,
            )
        ],
        schema=kafka_source_schema(),
    )


def test_parse_raw_trade_kafka_messages_returns_expected_schema(
    spark: SparkSession,
) -> None:
    parsed_df = parse_raw_trade_kafka_messages(kafka_source_df(spark))

    assert parsed_df.columns == EXPECTED_COLUMNS
    assert parsed_df.schema["exchange"].dataType == StringType()
    assert parsed_df.schema["symbol"].dataType == StringType()
    assert parsed_df.schema["trade_id"].dataType == StringType()
    assert parsed_df.schema["price"].dataType == TRADE_DECIMAL_TYPE
    assert parsed_df.schema["quantity"].dataType == TRADE_DECIMAL_TYPE
    assert parsed_df.schema["event_time_ms"].dataType == LongType()
    assert parsed_df.schema["ingested_at_ms"].dataType == LongType()
    assert parsed_df.schema["kafka_key"].dataType == StringType()
    assert parsed_df.schema["kafka_topic"].dataType == StringType()
    assert parsed_df.schema["kafka_partition"].dataType == IntegerType()
    assert parsed_df.schema["kafka_offset"].dataType == LongType()
    assert parsed_df.schema["kafka_timestamp"].dataType == TimestampType()
    assert parsed_df.schema["raw_json"].dataType == StringType()


def test_parse_raw_trade_kafka_messages_parses_event_and_preserves_metadata(
    spark: SparkSession,
) -> None:
    parsed_df = parse_raw_trade_kafka_messages(kafka_source_df(spark))

    row = parsed_df.collect()[0]

    assert row.exchange == "binance"
    assert row.symbol == "ETHUSDT"
    assert row.trade_id == "4217272724"
    assert row.price == Decimal("1847.500000000000000000")
    assert row.quantity == Decimal("0.010100000000000000")
    assert row.event_time_ms == 1784256121613
    assert row.ingested_at_ms == 1784256121551
    assert row.kafka_key == "binance:ETHUSDT"
    assert row.kafka_topic == "market.trades.raw"
    assert row.kafka_partition == 0
    assert row.kafka_offset == 42
    assert row.kafka_timestamp == datetime(2026, 7, 17, 2, 42, 4)
    assert row.raw_json == RAW_JSON
