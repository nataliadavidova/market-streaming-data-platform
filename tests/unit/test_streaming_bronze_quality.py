"""Tests for non-persisted Bronze trade quality classification."""

from datetime import datetime
from decimal import Decimal
import json

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

from jobs.streaming.bronze_quality import classify_raw_trade_kafka_messages


BASE_EVENT = {
    "exchange": "binance",
    "symbol": "BTCUSDT",
    "trade_id": "12345",
    "price": "68250.12",
    "quantity": "0.015",
    "event_time_ms": 1735689600123,
    "ingested_at_ms": 1735689600456,
}


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("market-streaming-bronze-quality-tests")
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


def event_json(**overrides: object) -> str:
    event = {**BASE_EVENT, **overrides}
    return json.dumps(event, separators=(",", ":"))


def kafka_row(
    value: str | None,
    *,
    key: bytes | None = b"binance:BTCUSDT",
    topic: str | None = "market.trades.raw",
    partition: int | None = 0,
    offset: int | None = 42,
    timestamp: datetime | None = datetime(2026, 7, 25, 1, 2, 3),
) -> tuple[object, ...]:
    return (
        key,
        None if value is None else value.encode("utf-8"),
        topic,
        partition,
        offset,
        timestamp,
    )


def classify_one(spark: SparkSession, row: tuple[object, ...]):
    source = spark.createDataFrame([row], schema=kafka_source_schema())
    return classify_raw_trade_kafka_messages(source).collect()[0]


def test_valid_trade_is_valid_with_decimal_values_and_empty_errors(
    spark: SparkSession,
) -> None:
    row = classify_one(spark, kafka_row(event_json()))

    assert row.is_valid is True
    assert row.validation_errors == []
    assert row.price == Decimal("68250.120000000000000000")
    assert row.quantity == Decimal("0.015000000000000000")


def test_output_schema_contains_quality_columns_and_required_types(
    spark: SparkSession,
) -> None:
    source = spark.createDataFrame([kafka_row(event_json())], kafka_source_schema())
    classified = classify_raw_trade_kafka_messages(source)

    assert classified.columns == [
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
        "is_valid",
        "validation_errors",
    ]
    assert str(classified.schema["price"].dataType) == "DecimalType(38,18)"
    assert str(classified.schema["quantity"].dataType) == "DecimalType(38,18)"
    assert str(classified.schema["is_valid"].dataType) == "BooleanType()"
    assert str(classified.schema["validation_errors"].dataType) == (
        "ArrayType(StringType(), True)"
    )


def test_null_kafka_value_is_classified_and_audit_metadata_is_preserved(
    spark: SparkSession,
) -> None:
    row = classify_one(spark, kafka_row(None))

    assert row.is_valid is False
    assert row.validation_errors == ["NULL_RAW_JSON"]
    assert row.raw_json is None
    assert row.kafka_key == "binance:BTCUSDT"
    assert row.kafka_topic == "market.trades.raw"
    assert row.kafka_partition == 0
    assert row.kafka_offset == 42
    assert row.kafka_timestamp == datetime(2026, 7, 25, 1, 2, 3)


def test_malformed_json_is_classified_without_losing_raw_audit_fields(
    spark: SparkSession,
) -> None:
    raw_json = "{not-json"
    row = classify_one(spark, kafka_row(raw_json))

    assert row.is_valid is False
    assert row.validation_errors == ["MALFORMED_JSON"]
    assert row.raw_json == raw_json
    assert row.kafka_topic == "market.trades.raw"
    assert row.kafka_partition == 0
    assert row.kafka_offset == 42


@pytest.mark.parametrize(
    ("field", "error_code"),
    [("price", "INVALID_PRICE"), ("quantity", "INVALID_QUANTITY")],
)
def test_invalid_decimal_is_null_and_classified_without_batch_failure(
    spark: SparkSession,
    field: str,
    error_code: str,
) -> None:
    row = classify_one(spark, kafka_row(event_json(**{field: "not-a-decimal"})))

    assert row.is_valid is False
    assert row.validation_errors == [error_code]
    assert getattr(row, field) is None


def test_invalid_decimal_is_safe_under_effective_ansi_configuration(
    spark: SparkSession,
) -> None:
    effective_ansi = spark.conf.get("spark.sql.ansi.enabled")
    row = classify_one(spark, kafka_row(event_json(price="not-a-decimal")))

    assert effective_ansi in {"true", "false"}
    assert row.validation_errors == ["INVALID_PRICE"]


@pytest.mark.parametrize(
    ("field", "error_code"),
    [
        ("price", "NON_POSITIVE_PRICE"),
        ("quantity", "NON_POSITIVE_QUANTITY"),
    ],
)
def test_non_positive_numeric_values_are_classified(
    spark: SparkSession,
    field: str,
    error_code: str,
) -> None:
    value = "0" if field == "price" else "-0.015"
    row = classify_one(spark, kafka_row(event_json(**{field: value})))

    assert row.is_valid is False
    assert row.validation_errors == [error_code]


@pytest.mark.parametrize(
    ("field", "value", "error_code"),
    [
        ("exchange", None, "MISSING_EXCHANGE"),
        ("symbol", "   ", "MISSING_SYMBOL"),
        ("trade_id", None, "MISSING_TRADE_ID"),
    ],
)
def test_missing_or_blank_identity_is_classified(
    spark: SparkSession,
    field: str,
    value: object,
    error_code: str,
) -> None:
    event = event_json(**{field: value})
    row = classify_one(spark, kafka_row(event))

    assert row.is_valid is False
    assert row.validation_errors == [error_code]


@pytest.mark.parametrize(
    ("field", "error_code"),
    [("price", "MISSING_PRICE"), ("quantity", "MISSING_QUANTITY")],
)
def test_missing_numeric_fields_are_classified(
    spark: SparkSession,
    field: str,
    error_code: str,
) -> None:
    row = classify_one(spark, kafka_row(event_json(**{field: None})))

    assert row.is_valid is False
    assert row.validation_errors == [error_code]


def test_missing_event_time_and_non_positive_ingestion_time_are_classified(
    spark: SparkSession,
) -> None:
    event = event_json(event_time_ms=None, ingested_at_ms=0)
    row = classify_one(spark, kafka_row(event))

    assert row.is_valid is False
    assert row.validation_errors == [
        "MISSING_EVENT_TIME",
        "NON_POSITIVE_INGESTED_AT",
    ]


def test_missing_kafka_topic_partition_and_offset_are_classified(
    spark: SparkSession,
) -> None:
    row = classify_one(
        spark,
        kafka_row(
            event_json(),
            topic=None,
            partition=None,
            offset=None,
        ),
    )

    assert row.is_valid is False
    assert row.validation_errors == [
        "MISSING_KAFKA_TOPIC",
        "MISSING_KAFKA_PARTITION",
        "MISSING_KAFKA_OFFSET",
    ]


def test_multiple_errors_have_stable_order_and_no_duplicates(
    spark: SparkSession,
) -> None:
    row = classify_one(
        spark,
        kafka_row(
            event_json(trade_id=None, price="oops", quantity="-1"),
        ),
    )

    assert row.validation_errors == [
        "MISSING_TRADE_ID",
        "INVALID_PRICE",
        "NON_POSITIVE_QUANTITY",
    ]
    assert len(row.validation_errors) == len(set(row.validation_errors))


def test_extra_json_field_remains_valid_and_raw_json_is_preserved(
    spark: SparkSession,
) -> None:
    raw_json = event_json(extra_field="kept-in-raw-json")
    row = classify_one(spark, kafka_row(raw_json))

    assert row.is_valid is True
    assert row.validation_errors == []
    assert row.raw_json == raw_json


def test_all_transport_audit_fields_are_preserved(spark: SparkSession) -> None:
    row = classify_one(spark, kafka_row(event_json(), key=b"custom-key"))

    assert row.kafka_key == "custom-key"
    assert row.kafka_topic == "market.trades.raw"
    assert row.kafka_partition == 0
    assert row.kafka_offset == 42
    assert row.kafka_timestamp == datetime(2026, 7, 25, 1, 2, 3)
    assert row.raw_json == event_json()
