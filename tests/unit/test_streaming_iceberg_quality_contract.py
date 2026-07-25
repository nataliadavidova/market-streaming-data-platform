"""Tests for the isolated persisted Bronze quality contract boundary."""

from datetime import datetime
from types import SimpleNamespace
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
from jobs.streaming.iceberg_quality_contract import (
    CANONICAL_BRONZE_TABLE_NAME,
    IcebergQualityContractError,
    QUALITY_CONTRACT_COLUMNS,
    QUALITY_SMOKE_TABLE_NAME,
    append_quality_contract_rows,
    ensure_quality_smoke_table,
    quality_smoke_table_ddl,
    validate_quality_smoke_table_name,
    validate_quality_smoke_table_schema,
)


class RecordingResult:
    def __init__(self, rows: list[object]) -> None:
        self.rows = rows

    def collect(self) -> list[object]:
        return self.rows


class RecordingSpark:
    def __init__(self, rows: list[object]) -> None:
        self.rows = rows
        self.queries: list[str] = []

    def sql(self, query: str) -> RecordingResult:
        self.queries.append(query)
        if query.startswith("DESCRIBE TABLE"):
            return RecordingResult(self.rows)
        return RecordingResult([])


class FakeWriter:
    def __init__(self) -> None:
        self.append_calls = 0

    def append(self) -> None:
        self.append_calls += 1


class FakeDataFrame:
    def __init__(self, columns: list[str], types: list[str], spark: RecordingSpark) -> None:
        self.columns = columns
        self.sparkSession = spark
        self.writer = FakeWriter()
        self.schema = SimpleNamespace(
            fields=[
                SimpleNamespace(
                    name=name,
                    dataType=SimpleNamespace(simpleString=lambda value=value: value),
                )
                for name, value in zip(columns, types)
            ]
        )

    def writeTo(self, table_name: str) -> FakeWriter:
        self.writer.table_name = table_name
        return self.writer


def expected_schema_rows() -> list[dict[str, str]]:
    return [
        {"col_name": name, "data_type": data_type}
        for name, data_type in QUALITY_CONTRACT_COLUMNS
    ]


def expected_names() -> list[str]:
    return [name for name, _ in QUALITY_CONTRACT_COLUMNS]


def expected_types() -> list[str]:
    return [data_type for _, data_type in QUALITY_CONTRACT_COLUMNS]


def test_quality_smoke_table_identifier_is_fixed() -> None:
    assert QUALITY_SMOKE_TABLE_NAME == (
        "market_catalog.market.bronze_trades_quality_smoke"
    )
    assert validate_quality_smoke_table_name(QUALITY_SMOKE_TABLE_NAME) == (
        QUALITY_SMOKE_TABLE_NAME
    )


def test_canonical_table_is_rejected_as_quality_target() -> None:
    with pytest.raises(IcebergQualityContractError, match="canonical Bronze"):
        validate_quality_smoke_table_name(CANONICAL_BRONZE_TABLE_NAME)


def test_other_table_names_are_rejected() -> None:
    with pytest.raises(IcebergQualityContractError, match="quality smoke target"):
        validate_quality_smoke_table_name("market_catalog.market.other")


def test_quality_smoke_ddl_has_exact_contract() -> None:
    ddl = quality_smoke_table_ddl()

    assert ddl == """CREATE TABLE IF NOT EXISTS market_catalog.market.bronze_trades_quality_smoke (
exchange STRING,
symbol STRING,
trade_id STRING,
price DECIMAL(38,18),
quantity DECIMAL(38,18),
event_time_ms BIGINT,
ingested_at_ms BIGINT,
kafka_key STRING,
kafka_topic STRING,
kafka_partition INT,
kafka_offset BIGINT,
kafka_timestamp TIMESTAMP,
raw_json STRING,
is_valid BOOLEAN,
validation_errors ARRAY<STRING>
)
USING iceberg"""


def test_quality_contract_columns_have_exact_order_and_types() -> None:
    assert expected_names() == [
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
    assert expected_types()[-2:] == ["boolean", "array<string>"]
    assert expected_types()[3:5] == ["decimal(38,18)", "decimal(38,18)"]


def test_ensure_creates_if_not_exists_then_validates_schema() -> None:
    spark = RecordingSpark(expected_schema_rows())

    ensure_quality_smoke_table(spark)

    assert spark.queries[0].startswith("CREATE TABLE IF NOT EXISTS")
    assert spark.queries[0].endswith("USING iceberg")
    assert spark.queries[1] == f"DESCRIBE TABLE {QUALITY_SMOKE_TABLE_NAME}"


def test_expected_existing_schema_passes_validation() -> None:
    spark = RecordingSpark(expected_schema_rows())

    validate_quality_smoke_table_schema(spark)

    assert spark.queries == [f"DESCRIBE TABLE {QUALITY_SMOKE_TABLE_NAME}"]


def test_mismatched_existing_schema_fails_clearly() -> None:
    rows = expected_schema_rows()
    rows[13]["data_type"] = "STRING"
    spark = RecordingSpark(rows)

    with pytest.raises(IcebergQualityContractError, match="schema mismatch"):
        validate_quality_smoke_table_schema(spark)


def test_missing_quality_columns_fail_validation() -> None:
    spark = RecordingSpark(expected_schema_rows()[:-2])

    with pytest.raises(IcebergQualityContractError, match="schema mismatch"):
        validate_quality_smoke_table_schema(spark)


def test_static_append_receives_exactly_fifteen_columns() -> None:
    spark = RecordingSpark(expected_schema_rows())
    dataframe = FakeDataFrame(expected_names(), expected_types(), spark)

    append_quality_contract_rows(dataframe)

    assert dataframe.writer.table_name == QUALITY_SMOKE_TABLE_NAME
    assert dataframe.writer.append_calls == 1
    assert spark.queries == [f"DESCRIBE TABLE {QUALITY_SMOKE_TABLE_NAME}"]


def test_extra_column_is_rejected_before_write() -> None:
    spark = RecordingSpark(expected_schema_rows())
    dataframe = FakeDataFrame(expected_names() + ["extra"], expected_types() + ["string"], spark)

    with pytest.raises(IcebergQualityContractError, match="columns must match"):
        append_quality_contract_rows(dataframe)

    assert not hasattr(dataframe.writer, "table_name")
    assert dataframe.writer.append_calls == 0
    assert spark.queries == []


def test_reordered_column_is_rejected_before_write() -> None:
    names = expected_names()
    types = expected_types()
    names[0], names[1] = names[1], names[0]
    types[0], types[1] = types[1], types[0]
    spark = RecordingSpark(expected_schema_rows())
    dataframe = FakeDataFrame(names, types, spark)

    with pytest.raises(IcebergQualityContractError, match="columns must match"):
        append_quality_contract_rows(dataframe)

    assert dataframe.writer.append_calls == 0


def test_append_uses_no_checkpoint_or_streaming_writer() -> None:
    spark = RecordingSpark(expected_schema_rows())
    dataframe = FakeDataFrame(expected_names(), expected_types(), spark)

    append_quality_contract_rows(dataframe)

    assert not hasattr(dataframe, "writeStream")
    assert "checkpoint" not in " ".join(spark.queries).lower()


def test_executed_queries_never_target_canonical_table() -> None:
    spark = RecordingSpark(expected_schema_rows())
    ensure_quality_smoke_table(spark)

    assert all(
        f" {CANONICAL_BRONZE_TABLE_NAME} " not in query
        and not query.endswith(CANONICAL_BRONZE_TABLE_NAME)
        for query in spark.queries
    )
    assert all(QUALITY_SMOKE_TABLE_NAME in query for query in spark.queries)


def test_injected_spark_session_is_not_stopped() -> None:
    spark = RecordingSpark(expected_schema_rows())

    validate_quality_smoke_table_schema(spark)

    assert not hasattr(spark, "stop")


def test_sql_failure_is_chained() -> None:
    class FailingSpark:
        def sql(self, query: str) -> object:
            raise ValueError("catalog unavailable")

    with pytest.raises(IcebergQualityContractError) as raised:
        ensure_quality_smoke_table(FailingSpark())

    assert isinstance(raised.value.__cause__, ValueError)
    assert "could not create quality smoke table" in str(raised.value)


def test_append_failure_is_chained() -> None:
    class FailingWriter:
        def append(self) -> None:
            raise ValueError("append failed")

    class FailingDataFrame(FakeDataFrame):
        def writeTo(self, table_name: str) -> FailingWriter:
            self.writer.table_name = table_name
            return FailingWriter()

    spark = RecordingSpark(expected_schema_rows())
    dataframe = FailingDataFrame(expected_names(), expected_types(), spark)

    with pytest.raises(IcebergQualityContractError) as raised:
        append_quality_contract_rows(dataframe)

    assert isinstance(raised.value.__cause__, ValueError)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("market-streaming-iceberg-quality-contract-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        yield session
    finally:
        session.stop()


def source_schema() -> StructType:
    return StructType(
        [
            StructField("key", BinaryType(), True),
            StructField("value", BinaryType(), True),
            StructField("topic", StringType(), True),
            StructField("partition", IntegerType(), True),
            StructField("offset", LongType(), True),
            StructField("timestamp", TimestampType(), True),
        ]
    )


def source_row(value: str, offset: int) -> tuple[object, ...]:
    return (
        f"key-{offset}".encode(),
        value.encode(),
        "quality.smoke",
        0,
        offset,
        datetime(2026, 7, 25, 1, 2, 3),
    )


def test_classifier_output_is_fifteen_columns_and_keeps_three_rows(
    spark: SparkSession,
) -> None:
    valid = {
        "exchange": "binance",
        "symbol": "BTCUSDT",
        "trade_id": "1",
        "price": "1.25",
        "quantity": "2",
        "event_time_ms": 1,
        "ingested_at_ms": 2,
    }
    invalid_price = {**valid, "trade_id": "3", "price": "not-decimal"}
    rows = [
        source_row(json.dumps(valid), 1),
        source_row("{bad-json", 2),
        source_row(json.dumps(invalid_price), 3),
    ]
    source = spark.createDataFrame(rows, source_schema())

    classified = classify_raw_trade_kafka_messages(source)
    output = classified.orderBy("kafka_offset").collect()

    assert classified.columns == expected_names()
    assert len(output) == 3
    assert [row.is_valid for row in output] == [True, False, False]
    assert output[1].validation_errors == ["MALFORMED_JSON"]
    assert output[2].validation_errors == ["INVALID_PRICE"]
    assert all(row.validation_errors is not None for row in output)
    assert [row.kafka_offset for row in output] == [1, 2, 3]
    assert all(row.raw_json is not None for row in output)
