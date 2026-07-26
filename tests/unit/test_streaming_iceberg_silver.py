"""Tests for the bounded deterministic Silver trade build."""

from types import SimpleNamespace
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession, functions as F

from jobs.streaming.iceberg_bronze import (
    BRONZE_TRADE_COLUMNS,
    QUALITY_BRONZE_COLUMNS,
)
from jobs.streaming.iceberg_silver import (
    SILVER_TRADE_COLUMNS,
    SILVER_TRADES_TABLE_NAME,
    SilverBuildError,
    build_silver_trades_dataframe,
    rebuild_silver_trades,
)


class FakeBronzeFrame:
    columns = [name for name, _ in QUALITY_BRONZE_COLUMNS]

    def where(self, expression):
        self.where_expression = expression
        return self

    def select(self, *expressions):
        self.selected_expressions = expressions
        return self


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("silver-unit-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_silver_contract_is_canonical_and_exactly_ordered() -> None:
    assert SILVER_TRADES_TABLE_NAME == "market_catalog.market.silver_trades"
    assert SILVER_TRADE_COLUMNS == (
        ("exchange", "STRING"),
        ("symbol", "STRING"),
        ("trade_id", "STRING"),
        ("price", "DECIMAL(38, 18)"),
        ("quantity", "DECIMAL(38, 18)"),
        ("notional", "DECIMAL(38, 18)"),
        ("event_time", "TIMESTAMP"),
        ("ingested_at", "TIMESTAMP"),
        ("latency_ms", "BIGINT"),
        ("kafka_topic", "STRING"),
        ("kafka_partition", "INT"),
        ("kafka_offset", "BIGINT"),
    )


def test_silver_transformation_requires_exact_bronze_columns() -> None:
    frame = SimpleNamespace(columns=[name for name, _ in BRONZE_TRADE_COLUMNS])
    with pytest.raises(SilverBuildError, match="exact quality-contract"):
        build_silver_trades_dataframe(frame)


def test_rebuild_rejects_noncanonical_source_before_sql() -> None:
    with pytest.raises(SilverBuildError, match="Silver source"):
        rebuild_silver_trades(object(), bronze_table="other.table")


def test_rebuild_rejects_noncanonical_target_before_sql() -> None:
    with pytest.raises(SilverBuildError, match="Silver target"):
        rebuild_silver_trades(object(), silver_table="other.table")


def test_transformation_builds_valid_row_filter_and_selected_contract(spark) -> None:
    frame = spark.createDataFrame(
        [("binance", "BTCUSDT", "1", Decimal("2.5"), Decimal("4"), True, 1000, 2500)],
        "exchange string, symbol string, trade_id string, "
        "price decimal(38,18), quantity decimal(38,18), is_valid boolean, "
        "event_time_ms bigint, ingested_at_ms bigint",
    )
    # Add the remaining contract fields with nulls for expression/schema coverage.
    for name, data_type in QUALITY_BRONZE_COLUMNS:
        if name not in frame.columns:
            frame = frame.withColumn(name, F.lit(None).cast(data_type))
    result = build_silver_trades_dataframe(frame.select(
        [name for name, _ in QUALITY_BRONZE_COLUMNS]
    ))
    assert result.columns == [name for name, _ in SILVER_TRADE_COLUMNS]
    row = result.collect()[0]
    assert row.notional == Decimal("10.000000000000000000")
    assert row.latency_ms == 1500


def _bronze_rows(*states: bool | None):
    values = []
    for index, state in enumerate(states):
        values.append(
            (
                "binance", "BTCUSDT", str(index), Decimal("2.5"),
                Decimal("4"), 1000 + index, 2500 + index, "key",
                "market.trades.raw", 0, index, None, "{}", state, [],
            )
        )
    return values


def test_only_true_quality_rows_are_materialized_and_coordinates_preserved(spark) -> None:
    schema = (
        "exchange string, symbol string, trade_id string, "
        "price decimal(38,18), quantity decimal(38,18), event_time_ms bigint, "
        "ingested_at_ms bigint, kafka_key string, kafka_topic string, "
        "kafka_partition int, kafka_offset bigint, kafka_timestamp timestamp, "
        "raw_json string, is_valid boolean, validation_errors array<string>"
    )
    frame = spark.createDataFrame(_bronze_rows(True, False, None), schema)
    result = build_silver_trades_dataframe(frame)
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0].trade_id == "0"
    assert rows[0].kafka_offset == 0
    assert rows[0].latency_ms == 1500
    assert rows[0].event_time.microsecond == 0


def test_valid_rows_with_reused_coordinates_are_preserved(spark) -> None:
    schema = (
        "exchange string, symbol string, trade_id string, "
        "price decimal(38,18), quantity decimal(38,18), event_time_ms bigint, "
        "ingested_at_ms bigint, kafka_key string, kafka_topic string, "
        "kafka_partition int, kafka_offset bigint, kafka_timestamp timestamp, "
        "raw_json string, is_valid boolean, validation_errors array<string>"
    )
    rows = [
        ("binance", "BTCUSDT", "epoch-one", Decimal("2"), Decimal("3"), 1000, 2000, "k1", "market.trades.raw", 0, 0, None, "{}", True, []),
        ("binance", "SOLUSDT", "epoch-two", Decimal("5"), Decimal("7"), 3000, 5000, "k2", "market.trades.raw", 0, 0, None, "{}", True, []),
    ]
    frame = spark.createDataFrame(rows, schema)
    result = build_silver_trades_dataframe(frame)
    output = result.select("symbol", "trade_id", "kafka_offset").orderBy("symbol").collect()
    assert [(row.symbol, row.trade_id, row.kafka_offset) for row in output] == [
        ("BTCUSDT", "epoch-one", 0),
        ("SOLUSDT", "epoch-two", 0),
    ]


def test_decimal_overflow_fails_with_ansi_enabled(spark) -> None:
    schema = (
        "exchange string, symbol string, trade_id string, "
        "price decimal(38,18), quantity decimal(38,18), event_time_ms bigint, "
        "ingested_at_ms bigint, kafka_key string, kafka_topic string, "
        "kafka_partition int, kafka_offset bigint, kafka_timestamp timestamp, "
        "raw_json string, is_valid boolean, validation_errors array<string>"
    )
    frame = spark.createDataFrame(
        _bronze_rows(True), schema
    ).withColumn(
        "price", F.lit(
            Decimal("99999999999999999999.999999999999999999")
        ).cast("decimal(38,18)")
    ).withColumn(
        "quantity", F.lit(
            Decimal("2")
        ).cast("decimal(38,18)")
    )
    with pytest.raises(Exception):
        build_silver_trades_dataframe(frame).collect()


def test_rebuild_uses_create_or_replace_and_counts_result(monkeypatch) -> None:
    class Writer:
        def using(self, value):
            assert value == "iceberg"
            return self

        def createOrReplace(self):
            calls.append("createOrReplace")

    calls: list[str] = []

    Frame = object()
    monkeypatch.setattr(
        "jobs.streaming.iceberg_silver.build_silver_trades_dataframe",
        lambda frame: SimpleNamespace(
            writeTo=lambda table: Writer(),
        ),
    )

    class Spark:
        def sql(self, query):
            assert query == "DESCRIBE TABLE market_catalog.market.bronze_trades"

            class Description:
                def collect(self):
                    return [
                        {"col_name": name, "data_type": data_type.lower()}
                        for name, data_type in QUALITY_BRONZE_COLUMNS
                    ]

            return Description()

        def table(self, name):
            if name == SILVER_TRADES_TABLE_NAME and calls:
                return SimpleNamespace(count=lambda: 184)
            assert name == "market_catalog.market.bronze_trades"
            return Frame

    assert rebuild_silver_trades(Spark()) == 184
    assert calls == ["createOrReplace"]
