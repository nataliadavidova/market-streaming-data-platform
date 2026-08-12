"""Unit tests for snapshot-bound Silver source validation."""

from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from jobs.serving import silver_source_validation as validation


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("silver-source-validation-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def _valid_row(symbol: str = "BTCUSDT", trade_id: str = "1") -> tuple[object, ...]:
    timestamp = datetime(2026, 1, 1, 0, 0, 0)
    return (
        "binance",
        symbol,
        trade_id,
        Decimal("2.000000000000000000"),
        Decimal("3.000000000000000000"),
        Decimal("6.000000000000000000"),
        timestamp,
        timestamp,
        1000,
        "market.trades.raw",
        0,
        1,
    )


def _dataframe(spark: SparkSession, rows: list[tuple[object, ...]]) -> object:
    return spark.createDataFrame(rows, validation.EXPECTED_SILVER_SCHEMA)


def _values_for_fields(fields: list[StructField]) -> tuple[object, ...]:
    values = dict(zip(validation.EXPECTED_SILVER_COLUMNS, _valid_row()))
    return tuple(values.get(field.name) for field in fields)


class RecordingReader:
    def __init__(
        self,
        dataframe: object,
        *,
        error: Exception | None = None,
        snapshot_dataframes: dict[int, object] | None = None,
    ) -> None:
        self.dataframe = dataframe
        self.error = error
        self.snapshot_dataframes = snapshot_dataframes
        self.formats: list[str] = []
        self.options: dict[str, str] = {}
        self.loaded_tables: list[str] = []

    def format(self, source_format: str) -> "RecordingReader":
        self.formats.append(source_format)
        return self

    def option(self, key: str, value: str) -> "RecordingReader":
        self.options[key] = value
        return self

    def load(self, table: str) -> object:
        self.loaded_tables.append(table)
        if self.error is not None:
            raise self.error
        if self.snapshot_dataframes is not None:
            return self.snapshot_dataframes[int(self.options["versionAsOf"])]
        return self.dataframe


class SnapshotSpark:
    def __init__(self, dataframe: object, snapshot_id: int | None) -> None:
        self.dataframe = dataframe
        self.snapshot_id = snapshot_id
        self.history_queries: list[str] = []
        self.reader = RecordingReader(dataframe)

    @property
    def read(self) -> RecordingReader:
        return self.reader

    def sql(self, query: str) -> SimpleNamespace:
        self.history_queries.append(query)
        if self.snapshot_id is None:
            return SimpleNamespace(collect=lambda: [])
        return SimpleNamespace(
            collect=lambda: [{"snapshot_id": self.snapshot_id}]
        )


def _zero_null_counts() -> dict[str, int]:
    return {column: 0 for column in validation.EXPECTED_SILVER_COLUMNS}


def test_exact_schema_succeeds(spark: SparkSession) -> None:
    dataframe = _dataframe(spark, [_valid_row()])

    validation.validate_silver_schema(dataframe)


@pytest.mark.parametrize(
    "fields",
    [
        list(validation.EXPECTED_SILVER_SCHEMA.fields[:-1]),
        [*validation.EXPECTED_SILVER_SCHEMA.fields, StructField("extra", StringType())],
        [
            StructField("symbol", StringType()),
            *validation.EXPECTED_SILVER_SCHEMA.fields[1:],
        ],
        [
            StructField("price", DecimalType(38, 17)),
            *validation.EXPECTED_SILVER_SCHEMA.fields[1:],
        ],
    ],
)
def test_schema_mismatch_fails_before_source_is_valid(
    spark: SparkSession,
    fields: list[StructField],
) -> None:
    dataframe = spark.createDataFrame(
        [_values_for_fields(fields)],
        StructType(fields),
    )

    with pytest.raises(validation.SilverSourceValidationError, match="schema mismatch"):
        validation.validate_silver_schema(dataframe)


@pytest.mark.parametrize("column", validation.EXPECTED_SILVER_COLUMNS)
def test_null_in_each_logical_column_family_is_detected(
    spark: SparkSession,
    column: str,
) -> None:
    values = list(_valid_row())
    values[validation.EXPECTED_SILVER_COLUMNS.index(column)] = None

    with pytest.raises(validation.SilverSourceValidationError, match=column):
        validation.validate_required_nulls(_dataframe(spark, [tuple(values)]))


def test_zero_nulls_succeeds_and_multiple_bad_columns_are_deterministic(
    spark: SparkSession,
) -> None:
    assert validation.validate_required_nulls(
        _dataframe(spark, [_valid_row()])
    ) == _zero_null_counts()

    values = list(_valid_row())
    values[0] = None
    values[3] = None
    with pytest.raises(
        validation.SilverSourceValidationError,
        match="exchange=1, price=1",
    ):
        validation.validate_required_nulls(_dataframe(spark, [tuple(values)]))


def test_source_summary_counts_duplicates_and_bounds_sorted_symbols(
    spark: SparkSession,
) -> None:
    dataframe = _dataframe(
        spark,
        [
            _valid_row("SOLUSDT", "1"),
            _valid_row("BTCUSDT", "2"),
            _valid_row("BTCUSDT", "3"),
            _valid_row("ETHUSDT", "4"),
        ],
    )
    summary = validation.compute_source_summary(
        dataframe,
        table_name=validation.DEFAULT_SILVER_TABLE,
        snapshot_id=101,
        null_counts=_zero_null_counts(),
        max_display_symbols=2,
    )

    assert summary.row_count == 4
    assert summary.distinct_symbol_count == 3
    assert summary.symbols == ("BTCUSDT", "ETHUSDT", "SOLUSDT")
    assert summary.per_symbol_counts == (
        ("BTCUSDT", 2),
        ("ETHUSDT", 1),
        ("SOLUSDT", 1),
    )
    assert summary.displayed_symbols == ("BTCUSDT", "ETHUSDT")
    assert summary.omitted_symbol_count == 1


def test_empty_snapshot_data_is_a_valid_zero_metric_summary(
    spark: SparkSession,
) -> None:
    summary = validation.compute_source_summary(
        _dataframe(spark, []),
        table_name=validation.DEFAULT_SILVER_TABLE,
        snapshot_id=101,
        null_counts=_zero_null_counts(),
    )

    assert summary.row_count == 0
    assert summary.distinct_symbol_count == 0
    assert summary.symbols == ()
    assert summary.per_symbol_counts == ()


def test_no_snapshot_fails() -> None:
    spark = SnapshotSpark(dataframe=object(), snapshot_id=None)

    with pytest.raises(validation.SilverSourceValidationError, match="no current snapshot"):
        validation.resolve_current_snapshot_id(spark)


def test_explicit_snapshot_read_uses_snapshot_id_and_safe_table() -> None:
    spark = SnapshotSpark(dataframe=object(), snapshot_id=123)

    result = validation.read_silver_snapshot(
        spark,
        123,
        validation.DEFAULT_SILVER_TABLE,
    )

    assert result is spark.dataframe
    assert spark.reader.formats == ["iceberg"]
    assert spark.reader.options == {"versionAsOf": "123"}
    assert spark.reader.loaded_tables == [validation.DEFAULT_SILVER_TABLE]


def test_snapshot_binding_resolves_once_and_metrics_use_explicit_snapshot(
    spark: SparkSession,
) -> None:
    dataframe = _dataframe(spark, [_valid_row()])
    fake_spark = SnapshotSpark(dataframe, snapshot_id=456)

    summary = validation.inspect_silver_source(fake_spark)
    fake_spark.snapshot_id = 789

    assert summary.snapshot_id == 456
    assert len(fake_spark.history_queries) == 1
    assert fake_spark.reader.options == {"versionAsOf": "456"}
    assert summary.row_count == 1


def test_snapshot_binding_survives_current_snapshot_change_before_read(
    spark: SparkSession,
) -> None:
    snapshot_a = 456
    snapshot_b = 789
    dataframe_a = _dataframe(spark, [_valid_row("BTCUSDT", "a")])
    dataframe_b = _dataframe(
        spark,
        [
            _valid_row("ETHUSDT", "b1"),
            _valid_row("ETHUSDT", "b2"),
        ],
    )
    fake_spark = SnapshotSpark(dataframe_a, snapshot_id=snapshot_a)
    fake_spark.reader = RecordingReader(
        dataframe_a,
        snapshot_dataframes={snapshot_a: dataframe_a, snapshot_b: dataframe_b},
    )

    original_sql = fake_spark.sql

    def resolve_then_advance(query: str) -> SimpleNamespace:
        result = original_sql(query)
        original_collect = result.collect

        def collect() -> list[dict[str, int]]:
            rows = original_collect()
            fake_spark.snapshot_id = snapshot_b
            return rows

        result.collect = collect
        return result

    fake_spark.sql = resolve_then_advance  # type: ignore[method-assign]

    summary = validation.inspect_silver_source(fake_spark)

    assert fake_spark.reader.options == {"versionAsOf": str(snapshot_a)}
    assert summary.snapshot_id == snapshot_a
    assert summary.row_count == 1
    assert summary.symbols == ("BTCUSDT",)
    assert summary.per_symbol_counts == (("BTCUSDT", 1),)


def test_snapshot_read_failure_is_bounded() -> None:
    fake_spark = SnapshotSpark(dataframe=object(), snapshot_id=456)
    fake_spark.reader = RecordingReader(object(), error=RuntimeError("read failed"))

    with pytest.raises(validation.SilverSourceValidationError, match="could not read"):
        validation.read_silver_snapshot(fake_spark, 456)


def test_cli_summary_is_bounded_and_has_no_row_payload_or_credentials(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    summary = validation.SilverSourceSummary(
        table_name=validation.DEFAULT_SILVER_TABLE,
        snapshot_id=456,
        row_count=184,
        distinct_symbol_count=3,
        symbols=("BTCUSDT", "ETHUSDT", "SOLUSDT"),
        per_symbol_counts=(("BTCUSDT", 162), ("ETHUSDT", 13), ("SOLUSDT", 9)),
        null_counts=tuple((column, 0) for column in validation.EXPECTED_SILVER_COLUMNS),
        displayed_symbols=("BTCUSDT", "ETHUSDT"),
        displayed_per_symbol_counts=(("BTCUSDT", 162), ("ETHUSDT", 13)),
        omitted_symbol_count=1,
    )
    monkeypatch.setattr(
        validation,
        "run_silver_source_inspection",
        lambda **_: summary,
    )

    assert validation.main(
        ["inspect"],
        environ={"CLICKHOUSE_PASSWORD": "secret-value"},
    ) == 0
    output = capsys.readouterr()
    assert "snapshot_id=456" in output.out
    assert "per_symbol_counts=BTCUSDT:162,ETHUSDT:13" in output.out
    assert "secret-value" not in output.out + output.err
    assert "row payload" not in output.out


@pytest.mark.parametrize(
    "error",
    [
        validation.SilverSourceValidationError("schema mismatch"),
        validation.SilverSourceValidationError("NULL validation failed: price=1"),
        validation.SilverSourceValidationError("no current snapshot"),
    ],
)
def test_cli_validation_failures_exit_nonzero(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    error: Exception,
) -> None:
    def fail(**_: object) -> None:
        raise error

    monkeypatch.setattr(validation, "run_silver_source_inspection", fail)

    assert validation.main(["inspect"]) == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert "error:" in output.err
