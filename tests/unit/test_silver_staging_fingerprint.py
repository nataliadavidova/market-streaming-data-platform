"""Unit tests for explicit-snapshot Silver-to-staging fingerprints."""

from datetime import datetime, timezone
from decimal import Decimal
import inspect
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

from jobs.serving import clickhouse_schema
from jobs.serving import silver_staging_fingerprint as fingerprint


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("silver-staging-fingerprint-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def _row(
    *,
    symbol: str = "BTCUSDT",
    trade_id: str = "1",
    price: Decimal = Decimal("2.000000000000000000"),
    event_time: datetime | None = None,
    partition: int = 0,
    offset: int = 1,
) -> tuple[object, ...]:
    timestamp = event_time or datetime(2026, 1, 1, 0, 0, 0)
    return (
        "binance",
        symbol,
        trade_id,
        price,
        Decimal("3.000000000000000000"),
        Decimal("6.000000000000000000"),
        timestamp,
        timestamp,
        -1000,
        "market.trades.raw",
        partition,
        offset,
    )


def _dataframe(spark: SparkSession, rows: list[tuple[object, ...]]):
    return spark.createDataFrame(rows, fingerprint_schema())


def fingerprint_schema() -> StructType:
    return StructType(
        [
            StructField("exchange", StringType()),
            StructField("symbol", StringType()),
            StructField("trade_id", StringType()),
            StructField("price", DecimalType(38, 18)),
            StructField("quantity", DecimalType(38, 18)),
            StructField("notional", DecimalType(38, 18)),
            StructField("event_time", TimestampType()),
            StructField("ingested_at", TimestampType()),
            StructField("latency_ms", LongType()),
            StructField("kafka_topic", StringType()),
            StructField("kafka_partition", IntegerType()),
            StructField("kafka_offset", LongType()),
        ]
    )


def _config() -> clickhouse_schema.ClickHouseConfig:
    return clickhouse_schema.ClickHouseConfig(
        host="localhost",
        http_port=18123,
        database="market_analytics",
        user="market_loader",
        password="secret-value",
    )


def test_canonical_json_is_unambiguous_and_column_ordered(spark: SparkSession) -> None:
    first = _dataframe(spark, [_row(symbol="a|b", trade_id="c")])
    second = _dataframe(spark, [_row(symbol="a", trade_id="b|c")])

    first_json = fingerprint.canonicalize_silver_rows(first).collect()[0][0]
    second_json = fingerprint.canonicalize_silver_rows(second).collect()[0][0]

    assert first_json != second_json
    assert first_json.index('"symbol"') < first_json.index('"trade_id"')
    assert '"symbol":"a|b"' in first_json


def test_decimal_sign_scale_and_timestamp_are_canonicalized_deterministically(
    spark: SparkSession,
) -> None:
    dataframe = _dataframe(
        spark,
        [
            _row(
                price=Decimal("-2.000000000000000000"),
                event_time=datetime(
                    2026,
                    1,
                    1,
                    0,
                    0,
                    0,
                    123456,
                    tzinfo=timezone.utc,
                ),
            )
        ],
    )

    canonical = fingerprint.canonicalize_silver_rows(dataframe).collect()[0][0]

    assert '"price":"-2.000000000000000000"' in canonical
    assert '"event_time":"2026-01-01T00:00:00.123Z"' in canonical


@pytest.mark.parametrize(
    "value",
    [
        (2_147_483_647, 9_223_372_036_854_775_807),
        (-2_147_483_648, -9_223_372_036_854_775_808),
    ],
)
def test_signed_integer_values_are_preserved(
    spark: SparkSession,
    value: tuple[int, int],
) -> None:
    row = list(_row())
    row[8] = value[1]
    row[10] = value[0]
    canonical = fingerprint.canonicalize_silver_rows(
        _dataframe(spark, [tuple(row)])
    ).collect()[0][0]

    assert f'"latency_ms":"{value[1]}"' in canonical
    assert f'"kafka_partition":"{value[0]}"' in canonical


def test_duplicate_identical_rows_have_identical_row_hashes(spark: SparkSession) -> None:
    dataframe = _dataframe(spark, [_row(), _row()])

    hashes = [row[fingerprint.ROW_HASH_COLUMN] for row in fingerprint.add_canonical_row_hash(dataframe).collect()]

    assert len(hashes) == 2
    assert hashes[0] == hashes[1]


def test_multiset_fingerprint_is_order_independent_and_repeatable(
    spark: SparkSession,
) -> None:
    rows = [_row(trade_id="1"), _row(trade_id="2"), _row(trade_id="2")]
    first = fingerprint.compute_row_multiset_fingerprint(_dataframe(spark, rows))
    reordered = fingerprint.compute_row_multiset_fingerprint(
        _dataframe(spark, [rows[2], rows[0], rows[1]])
    )
    repeated = fingerprint.compute_row_multiset_fingerprint(_dataframe(spark, rows))

    assert first == reordered == repeated


def test_multiset_fingerprint_changes_when_duplicate_multiplicity_changes(
    spark: SparkSession,
) -> None:
    one = fingerprint.compute_row_multiset_fingerprint(
        _dataframe(spark, [_row(trade_id="1"), _row(trade_id="2")])
    )
    two = fingerprint.compute_row_multiset_fingerprint(
        _dataframe(spark, [_row(trade_id="1"), _row(trade_id="2"), _row(trade_id="2")])
    )

    assert one != two


def test_empty_dataset_fingerprint_is_deterministic(spark: SparkSession) -> None:
    empty = _dataframe(spark, [])

    assert fingerprint.compute_row_multiset_fingerprint(empty) == fingerprint.compute_row_multiset_fingerprint(empty)


def test_fingerprint_consumes_ordered_pairs_incrementally() -> None:
    source = inspect.getsource(fingerprint.compute_row_multiset_fingerprint)

    assert "collect_list" not in source
    assert ".collect(" not in source

    class StreamingRows:
        def __iter__(self):
            for index in range(10_000):
                yield {
                    fingerprint.ROW_HASH_COLUMN: f"{index:064x}",
                    "multiplicity": 1,
                }

    result = fingerprint._fingerprint_ordered_pairs(StreamingRows())

    assert len(result) == 64


def test_pair_encoding_is_unambiguous_and_deterministic() -> None:
    first = fingerprint._encode_hash_count_pair("ab", 12)
    second = fingerprint._encode_hash_count_pair("a", 212)

    assert first != second
    assert fingerprint._encode_hash_count_pair("ab", 12) == first


@pytest.mark.parametrize(
    "fields",
    [
        list(fingerprint_schema().fields[:-1]),
        [*fingerprint_schema().fields, StructField("extra", StringType())],
        [StructField("symbol", StringType()), *fingerprint_schema().fields[1:]],
        [StructField("price", DecimalType(38, 17)), *fingerprint_schema().fields[1:]],
        [StructField("event_time", StringType()), *fingerprint_schema().fields[1:]],
    ],
)
def test_jdbc_read_schema_requires_exact_contract(
    spark: SparkSession,
    fields: list[StructField],
) -> None:
    values = dict(zip(fingerprint.EXPECTED_SILVER_COLUMNS, _row()))
    dataframe = spark.createDataFrame(
        [tuple(values.get(field.name) for field in fields)],
        StructType(fields),
    )

    with pytest.raises(Exception, match="schema mismatch"):
        fingerprint.validate_silver_schema(dataframe)


def test_staging_jdbc_read_uses_exact_http_options() -> None:
    class Reader:
        def __init__(self) -> None:
            self.options: dict[str, str] = {}
            self.format_name = ""

        def format(self, value: str) -> "Reader":
            self.format_name = value
            return self

        def option(self, key: str, value: str) -> "Reader":
            self.options[key] = value
            return self

        def load(self) -> object:
            return self

    reader = Reader()
    spark = SimpleNamespace(read=reader)

    result = fingerprint.read_staging_table(spark, _config())

    assert result is reader
    assert reader.format_name == "jdbc"
    assert reader.options == {
        "url": "jdbc:clickhouse://localhost:18123/market_analytics?session_timezone=UTC",
        "dbtable": "market_analytics.silver_trades_staging",
        "driver": "com.clickhouse.jdbc.Driver",
        "user": "market_loader",
        "password": "secret-value",
    }


def test_validation_keeps_snapshot_a_when_current_advances_to_b(
    monkeypatch: pytest.MonkeyPatch,
    spark: SparkSession,
) -> None:
    snapshot_a = 456
    snapshot_b = 789
    source_a = _dataframe(spark, [_row(symbol="AAA", trade_id="a")])
    source_b = _dataframe(spark, [_row(symbol="BBB", trade_id="b")])
    staging = _dataframe(spark, [_row(symbol="AAA", trade_id="a")])
    current_snapshot = {"id": snapshot_a}
    requested: list[int] = []

    def read_explicit_snapshot(_spark, snapshot_id, _table):
        requested.append(snapshot_id)
        current_snapshot["id"] = snapshot_b
        return source_a if snapshot_id == snapshot_a else source_b

    monkeypatch.setattr(
        fingerprint,
        "read_silver_snapshot",
        read_explicit_snapshot,
    )
    monkeypatch.setattr(fingerprint, "read_staging_table", lambda *_: staging)
    monkeypatch.setattr(
        fingerprint,
        "resolve_current_snapshot_id",
        lambda *_: (_ for _ in ()).throw(AssertionError("latest was resolved")),
        raising=False,
    )

    result = fingerprint.validate_staging_against_snapshot(
        spark,
        _config(),
        snapshot_a,
    )

    assert current_snapshot["id"] == snapshot_b
    assert requested == [snapshot_a]
    assert result.snapshot_id == snapshot_a
    assert result.source_symbols == ("AAA",)
    assert result.source_row_count == 1
    assert result.exact_copy is True


def test_exact_copy_succeeds_and_mismatch_fails_without_mutation(
    monkeypatch: pytest.MonkeyPatch,
    spark: SparkSession,
) -> None:
    source = _dataframe(spark, [_row()])
    changed = _dataframe(spark, [_row(price=Decimal("2.000000000000000001"))])
    monkeypatch.setattr(fingerprint, "read_silver_snapshot", lambda *_: source)
    monkeypatch.setattr(fingerprint, "read_staging_table", lambda *_: changed)

    with pytest.raises(fingerprint.SilverStagingFingerprintError, match="fingerprints differ"):
        fingerprint.validate_staging_against_snapshot(spark, _config(), 456)

    assert not hasattr(fingerprint, "truncate_staging")
    assert not hasattr(fingerprint, "exchange_tables")


def test_cli_output_is_bounded_and_credentials_are_absent(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    result = fingerprint.SilverStagingFingerprintResult(
        source_table="market_catalog.market.silver_trades",
        snapshot_id=456,
        staging_table="market_analytics.silver_trades_staging",
        source_row_count=184,
        staging_row_count=184,
        source_distinct_symbol_count=3,
        staging_distinct_symbol_count=3,
        source_symbols=("BTCUSDT", "ETHUSDT", "SOLUSDT"),
        staging_symbols=("BTCUSDT", "ETHUSDT", "SOLUSDT"),
        source_per_symbol_counts=(("BTCUSDT", 164), ("ETHUSDT", 13), ("SOLUSDT", 7)),
        staging_per_symbol_counts=(("BTCUSDT", 164), ("ETHUSDT", 13), ("SOLUSDT", 7)),
        source_fingerprint="a" * 64,
        staging_fingerprint="a" * 64,
        exact_copy=True,
        validation_status="exact_copy_verified",
    )
    monkeypatch.setattr(fingerprint, "run_validation", lambda **_: result)

    assert fingerprint.main(
        ["validate", "--snapshot-id", "456"],
        environ={"CLICKHOUSE_PASSWORD": "secret-value"},
    ) == 0
    output = capsys.readouterr()
    assert "exact_copy=true" in output.out
    assert "secret-value" not in output.out + output.err
    assert "source_per_symbol_counts" not in output.out


@pytest.mark.parametrize(
    "error",
    [
        fingerprint.SilverStagingFingerprintError("row counts differ"),
        fingerprint.SilverStagingFingerprintError("schema mismatch"),
        fingerprint.SilverStagingFingerprintError("fingerprints differ"),
        fingerprint.SilverSourceValidationError("source schema mismatch"),
    ],
)
def test_cli_validation_failures_are_nonzero_and_bounded(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    error: Exception,
) -> None:
    monkeypatch.setattr(
        fingerprint,
        "run_validation",
        lambda **_: (_ for _ in ()).throw(error),
    )

    assert fingerprint.main(["validate", "--snapshot-id", "456"]) == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert "error:" in output.err
