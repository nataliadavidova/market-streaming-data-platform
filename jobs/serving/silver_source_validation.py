"""Validate one immutable Iceberg Silver snapshot for a serving attempt."""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
import os
import sys
from typing import Any, Protocol

from pyspark.sql import DataFrame, functions as F
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

from jobs.streaming.iceberg_inspection import validate_table_identifier
from jobs.streaming.iceberg_silver import SILVER_TRADES_TABLE_NAME
from jobs.streaming.iceberg_trade_streaming_job import (
    build_iceberg_trade_spark_session,
    parse_args as parse_streaming_args,
)


DEFAULT_SILVER_TABLE = SILVER_TRADES_TABLE_NAME
DEFAULT_MAX_DISPLAY_SYMBOLS = 20
SPARK_ICEBERG_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2,"
    "org.apache.hadoop:hadoop-aws:3.4.2,"
    "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0,"
    "org.apache.iceberg:iceberg-aws-bundle:1.11.0"
)
EXPECTED_SILVER_SCHEMA = StructType(
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
EXPECTED_SILVER_COLUMNS = tuple(field.name for field in EXPECTED_SILVER_SCHEMA)


class SilverSourceValidationError(RuntimeError):
    """Raised when one snapshot cannot satisfy the Silver source contract."""


class SnapshotReader(Protocol):
    def format(self, source_format: str) -> "SnapshotReader":
        ...

    def option(self, key: str, value: str) -> "SnapshotReader":
        ...

    def load(self, table: str) -> DataFrame:
        ...


class SparkSource(Protocol):
    @property
    def read(self) -> SnapshotReader:
        ...

    def sql(self, query: str) -> Any:
        ...


@dataclass(frozen=True)
class SilverSourceSummary:
    """Structured validation and source metrics for one Silver snapshot."""

    table_name: str
    snapshot_id: int
    row_count: int
    distinct_symbol_count: int
    symbols: tuple[str, ...]
    per_symbol_counts: tuple[tuple[str, int], ...]
    null_counts: tuple[tuple[str, int], ...]
    displayed_symbols: tuple[str, ...]
    displayed_per_symbol_counts: tuple[tuple[str, int], ...]
    omitted_symbol_count: int


def _row_value(row: object, name: str, position: int) -> object:
    if isinstance(row, Mapping):
        return row[name]
    try:
        return row[name]  # type: ignore[index]
    except (KeyError, IndexError, TypeError):
        return row[position]  # type: ignore[index]


def _validate_max_display_symbols(value: int) -> int:
    if value <= 0:
        raise SilverSourceValidationError(
            "max displayed symbols must be a positive integer"
        )
    return value


def resolve_current_snapshot_id(
    spark: SparkSource,
    table_name: str = DEFAULT_SILVER_TABLE,
) -> int:
    """Resolve the current snapshot once from Iceberg's history relation."""
    table = validate_table_identifier(table_name)
    query = (
        f"SELECT snapshot_id FROM {table}.history "
        "WHERE is_current_ancestor = true "
        "ORDER BY made_current_at DESC, snapshot_id DESC LIMIT 1"
    )
    try:
        rows = spark.sql(query).collect()
    except Exception as exc:
        raise SilverSourceValidationError(
            f"could not resolve current Silver snapshot for {table}"
        ) from exc
    if not rows:
        raise SilverSourceValidationError(
            f"Silver table {table} has no current snapshot"
        )
    try:
        snapshot_id = int(_row_value(rows[0], "snapshot_id", 0))
    except (TypeError, ValueError, KeyError, IndexError) as exc:
        raise SilverSourceValidationError(
            f"Silver table {table} returned an invalid snapshot ID"
        ) from exc
    if snapshot_id <= 0:
        raise SilverSourceValidationError(
            f"Silver table {table} returned an invalid snapshot ID"
        )
    return snapshot_id


def read_silver_snapshot(
    spark: SparkSource,
    snapshot_id: int,
    table_name: str = DEFAULT_SILVER_TABLE,
) -> DataFrame:
    """Read one explicit Iceberg snapshot using the supported Spark reader option."""
    table = validate_table_identifier(table_name)
    if snapshot_id <= 0:
        raise SilverSourceValidationError("snapshot_id must be a positive integer")
    try:
        return (
            spark.read.format("iceberg")
            .option("versionAsOf", str(snapshot_id))
            .load(table)
        )
    except Exception as exc:
        raise SilverSourceValidationError(
            f"could not read Silver snapshot {snapshot_id} for {table}"
        ) from exc


def validate_silver_schema(dataframe: DataFrame) -> None:
    """Require the exact ordered Silver names and Spark data types."""
    actual = [
        (field.name, field.dataType.simpleString())
        for field in dataframe.schema.fields
    ]
    expected = [
        (field.name, field.dataType.simpleString())
        for field in EXPECTED_SILVER_SCHEMA.fields
    ]
    if actual != expected:
        raise SilverSourceValidationError(
            "Silver schema mismatch: expected the exact 12-column ordered contract"
        )


def validate_required_nulls(dataframe: DataFrame) -> dict[str, int]:
    """Require zero NULL values in every approved Silver column."""
    expressions = [
        F.count(F.when(F.col(column).isNull(), 1)).alias(column)
        for column in EXPECTED_SILVER_COLUMNS
    ]
    try:
        row = dataframe.agg(*expressions).collect()[0]
        counts = {
            column: int(row[column])
            for column in EXPECTED_SILVER_COLUMNS
        }
    except Exception as exc:
        raise SilverSourceValidationError(
            "could not validate Silver NULL counts"
        ) from exc
    invalid = [(column, count) for column, count in counts.items() if count]
    if invalid:
        detail = ", ".join(f"{column}={count}" for column, count in invalid)
        raise SilverSourceValidationError(f"Silver NULL validation failed: {detail}")
    return counts


def compute_source_summary(
    dataframe: DataFrame,
    *,
    table_name: str,
    snapshot_id: int,
    null_counts: Mapping[str, int],
    max_display_symbols: int = DEFAULT_MAX_DISPLAY_SYMBOLS,
) -> SilverSourceSummary:
    """Compute all source metrics from the already snapshot-bound DataFrame."""
    limit = _validate_max_display_symbols(max_display_symbols)
    try:
        row_count = int(dataframe.count())
        grouped = (
            dataframe.groupBy("symbol")
            .count()
            .orderBy(F.col("symbol").asc())
            .collect()
        )
    except Exception as exc:
        raise SilverSourceValidationError(
            f"could not compute Silver source metrics for snapshot {snapshot_id}"
        ) from exc
    per_symbol_counts = tuple(
        (
            str(_row_value(row, "symbol", 0)),
            int(_row_value(row, "count", 1)),
        )
        for row in grouped
    )
    symbols = tuple(symbol for symbol, _count in per_symbol_counts)
    displayed_per_symbol_counts = per_symbol_counts[:limit]
    return SilverSourceSummary(
        table_name=validate_table_identifier(table_name),
        snapshot_id=snapshot_id,
        row_count=row_count,
        distinct_symbol_count=len(symbols),
        symbols=symbols,
        per_symbol_counts=per_symbol_counts,
        null_counts=tuple(
            (column, int(null_counts[column]))
            for column in EXPECTED_SILVER_COLUMNS
        ),
        displayed_symbols=tuple(symbols[:limit]),
        displayed_per_symbol_counts=displayed_per_symbol_counts,
        omitted_symbol_count=max(0, len(symbols) - limit),
    )


def inspect_silver_source(
    spark: SparkSource,
    *,
    table_name: str = DEFAULT_SILVER_TABLE,
    max_display_symbols: int = DEFAULT_MAX_DISPLAY_SYMBOLS,
) -> SilverSourceSummary:
    """Bind one snapshot and derive schema, NULL, and metrics from that read."""
    table = validate_table_identifier(table_name)
    snapshot_id = resolve_current_snapshot_id(spark, table)
    dataframe = read_silver_snapshot(spark, snapshot_id, table)
    validate_silver_schema(dataframe)
    null_counts = validate_required_nulls(dataframe)
    return compute_source_summary(
        dataframe,
        table_name=table,
        snapshot_id=snapshot_id,
        null_counts=null_counts,
        max_display_symbols=max_display_symbols,
    )


def _print_summary(summary: SilverSourceSummary, output: Callable[[str], None]) -> None:
    """Print only bounded deterministic summary fields."""
    output(f"table={summary.table_name}")
    output(f"snapshot_id={summary.snapshot_id}")
    output(f"row_count={summary.row_count}")
    output(f"distinct_symbol_count={summary.distinct_symbol_count}")
    output(f"symbols={','.join(summary.displayed_symbols)}")
    output(f"symbols_omitted={summary.omitted_symbol_count}")
    per_symbol = ",".join(
        f"{symbol}:{count}"
        for symbol, count in summary.displayed_per_symbol_counts
    )
    output(f"per_symbol_counts={per_symbol}")
    output("null_validation=passed")


def parse_args(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
) -> argparse.Namespace:
    """Parse the bounded source-inspection CLI arguments."""
    environment = os.environ if environ is None else environ
    parser = argparse.ArgumentParser(
        description="Inspect one immutable Iceberg Silver snapshot"
    )
    parser.add_argument("operation", choices=("inspect",))
    parser.add_argument(
        "--table",
        default=environment.get("ICEBERG_SILVER_TABLE", DEFAULT_SILVER_TABLE),
        type=validate_table_identifier,
    )
    parser.add_argument(
        "--max-symbols",
        type=int,
        default=int(
            environment.get(
                "ICEBERG_SILVER_MAX_DISPLAY_SYMBOLS",
                DEFAULT_MAX_DISPLAY_SYMBOLS,
            )
        ),
    )
    return parser.parse_args(argv)


def run_silver_source_inspection(
    *,
    table_name: str = DEFAULT_SILVER_TABLE,
    max_display_symbols: int = DEFAULT_MAX_DISPLAY_SYMBOLS,
    environ: Mapping[str, str] | None = None,
) -> SilverSourceSummary:
    """Own one configured Spark session for a bounded read-only inspection."""
    environment = os.environ if environ is None else environ
    table = validate_table_identifier(table_name)
    streaming_args = parse_streaming_args(
        ["--table-name", table],
        environ=environment,
    )
    spark_builder = SparkSession.builder.config(
        "spark.jars.packages",
        SPARK_ICEBERG_PACKAGES,
    )
    spark = build_iceberg_trade_spark_session(
        app_name="market-iceberg-silver-source-validation",
        catalog_name=streaming_args.catalog_name,
        catalog_uri=streaming_args.catalog_uri,
        warehouse=streaming_args.warehouse,
        s3_endpoint=streaming_args.s3_endpoint,
        s3_region=streaming_args.s3_region,
        s3_access_key=streaming_args.s3_access_key,
        s3_secret_key=streaming_args.s3_secret_key,
        s3_path_style_access=streaming_args.s3_path_style_access,
        s3a_ssl_enabled=streaming_args.s3a_ssl_enabled,
        builder=spark_builder,
    )
    inspection_error: BaseException | None = None
    try:
        summary = inspect_silver_source(
            spark,
            table_name=table,
            max_display_symbols=max_display_symbols,
        )
        return summary
    except BaseException as error:
        inspection_error = error
        raise
    finally:
        try:
            spark.stop()
        except BaseException as cleanup_error:
            if inspection_error is None:
                raise
            inspection_error.add_note(
                f"Spark session cleanup also failed: {cleanup_error!r}"
            )


def main(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
) -> int:
    """Run the source-validation CLI and return a process exit code."""
    args = parse_args(argv, environ=environ)
    try:
        summary = run_silver_source_inspection(
            table_name=args.table,
            max_display_symbols=args.max_symbols,
            environ=environ,
        )
        _print_summary(summary, print)
    except SilverSourceValidationError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
