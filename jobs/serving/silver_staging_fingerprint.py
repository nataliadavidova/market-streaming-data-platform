"""Validate an explicit Silver snapshot against ClickHouse staging read-only."""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
import hashlib
import os
import sys
from typing import Any

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import DecimalType

from jobs.serving import clickhouse_schema
from jobs.serving.clickhouse_staging_loader import (
    CLICKHOUSE_JDBC_DRIVER,
    DEFAULT_SILVER_TABLE,
    STAGING_TABLE,
    build_jdbc_url,
    _build_spark,
)
from jobs.serving.silver_source_validation import (
    EXPECTED_SILVER_COLUMNS,
    SilverSourceValidationError,
    SilverSourceSummary,
    compute_source_summary,
    read_silver_snapshot,
    validate_required_nulls,
    validate_silver_schema,
)


CANONICAL_ROW_COLUMN = "_canonical_row"
ROW_HASH_COLUMN = "_row_hash"
DATASET_FINGERPRINT_COLUMN = "fingerprint"


class SilverStagingFingerprintError(RuntimeError):
    """Raised when explicit-snapshot staging validation cannot complete."""


@dataclass(frozen=True)
class SilverStagingFingerprintResult:
    """Bounded exact-copy comparison result for one explicit snapshot."""

    source_table: str
    snapshot_id: int
    staging_table: str
    source_row_count: int
    staging_row_count: int
    source_distinct_symbol_count: int
    staging_distinct_symbol_count: int
    source_symbols: tuple[str, ...]
    staging_symbols: tuple[str, ...]
    source_per_symbol_counts: tuple[tuple[str, int], ...]
    staging_per_symbol_counts: tuple[tuple[str, int], ...]
    source_fingerprint: str
    staging_fingerprint: str
    exact_copy: bool
    validation_status: str


def _timestamp_string(column: str) -> F.Column:
    # Spark sessions in this repository use UTC; applying to_utc_timestamp to
    # an already UTC-interpreted TimestampType would shift it a second time.
    return F.date_format(
        F.col(column),
        "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
    )


def _canonical_value(column: str) -> F.Column:
    if column in {"price", "quantity", "notional"}:
        return F.col(column).cast(DecimalType(38, 18)).cast("string")
    if column in {"event_time", "ingested_at"}:
        return _timestamp_string(column)
    return F.col(column).cast("string")


def canonicalize_silver_rows(dataframe: DataFrame) -> DataFrame:
    """Return one unambiguous canonical JSON representation per Silver row.

    JSON struct fields provide explicit names, ordering, and escaping. Decimal
    values use the approved scale, timestamps use UTC millisecond ISO text,
    and signed integer values use locale-independent decimal text.
    """
    fields = [
        _canonical_value(column).alias(column)
        for column in EXPECTED_SILVER_COLUMNS
    ]
    return dataframe.select(
        F.to_json(F.struct(*fields), options={"ignoreNullFields": "false"}).alias(
            CANONICAL_ROW_COLUMN
        )
    )


def add_canonical_row_hash(dataframe: DataFrame) -> DataFrame:
    """Add the shared Spark SHA-256 hash for each canonical Silver row."""
    canonical = canonicalize_silver_rows(dataframe)
    return canonical.withColumn(
        ROW_HASH_COLUMN,
        F.sha2(F.col(CANONICAL_ROW_COLUMN), 256),
    )


def compute_row_multiset_fingerprint(dataframe: DataFrame) -> str:
    """Compute an order-independent SHA-256 fingerprint preserving duplicates.

    Hash/count pairs are aggregated and ordered in Spark, then consumed one
    pair at a time. This avoids materializing all distinct pairs in Python;
    a future hierarchical/bucketed digest could remove the sequential final
    pass if larger-scale serving requires it.
    """
    hash_counts = (
        add_canonical_row_hash(dataframe)
        .groupBy(ROW_HASH_COLUMN)
        .count()
        .withColumnRenamed("count", "multiplicity")
    )
    ordered_pairs = hash_counts.orderBy(F.col(ROW_HASH_COLUMN).asc()).select(
        ROW_HASH_COLUMN,
        F.col("multiplicity").cast("long").alias("multiplicity"),
    )
    return _fingerprint_ordered_pairs(ordered_pairs.toLocalIterator())


def _encode_hash_count_pair(row_hash: str, multiplicity: int) -> bytes:
    """Encode one ordered pair with explicit byte lengths and ASCII values."""
    hash_bytes = row_hash.encode("ascii")
    multiplicity_bytes = str(multiplicity).encode("ascii")
    return (
        str(len(hash_bytes)).encode("ascii")
        + b":"
        + hash_bytes
        + str(len(multiplicity_bytes)).encode("ascii")
        + b":"
        + multiplicity_bytes
    )


def _fingerprint_ordered_pairs(rows: Any) -> str:
    """Hash an ordered pair iterator without retaining the pair sequence."""
    digest = hashlib.sha256()
    for row in rows:
        digest.update(
            _encode_hash_count_pair(
                str(row[ROW_HASH_COLUMN]),
                int(row["multiplicity"]),
            )
        )
    return digest.hexdigest()


def read_staging_table(
    spark: Any,
    config: clickhouse_schema.ClickHouseConfig,
) -> DataFrame:
    """Read only the pre-existing staging table through the ClickHouse JDBC URL."""
    try:
        return (
            spark.read.format("jdbc")
            .option("url", build_jdbc_url(config))
            .option("dbtable", f"{config.database}.{STAGING_TABLE}")
            .option("driver", CLICKHOUSE_JDBC_DRIVER)
            .option("user", config.user)
            .option("password", config.password)
            .load()
        )
    except Exception as exc:
        raise SilverStagingFingerprintError(
            "could not read ClickHouse staging through JDBC"
        ) from exc


def _validated_summary(
    dataframe: DataFrame,
    *,
    table_name: str,
    snapshot_id: int,
) -> SilverSourceSummary:
    validate_silver_schema(dataframe)
    null_counts = validate_required_nulls(dataframe)
    return compute_source_summary(
        dataframe,
        table_name=table_name,
        snapshot_id=snapshot_id,
        null_counts=null_counts,
    )


def _compare_metrics(
    source: SilverSourceSummary,
    staging: SilverSourceSummary,
) -> None:
    if source.row_count != staging.row_count:
        raise SilverStagingFingerprintError(
            "source and staging row counts differ"
        )
    if source.symbols != staging.symbols:
        raise SilverStagingFingerprintError(
            "source and staging symbol sets differ"
        )
    if source.per_symbol_counts != staging.per_symbol_counts:
        raise SilverStagingFingerprintError(
            "source and staging per-symbol counts differ"
        )


def validate_staging_against_snapshot(
    spark: Any,
    config: clickhouse_schema.ClickHouseConfig,
    snapshot_id: int,
    *,
    source_table: str = DEFAULT_SILVER_TABLE,
) -> SilverStagingFingerprintResult:
    """Compare staging with one explicit Silver snapshot without mutations."""
    if snapshot_id <= 0:
        raise SilverStagingFingerprintError("snapshot_id must be positive")
    source_dataframe = read_silver_snapshot(spark, snapshot_id, source_table)
    source_summary = _validated_summary(
        source_dataframe,
        table_name=source_table,
        snapshot_id=snapshot_id,
    )
    staging_dataframe = read_staging_table(spark, config)
    staging_summary = _validated_summary(
        staging_dataframe,
        table_name=f"{config.database}.{STAGING_TABLE}",
        snapshot_id=snapshot_id,
    )
    _compare_metrics(source_summary, staging_summary)
    source_fingerprint = compute_row_multiset_fingerprint(source_dataframe)
    staging_fingerprint = compute_row_multiset_fingerprint(staging_dataframe)
    exact_copy = source_fingerprint == staging_fingerprint
    if not exact_copy:
        raise SilverStagingFingerprintError(
            "source and staging row-multiset fingerprints differ"
        )
    return SilverStagingFingerprintResult(
        source_table=source_summary.table_name,
        snapshot_id=snapshot_id,
        staging_table=f"{config.database}.{STAGING_TABLE}",
        source_row_count=source_summary.row_count,
        staging_row_count=staging_summary.row_count,
        source_distinct_symbol_count=source_summary.distinct_symbol_count,
        staging_distinct_symbol_count=staging_summary.distinct_symbol_count,
        source_symbols=source_summary.symbols,
        staging_symbols=staging_summary.symbols,
        source_per_symbol_counts=source_summary.per_symbol_counts,
        staging_per_symbol_counts=staging_summary.per_symbol_counts,
        source_fingerprint=source_fingerprint,
        staging_fingerprint=staging_fingerprint,
        exact_copy=True,
        validation_status="exact_copy_verified",
    )


def run_validation(
    *,
    snapshot_id: int,
    environ: Mapping[str, str] | None = None,
) -> SilverStagingFingerprintResult:
    """Own one shared Spark session for one explicit read-only validation."""
    environment = os.environ if environ is None else environ
    config = clickhouse_schema.ClickHouseConfig.from_environment(environment)
    spark = _build_spark(environment)
    try:
        return validate_staging_against_snapshot(spark, config, snapshot_id)
    finally:
        spark.stop()


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse the explicit-snapshot validation CLI."""
    parser = argparse.ArgumentParser(
        description="Validate ClickHouse staging against one Silver snapshot"
    )
    parser.add_argument("operation", choices=("validate",))
    parser.add_argument("--snapshot-id", type=int, required=True)
    return parser.parse_args(argv)


def _print_result(
    result: SilverStagingFingerprintResult,
    output: Callable[[str], None],
) -> None:
    """Print bounded deterministic validation output without raw data."""
    output(f"source_table={result.source_table}")
    output(f"snapshot_id={result.snapshot_id}")
    output(f"staging_table={result.staging_table}")
    output(f"source_rows={result.source_row_count}")
    output(f"staging_rows={result.staging_row_count}")
    output(f"source_symbols={','.join(result.source_symbols)}")
    output(f"staging_symbols={','.join(result.staging_symbols)}")
    output(f"source_fingerprint={result.source_fingerprint}")
    output(f"staging_fingerprint={result.staging_fingerprint}")
    output(f"exact_copy={str(result.exact_copy).lower()}")


def main(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
) -> int:
    """Run the read-only explicit-snapshot validation CLI."""
    args = parse_args(argv)
    try:
        result = run_validation(snapshot_id=args.snapshot_id, environ=environ)
        _print_result(result, print)
    except (
        SilverStagingFingerprintError,
        SilverSourceValidationError,
        clickhouse_schema.ClickHouseControlPlaneError,
    ) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
