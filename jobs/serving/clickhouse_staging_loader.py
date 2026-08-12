"""Load one validated Silver Iceberg snapshot into ClickHouse staging."""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
import os
import sys
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from jobs.serving import clickhouse_schema
from jobs.serving.silver_source_validation import (
    DEFAULT_SILVER_TABLE,
    EXPECTED_SILVER_COLUMNS,
    SilverSourceValidationError,
    SPARK_ICEBERG_PACKAGES,
    compute_source_summary,
    read_silver_snapshot,
    resolve_current_snapshot_id,
    validate_required_nulls,
    validate_silver_schema,
)
from jobs.streaming.iceberg_trade_streaming_job import (
    build_iceberg_trade_spark_session,
    parse_args as parse_streaming_args,
)


CLICKHOUSE_JDBC_PACKAGE = "com.clickhouse:clickhouse-jdbc:0.8.6"
CLICKHOUSE_JDBC_DRIVER = "com.clickhouse.jdbc.Driver"
STAGING_TABLE = clickhouse_schema.STAGING_TABLE


class ClickHouseStagingLoadError(RuntimeError):
    """Raised when a snapshot-bound staging load cannot complete."""


@dataclass(frozen=True)
class StagingLoadSummary:
    """Bounded result for one source-snapshot transport attempt."""

    source_table: str
    snapshot_id: int
    source_row_count: int
    staging_table: str
    staging_row_count: int
    database: str
    load_status: str


def build_jdbc_url(config: clickhouse_schema.ClickHouseConfig) -> str:
    """Build the HTTP ClickHouse JDBC URL without credentials."""
    return f"jdbc:clickhouse://{config.host}:{config.http_port}/{config.database}"


def write_snapshot_to_staging(
    dataframe: DataFrame,
    config: clickhouse_schema.ClickHouseConfig,
) -> None:
    """Append the already validated DataFrame to the existing staging table."""
    if dataframe.columns != list(EXPECTED_SILVER_COLUMNS):
        raise ClickHouseStagingLoadError(
            "Silver DataFrame must use the exact approved column order"
        )
    try:
        (
            dataframe.write.format("jdbc")
            .option("url", build_jdbc_url(config))
            .option("dbtable", f"{config.database}.{STAGING_TABLE}")
            .option("driver", CLICKHOUSE_JDBC_DRIVER)
            .option("user", config.user)
            .option("password", config.password)
            .mode("append")
            .save()
        )
    except Exception as exc:
        raise ClickHouseStagingLoadError("ClickHouse JDBC staging write failed") from exc


def load_silver_snapshot_to_staging(
    spark: Any,
    config: clickhouse_schema.ClickHouseConfig,
    *,
    client_factory: clickhouse_schema.ClientFactory = clickhouse_schema.connect_client,
    source_table: str = DEFAULT_SILVER_TABLE,
    max_display_symbols: int = 20,
) -> StagingLoadSummary:
    """Validate one bound snapshot, load staging, and check row-count transport."""
    snapshot_id = resolve_current_snapshot_id(spark, source_table)
    dataframe = read_silver_snapshot(spark, snapshot_id, source_table)
    validate_silver_schema(dataframe)
    null_counts = validate_required_nulls(dataframe)
    source_summary = compute_source_summary(
        dataframe,
        table_name=source_table,
        snapshot_id=snapshot_id,
        null_counts=null_counts,
        max_display_symbols=max_display_symbols,
    )

    try:
        client = client_factory(config)
        clickhouse_schema.ensure_schema(client, config)
        clickhouse_schema.truncate_staging(client, config)
    except clickhouse_schema.ClickHouseControlPlaneError as exc:
        raise ClickHouseStagingLoadError(
            "ClickHouse schema validation or staging preparation failed"
        ) from exc

    write_snapshot_to_staging(dataframe, config)
    try:
        staging_row_count = clickhouse_schema.staging_row_count(client, config)
    except clickhouse_schema.ClickHouseControlPlaneError as exc:
        raise ClickHouseStagingLoadError(
            "could not verify ClickHouse staging row count"
        ) from exc
    if staging_row_count != source_summary.row_count:
        raise ClickHouseStagingLoadError(
            "staging row count does not match the source snapshot row count"
        )
    return StagingLoadSummary(
        source_table=source_summary.table_name,
        snapshot_id=source_summary.snapshot_id,
        source_row_count=source_summary.row_count,
        staging_table=STAGING_TABLE,
        staging_row_count=staging_row_count,
        database=config.database,
        load_status="transport_row_count_verified",
    )


def _build_spark(environ: Mapping[str, str]) -> object:
    """Build the shared Iceberg/S3A session with the serving-only JDBC package."""
    args = parse_streaming_args([], environ=environ)
    packages = f"{SPARK_ICEBERG_PACKAGES},{CLICKHOUSE_JDBC_PACKAGE}"
    builder = SparkSession.builder.config("spark.jars.packages", packages)
    return build_iceberg_trade_spark_session(
        app_name="market-clickhouse-silver-staging-loader",
        catalog_name=args.catalog_name,
        catalog_uri=args.catalog_uri,
        warehouse=args.warehouse,
        s3_endpoint=args.s3_endpoint,
        s3_region=args.s3_region,
        s3_access_key=args.s3_access_key,
        s3_secret_key=args.s3_secret_key,
        s3_path_style_access=args.s3_path_style_access,
        s3a_ssl_enabled=args.s3a_ssl_enabled,
        builder=builder,
    )


def run_staging_load(
    *,
    environ: Mapping[str, str] | None = None,
) -> StagingLoadSummary:
    """Own one configured Spark session for one bounded staging load."""
    environment = os.environ if environ is None else environ
    config = clickhouse_schema.ClickHouseConfig.from_environment(environment)
    spark = _build_spark(environment)
    try:
        return load_silver_snapshot_to_staging(spark, config)
    finally:
        spark.stop()


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse the intentionally small staging-loader CLI."""
    parser = argparse.ArgumentParser(
        description="Load one validated Silver snapshot into ClickHouse staging"
    )
    parser.add_argument("operation", choices=("load",))
    return parser.parse_args(argv)


def _print_summary(summary: StagingLoadSummary, output: Callable[[str], None]) -> None:
    """Print bounded load status without credentials or row payloads."""
    output(
        f"status={summary.load_status} database={summary.database} "
        f"source_table={summary.source_table} snapshot_id={summary.snapshot_id} "
        f"source_row_count={summary.source_row_count} "
        f"staging_table={summary.staging_table} "
        f"staging_row_count={summary.staging_row_count}"
    )


def main(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
) -> int:
    """Run the bounded staging-loader CLI and return a process exit code."""
    parse_args(argv)
    try:
        summary = run_staging_load(environ=environ)
        _print_summary(summary, print)
    except (
        ClickHouseStagingLoadError,
        clickhouse_schema.ClickHouseControlPlaneError,
        SilverSourceValidationError,
    ) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
