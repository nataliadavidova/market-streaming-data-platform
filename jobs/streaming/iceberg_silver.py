"""Build and deterministically replace the bounded Silver trade table."""

from __future__ import annotations

import argparse
import os
from collections.abc import Mapping, Sequence
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from jobs.streaming.iceberg_bronze import (
    CANONICAL_BRONZE_TABLE_NAME,
    QUALITY_BRONZE_COLUMNS,
)
from jobs.streaming.iceberg_bronze_migration import (
    BronzeSchemaState,
    inspect_bronze_schema_state,
)
from jobs.streaming.iceberg_trade_streaming_job import (
    build_iceberg_trade_spark_session,
    parse_args as parse_streaming_args,
)


SILVER_TRADES_TABLE_NAME = "market_catalog.market.silver_trades"
SILVER_TRADE_COLUMNS: tuple[tuple[str, str], ...] = (
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


class SilverBuildError(RuntimeError):
    """Raised when the bounded Silver build cannot complete safely."""


def build_silver_trades_dataframe(
    bronze_df: DataFrame,
) -> DataFrame:
    """Transform the complete Bronze DataFrame into the valid Silver contract."""
    expected_columns = [name for name, _ in QUALITY_BRONZE_COLUMNS]
    if bronze_df.columns != expected_columns:
        raise SilverBuildError(
            "Bronze DataFrame must use the exact quality-contract column order"
        )

    decimal_type = DecimalType(38, 18)
    return (
        bronze_df.where(F.col("is_valid") == F.lit(True))
        .select(
            F.col("exchange"),
            F.col("symbol"),
            F.col("trade_id"),
            F.col("price").cast(decimal_type).alias("price"),
            F.col("quantity").cast(decimal_type).alias("quantity"),
            (F.col("price") * F.col("quantity"))
            .cast(decimal_type)
            .alias("notional"),
            F.timestamp_millis(F.col("event_time_ms")).alias("event_time"),
            F.timestamp_millis(F.col("ingested_at_ms")).alias("ingested_at"),
            (F.col("ingested_at_ms") - F.col("event_time_ms"))
            .cast("bigint")
            .alias("latency_ms"),
            F.col("kafka_topic"),
            F.col("kafka_partition"),
            F.col("kafka_offset"),
        )
    )


def _validate_bronze_source(spark: Any, table_name: str) -> None:
    state = inspect_bronze_schema_state(spark, table_name=table_name)
    if state is not BronzeSchemaState.QUALITY_15_COLUMN:
        raise SilverBuildError(
            "Silver build requires the exact QUALITY_15_COLUMN Bronze schema; "
            f"found {state.value}"
        )


def rebuild_silver_trades(
    spark: Any,
    *,
    bronze_table: str = CANONICAL_BRONZE_TABLE_NAME,
    silver_table: str = SILVER_TRADES_TABLE_NAME,
) -> int:
    """Replace Silver with a complete valid-row rebuild and return its row count."""
    if bronze_table != CANONICAL_BRONZE_TABLE_NAME:
        raise SilverBuildError(
            f"Silver source must be {CANONICAL_BRONZE_TABLE_NAME!r}"
        )
    if silver_table != SILVER_TRADES_TABLE_NAME:
        raise SilverBuildError(f"Silver target must be {SILVER_TRADES_TABLE_NAME!r}")

    _validate_bronze_source(spark, bronze_table)
    silver_df = build_silver_trades_dataframe(spark.table(bronze_table))
    # Spark/Iceberg V2 createOrReplace is the runtime-validated full-rebuild
    # boundary. It replaces table metadata; it does not append duplicate rows.
    silver_df.writeTo(silver_table).using("iceberg").createOrReplace()
    return int(spark.table(silver_table).count())


def run_silver_rebuild(*, environ: Mapping[str, str] | None = None) -> int:
    """Create one owned Spark session, rebuild Silver, and stop it once."""
    environment = os.environ if environ is None else environ
    args = parse_streaming_args([], environ=environment)
    spark = build_iceberg_trade_spark_session(
        app_name="market-iceberg-silver-rebuild",
        catalog_name=args.catalog_name,
        catalog_uri=args.catalog_uri,
        warehouse=args.warehouse,
        s3_endpoint=args.s3_endpoint,
        s3_region=args.s3_region,
        s3_access_key=args.s3_access_key,
        s3_secret_key=args.s3_secret_key,
        s3_path_style_access=args.s3_path_style_access,
        s3a_ssl_enabled=args.s3a_ssl_enabled,
    )
    build_error: BaseException | None = None
    try:
        return rebuild_silver_trades(spark)
    except BaseException as error:
        build_error = error
        raise
    finally:
        try:
            spark.stop()
        except BaseException as cleanup_error:
            if build_error is None:
                raise
            build_error.add_note(f"Spark cleanup failed: {cleanup_error!r}")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rebuild the bounded Silver trades Iceberg table"
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    parse_args(argv)
    print(f"table={SILVER_TRADES_TABLE_NAME}")
    print(f"row_count={run_silver_rebuild()}")


if __name__ == "__main__":
    main()
