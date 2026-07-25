"""Migrate the canonical Bronze table to the persisted quality contract."""

from __future__ import annotations

import argparse
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
import re
from typing import Protocol

from jobs.streaming.iceberg_bronze import BRONZE_TRADE_COLUMNS
from jobs.streaming.iceberg_inspection import (
    IcebergInspectionError,
    validate_table_identifier,
)
from jobs.streaming.iceberg_trade_streaming_job import (
    build_iceberg_trade_spark_session,
    parse_args as parse_streaming_args,
)


CANONICAL_BRONZE_TABLE_NAME = "market_catalog.market.bronze_trades"

QUALITY_BRONZE_COLUMNS: tuple[tuple[str, str], ...] = (
    *BRONZE_TRADE_COLUMNS,
    ("is_valid", "BOOLEAN"),
    ("validation_errors", "ARRAY<STRING>"),
)


class BronzeSchemaState(str, Enum):
    LEGACY_13_COLUMN = "LEGACY_13_COLUMN"
    QUALITY_15_COLUMN = "QUALITY_15_COLUMN"
    INCOMPATIBLE = "INCOMPATIBLE"


class BronzeMigrationResult(str, Enum):
    MIGRATED = "MIGRATED"
    ALREADY_MIGRATED = "ALREADY_MIGRATED"


class IcebergBronzeMigrationError(RuntimeError):
    """Raised when the canonical Bronze schema cannot be migrated safely."""


class SparkSqlExecutor(Protocol):
    def sql(self, query: str) -> object:
        ...


@dataclass(frozen=True)
class MigrationReport:
    table_name: str
    initial_state: BronzeSchemaState
    final_state: BronzeSchemaState
    result: BronzeMigrationResult


def validate_bronze_table_name(table_name: str) -> str:
    try:
        validated = validate_table_identifier(table_name)
    except (IcebergInspectionError, ValueError) as exc:
        raise IcebergBronzeMigrationError(str(exc)) from exc
    if validated != CANONICAL_BRONZE_TABLE_NAME:
        raise IcebergBronzeMigrationError(
            f"canonical migration target must be {CANONICAL_BRONZE_TABLE_NAME!r}"
        )
    return validated


def _normalize_type(type_name: object) -> str:
    return re.sub(r"\s+", "", str(type_name)).lower()


def _row_value(row: object, name: str, position: int) -> object:
    if hasattr(row, "asDict"):
        values = row.asDict()
        if name in values:
            return values[name]
    try:
        return row[name]  # type: ignore[index]
    except (KeyError, IndexError, TypeError):
        return row[position]  # type: ignore[index]


def _described_columns(result: object) -> list[tuple[str, str]]:
    columns: list[tuple[str, str]] = []
    for row in result.collect():
        name = str(_row_value(row, "col_name", 0) or "").strip()
        if not name or name.startswith("#"):
            break
        columns.append((name, _normalize_type(_row_value(row, "data_type", 1))))
    return columns


def _expected_columns(
    columns: tuple[tuple[str, str], ...],
) -> list[tuple[str, str]]:
    return [(name, _normalize_type(data_type)) for name, data_type in columns]


def inspect_bronze_schema_state(
    spark: SparkSqlExecutor,
    *,
    table_name: str = CANONICAL_BRONZE_TABLE_NAME,
) -> BronzeSchemaState:
    """Inspect the fixed canonical table and classify its exact schema state."""
    table = validate_bronze_table_name(table_name)
    try:
        described = spark.sql(f"DESCRIBE TABLE {table}")
        actual = _described_columns(described)
    except Exception as exc:
        raise IcebergBronzeMigrationError(
            f"could not inspect canonical Bronze schema for {table}"
        ) from exc

    if actual == _expected_columns(BRONZE_TRADE_COLUMNS):
        return BronzeSchemaState.LEGACY_13_COLUMN
    if actual == _expected_columns(QUALITY_BRONZE_COLUMNS):
        return BronzeSchemaState.QUALITY_15_COLUMN
    return BronzeSchemaState.INCOMPATIBLE


def bronze_quality_migration_sql(
    *,
    table_name: str = CANONICAL_BRONZE_TABLE_NAME,
) -> str:
    """Return the only allowed additive canonical-table migration statement."""
    table = validate_bronze_table_name(table_name)
    return (
        f"ALTER TABLE {table}\n"
        "ADD COLUMNS (\n"
        "is_valid BOOLEAN,\n"
        "validation_errors ARRAY<STRING>\n"
        ")"
    )


def migrate_bronze_table_to_quality_contract(
    spark: SparkSqlExecutor,
    *,
    table_name: str = CANONICAL_BRONZE_TABLE_NAME,
) -> MigrationReport:
    """Apply the additive migration once and require the exact final schema."""
    table = validate_bronze_table_name(table_name)
    initial_state = inspect_bronze_schema_state(spark, table_name=table)

    if initial_state is BronzeSchemaState.QUALITY_15_COLUMN:
        return MigrationReport(
            table_name=table,
            initial_state=initial_state,
            final_state=initial_state,
            result=BronzeMigrationResult.ALREADY_MIGRATED,
        )

    if initial_state is BronzeSchemaState.INCOMPATIBLE:
        raise IcebergBronzeMigrationError(
            f"incompatible canonical Bronze schema for {table}"
        )

    try:
        spark.sql(bronze_quality_migration_sql(table_name=table))
    except Exception as exc:
        raise IcebergBronzeMigrationError(
            f"could not migrate canonical Bronze table {table}"
        ) from exc

    final_state = inspect_bronze_schema_state(spark, table_name=table)
    if final_state is not BronzeSchemaState.QUALITY_15_COLUMN:
        raise IcebergBronzeMigrationError(
            f"canonical Bronze migration did not produce the exact quality schema: "
            f"{final_state.value}"
        )
    return MigrationReport(
        table_name=table,
        initial_state=initial_state,
        final_state=final_state,
        result=BronzeMigrationResult.MIGRATED,
    )


def _add_cleanup_note(primary: BaseException, cleanup: BaseException) -> None:
    primary.add_note(f"Spark session cleanup also failed: {cleanup!r}")


def run_migration(
    *,
    environ: Mapping[str, str] | None = None,
) -> MigrationReport:
    """Create an owned Spark session and run the canonical migration once."""
    environment = os.environ if environ is None else environ
    streaming_args = parse_streaming_args(
        ["--table-name", CANONICAL_BRONZE_TABLE_NAME],
        environ=environment,
    )
    try:
        spark = build_iceberg_trade_spark_session(
            app_name=streaming_args.app_name,
            catalog_name=streaming_args.catalog_name,
            catalog_uri=streaming_args.catalog_uri,
            warehouse=streaming_args.warehouse,
            s3_endpoint=streaming_args.s3_endpoint,
            s3_region=streaming_args.s3_region,
            s3_access_key=streaming_args.s3_access_key,
            s3_secret_key=streaming_args.s3_secret_key,
            s3_path_style_access=streaming_args.s3_path_style_access,
            s3a_ssl_enabled=streaming_args.s3a_ssl_enabled,
        )
    except Exception as exc:
        raise IcebergBronzeMigrationError(
            "could not start Spark for canonical Bronze migration"
        ) from exc
    migration_error: BaseException | None = None
    try:
        return migrate_bronze_table_to_quality_contract(spark)
    except BaseException as exc:
        migration_error = exc
        raise
    finally:
        try:
            spark.stop()
        except BaseException as cleanup_error:
            if migration_error is None:
                raise
            _add_cleanup_note(migration_error, cleanup_error)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse the migration CLI; deployment configuration remains environment-backed."""
    parser = argparse.ArgumentParser(
        description="Migrate the canonical Bronze table to the quality schema",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    """Run the explicit canonical Bronze schema migration."""
    parse_args(argv)
    report = run_migration()
    print(f"table={report.table_name}")
    print(f"initial_state={report.initial_state.value}")
    print(
        "alter_table_executed="
        f"{report.result is BronzeMigrationResult.MIGRATED}"
    )
    print(f"final_state={report.final_state.value}")
    print(f"result={report.result.value}")


if __name__ == "__main__":
    main()
