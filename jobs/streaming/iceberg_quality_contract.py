"""Persist classified Bronze rows to an isolated Iceberg smoke table."""

from __future__ import annotations

import re
from typing import Protocol

from pyspark.sql import DataFrame

from jobs.streaming.iceberg_bronze import (
    CANONICAL_BRONZE_TABLE_NAME,
    QUALITY_BRONZE_COLUMNS,
)
from jobs.streaming.iceberg_inspection import validate_table_identifier


QUALITY_SMOKE_TABLE_NAME = "market_catalog.market.bronze_trades_quality_smoke"

# Compatibility alias retained for existing callers of this module.
QUALITY_CONTRACT_COLUMNS = QUALITY_BRONZE_COLUMNS


class SparkSqlExecutor(Protocol):
    def sql(self, query: str) -> object:
        ...


class IcebergQualityContractError(RuntimeError):
    """Raised when the isolated persisted quality contract is unsafe to use."""


def validate_quality_smoke_table_name(table_name: str) -> str:
    """Accept only the fixed isolated table and reject the canonical table."""
    try:
        validated = validate_table_identifier(table_name)
    except ValueError as exc:
        raise IcebergQualityContractError(str(exc)) from exc

    if validated == CANONICAL_BRONZE_TABLE_NAME:
        raise IcebergQualityContractError(
            "the canonical Bronze table is not a quality-contract smoke target"
        )
    if validated != QUALITY_SMOKE_TABLE_NAME:
        raise IcebergQualityContractError(
            f"quality smoke target must be {QUALITY_SMOKE_TABLE_NAME!r}"
        )
    return validated


def quality_smoke_table_ddl(
    *,
    table_name: str = QUALITY_SMOKE_TABLE_NAME,
) -> str:
    """Build the exact isolated 15-column Iceberg table definition."""
    table = validate_quality_smoke_table_name(table_name)
    definitions = ",\n".join(
        f"{name} {sql_type.replace(' ', '').upper()}"
        for name, sql_type in QUALITY_CONTRACT_COLUMNS
    )
    return (
        f"CREATE TABLE IF NOT EXISTS {table} (\n"
        f"{definitions}\n"
        ")\n"
        "USING iceberg"
    )


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
    rows = result.collect()
    columns: list[tuple[str, str]] = []
    for row in rows:
        name = str(_row_value(row, "col_name", 0) or "").strip()
        if not name or name.startswith("#"):
            break
        data_type = _row_value(row, "data_type", 1)
        columns.append((name, _normalize_type(data_type)))
    return columns


def validate_quality_smoke_table_schema(
    spark: SparkSqlExecutor,
    *,
    table_name: str = QUALITY_SMOKE_TABLE_NAME,
) -> None:
    """Require an existing isolated table to match the 15-column contract."""
    table = validate_quality_smoke_table_name(table_name)
    try:
        described = spark.sql(f"DESCRIBE TABLE {table}")
        actual = _described_columns(described)
    except Exception as exc:
        raise IcebergQualityContractError(
            f"could not validate quality smoke table schema for {table}"
        ) from exc

    expected = [
        (name, _normalize_type(data_type))
        for name, data_type in QUALITY_CONTRACT_COLUMNS
    ]
    if actual != expected:
        raise IcebergQualityContractError(
            f"quality smoke table schema mismatch for {table}: "
            f"expected {expected!r}, got {actual!r}"
        )


def ensure_quality_smoke_table(
    spark: SparkSqlExecutor,
    *,
    table_name: str = QUALITY_SMOKE_TABLE_NAME,
) -> None:
    """Create the isolated table if needed, then validate its exact schema."""
    table = validate_quality_smoke_table_name(table_name)
    try:
        spark.sql(quality_smoke_table_ddl(table_name=table))
    except Exception as exc:
        raise IcebergQualityContractError(
            f"could not create quality smoke table {table}"
        ) from exc
    validate_quality_smoke_table_schema(spark, table_name=table)


def _dataframe_columns(dataframe: DataFrame) -> list[tuple[str, str]]:
    fields = getattr(dataframe.schema, "fields", ())
    return [
        (field.name, _normalize_type(field.dataType.simpleString()))
        for field in fields
    ]


def _validate_classified_dataframe(dataframe: DataFrame) -> None:
    expected_names = [name for name, _ in QUALITY_CONTRACT_COLUMNS]
    if list(dataframe.columns) != expected_names:
        raise IcebergQualityContractError(
            "classified DataFrame columns must match the quality contract exactly"
        )

    expected = [
        (name, _normalize_type(data_type))
        for name, data_type in QUALITY_CONTRACT_COLUMNS
    ]
    actual = _dataframe_columns(dataframe)
    if actual != expected:
        raise IcebergQualityContractError(
            f"classified DataFrame schema mismatch: expected {expected!r}, "
            f"got {actual!r}"
        )


def append_quality_contract_rows(
    classified_df: DataFrame,
    *,
    table_name: str = QUALITY_SMOKE_TABLE_NAME,
) -> None:
    """Append one static classified DataFrame to the isolated table."""
    table = validate_quality_smoke_table_name(table_name)
    _validate_classified_dataframe(classified_df)
    spark = getattr(classified_df, "sparkSession", None)
    if spark is None:
        raise IcebergQualityContractError(
            "classified DataFrame must expose its owning Spark session"
        )
    validate_quality_smoke_table_schema(spark, table_name=table)
    try:
        classified_df.writeTo(table).append()
    except Exception as exc:
        raise IcebergQualityContractError(
            f"could not append classified rows to {table}"
        ) from exc
