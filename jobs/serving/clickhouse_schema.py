"""Ensure and validate the ClickHouse serving database and table schemas."""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
import os
import re
from typing import Any, Protocol

import clickhouse_connect


DEFAULT_HOST = "localhost"
DEFAULT_HTTP_PORT = 18123
DEFAULT_DATABASE = "market_analytics"
TARGET_TABLE = "silver_trades"
STAGING_TABLE = "silver_trades_staging"
TABLE_ENGINE = "MergeTree"
PARTITION_EXPRESSION = "toYYYYMM(event_time)"
ORDERING_KEY = (
    "exchange",
    "symbol",
    "event_time",
    "trade_id",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
)
TABLE_COLUMNS = (
    ("exchange", "String"),
    ("symbol", "String"),
    ("trade_id", "String"),
    ("price", "Decimal(38,18)"),
    ("quantity", "Decimal(38,18)"),
    ("notional", "Decimal(38,18)"),
    ("event_time", "DateTime64(3, 'UTC')"),
    ("ingested_at", "DateTime64(3, 'UTC')"),
    ("latency_ms", "Int64"),
    ("kafka_topic", "String"),
    ("kafka_partition", "Int32"),
    ("kafka_offset", "Int64"),
)
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ClickHouseControlPlaneError(RuntimeError):
    """Raised when configuration, connectivity, or schema validation fails."""


@dataclass(frozen=True, repr=True)
class ClickHouseConfig:
    """Validated HTTP connection settings for the ClickHouse control plane."""

    host: str = DEFAULT_HOST
    http_port: int = DEFAULT_HTTP_PORT
    database: str = DEFAULT_DATABASE
    user: str = field(repr=False, default="")
    password: str = field(repr=False, default="")

    @classmethod
    def from_environment(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> "ClickHouseConfig":
        values = os.environ if environ is None else environ
        host = values.get("CLICKHOUSE_HOST", DEFAULT_HOST)
        database = values.get("CLICKHOUSE_DATABASE", DEFAULT_DATABASE)
        user = values.get("CLICKHOUSE_USER", "")
        password = values.get("CLICKHOUSE_PASSWORD", "")
        raw_port = values.get("CLICKHOUSE_HTTP_PORT", str(DEFAULT_HTTP_PORT))

        if not host.strip():
            raise ClickHouseControlPlaneError("CLICKHOUSE_HOST must be non-empty")
        try:
            http_port = int(raw_port)
        except (TypeError, ValueError) as exc:
            raise ClickHouseControlPlaneError(
                "CLICKHOUSE_HTTP_PORT must be an integer TCP port"
            ) from exc
        if not 1 <= http_port <= 65535:
            raise ClickHouseControlPlaneError(
                "CLICKHOUSE_HTTP_PORT must be between 1 and 65535"
            )
        validate_identifier(database, "CLICKHOUSE_DATABASE")
        if not user:
            raise ClickHouseControlPlaneError("CLICKHOUSE_USER must be non-empty")
        if not password:
            raise ClickHouseControlPlaneError(
                "CLICKHOUSE_PASSWORD must be present"
            )
        return cls(
            host=host,
            http_port=http_port,
            database=database,
            user=user,
            password=password,
        )


class ClickHouseClient(Protocol):
    """Minimal clickhouse-connect client surface used by this control plane."""

    def command(self, query: str) -> object:
        ...

    def query(self, query: str) -> object:
        ...


ClientFactory = Callable[..., ClickHouseClient]


@dataclass(frozen=True)
class SchemaReport:
    """Concise result used by the CLI and runtime callers."""

    database: str
    database_engine: str
    target_table: str
    staging_table: str
    table_engine: str
    column_count: int


def validate_identifier(value: str, setting_name: str = "identifier") -> str:
    """Validate one unquoted ClickHouse identifier for safe SQL composition."""
    if not _IDENTIFIER.fullmatch(value):
        raise ClickHouseControlPlaneError(
            f"{setting_name} must be a safe ClickHouse identifier"
        )
    return value


def build_create_database_query(database: str = DEFAULT_DATABASE) -> str:
    """Build the approved idempotent Atomic database DDL."""
    database = validate_identifier(database, "database")
    return f"CREATE DATABASE IF NOT EXISTS {database} ENGINE = Atomic"


def build_create_table_query(
    table: str,
    *,
    database: str = DEFAULT_DATABASE,
) -> str:
    """Build the approved idempotent MergeTree table DDL."""
    database = validate_identifier(database, "database")
    table = validate_identifier(table, "table")
    columns = ",\n    ".join(f"{name} {data_type}" for name, data_type in TABLE_COLUMNS)
    ordering_key = ",\n        ".join(ORDERING_KEY)
    return (
        f"CREATE TABLE IF NOT EXISTS {database}.{table} (\n"
        f"    {columns}\n"
        ")\n"
        "ENGINE = MergeTree\n"
        f"PARTITION BY {PARTITION_EXPRESSION}\n"
        "ORDER BY (\n"
        f"        {ordering_key}\n"
        ")"
    )


def connect_client(
    config: ClickHouseConfig,
    *,
    client_factory: ClientFactory = clickhouse_connect.get_client,
) -> ClickHouseClient:
    """Connect through HTTP using the bootstrap database, without logging secrets."""
    try:
        return client_factory(
            host=config.host,
            port=config.http_port,
            username=config.user,
            password=config.password,
            database="default",
        )
    except Exception as exc:
        raise ClickHouseControlPlaneError(
            "could not connect to ClickHouse control plane"
        ) from exc


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _query_rows(result: object) -> list[object]:
    rows = getattr(result, "result_rows", result)
    return list(rows)


def _command(client: ClickHouseClient, query: str, operation: str) -> None:
    try:
        client.command(query)
    except Exception as exc:
        raise ClickHouseControlPlaneError(f"could not {operation}") from exc


def _query(client: ClickHouseClient, query: str, operation: str) -> list[object]:
    try:
        return _query_rows(client.query(query))
    except ClickHouseControlPlaneError:
        raise
    except Exception as exc:
        raise ClickHouseControlPlaneError(f"could not {operation}") from exc


def _row_value(row: object, name: str, position: int) -> object:
    if hasattr(row, "named_results"):
        values = row.named_results()
        if name in values:
            return values[name]
    if isinstance(row, Mapping):
        return row[name]
    try:
        return row[name]  # type: ignore[index]
    except (KeyError, IndexError, TypeError):
        return row[position]  # type: ignore[index]


def _normalize_metadata_expression(value: object) -> str:
    expression = re.sub(r"\s+", "", str(value or ""))
    while expression.startswith("(") and expression.endswith(")"):
        depth = 0
        balanced_outer = True
        for index, character in enumerate(expression):
            if character == "(":
                depth += 1
            elif character == ")":
                depth -= 1
                if depth == 0 and index != len(expression) - 1:
                    balanced_outer = False
                    break
        if balanced_outer:
            expression = expression[1:-1]
        else:
            break
    return expression


def _normalize_type(value: object) -> str:
    return re.sub(r"\s+", "", str(value or ""))


def _database_metadata(client: ClickHouseClient, database: str) -> tuple[str, str]:
    rows = _query(
        client,
        "SELECT name, engine FROM system.databases "
        f"WHERE name = {_sql_string(database)}",
        f"inspect database metadata for {database}",
    )
    if len(rows) != 1:
        raise ClickHouseControlPlaneError(
            f"database metadata missing for {database}"
        )
    return (
        str(_row_value(rows[0], "name", 0)),
        str(_row_value(rows[0], "engine", 1)),
    )


def _table_metadata(
    client: ClickHouseClient,
    database: str,
    table: str,
) -> tuple[str, str, str, str, list[tuple[str, str]]]:
    table_rows = _query(
        client,
        "SELECT name, engine, partition_key, sorting_key "
        "FROM system.tables "
        f"WHERE database = {_sql_string(database)} "
        f"AND name = {_sql_string(table)}",
        f"inspect table metadata for {database}.{table}",
    )
    if len(table_rows) != 1:
        raise ClickHouseControlPlaneError(
            f"table metadata missing for {database}.{table}"
        )
    table_row = table_rows[0]
    columns = [
        (
            str(_row_value(row, "name", 0)),
            _normalize_type(_row_value(row, "type", 1)),
        )
        for row in _query(
            client,
            "SELECT name, type FROM system.columns "
            f"WHERE database = {_sql_string(database)} "
            f"AND table = {_sql_string(table)} ORDER BY position",
            f"inspect columns for {database}.{table}",
        )
    ]
    return (
        str(_row_value(table_row, "name", 0)),
        str(_row_value(table_row, "engine", 1)),
        _normalize_metadata_expression(_row_value(table_row, "partition_key", 2)),
        _normalize_metadata_expression(_row_value(table_row, "sorting_key", 3)),
        columns,
    )


def _validate_table_contract(
    client: ClickHouseClient,
    *,
    database: str,
    table: str,
) -> None:
    name, engine, partition_key, sorting_key, columns = _table_metadata(
        client, database, table
    )
    expected_columns = [(column, _normalize_type(data_type)) for column, data_type in TABLE_COLUMNS]
    expected_sorting_key = _normalize_metadata_expression(",".join(ORDERING_KEY))
    if name != table:
        raise ClickHouseControlPlaneError(f"table name mismatch for {database}.{table}")
    if engine != TABLE_ENGINE:
        raise ClickHouseControlPlaneError(f"table engine mismatch for {database}.{table}")
    if partition_key != _normalize_metadata_expression(PARTITION_EXPRESSION):
        raise ClickHouseControlPlaneError(
            f"table partition key mismatch for {database}.{table}"
        )
    if sorting_key != expected_sorting_key:
        raise ClickHouseControlPlaneError(
            f"table sorting key mismatch for {database}.{table}"
        )
    if columns != expected_columns:
        raise ClickHouseControlPlaneError(f"table columns mismatch for {database}.{table}")


def ensure_schema(
    client: ClickHouseClient,
    config: ClickHouseConfig,
) -> SchemaReport:
    """Create missing serving objects and require the complete exact contract."""
    _command(
        client,
        build_create_database_query(config.database),
        f"create database {config.database}",
    )
    _command(
        client,
        build_create_table_query(TARGET_TABLE, database=config.database),
        f"create table {config.database}.{TARGET_TABLE}",
    )
    _command(
        client,
        build_create_table_query(STAGING_TABLE, database=config.database),
        f"create table {config.database}.{STAGING_TABLE}",
    )

    database_name, database_engine = _database_metadata(client, config.database)
    if database_name != config.database:
        raise ClickHouseControlPlaneError("database name mismatch")
    if database_engine != "Atomic":
        raise ClickHouseControlPlaneError(
            f"database engine mismatch for {config.database}: expected Atomic"
        )
    _validate_table_contract(client, database=config.database, table=TARGET_TABLE)
    _validate_table_contract(client, database=config.database, table=STAGING_TABLE)
    return SchemaReport(
        database=config.database,
        database_engine=database_engine,
        target_table=TARGET_TABLE,
        staging_table=STAGING_TABLE,
        table_engine=TABLE_ENGINE,
        column_count=len(TABLE_COLUMNS),
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ensure the ClickHouse serving schema without loading data"
    )
    parser.add_argument("command", choices=("ensure",))
    return parser.parse_args(argv)


def main(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    client_factory: ClientFactory = clickhouse_connect.get_client,
) -> int:
    """Run the bounded schema-control CLI and return a process exit code."""
    args = parse_args(argv)
    try:
        config = ClickHouseConfig.from_environment(environ)
        client = connect_client(config, client_factory=client_factory)
        report = ensure_schema(client, config)
    except ClickHouseControlPlaneError as exc:
        print(f"error: {exc}", file=__import__("sys").stderr)
        return 1
    print(
        f"ClickHouse schema ensured: database={report.database} "
        f"engine={report.database_engine} target={report.target_table} "
        f"staging={report.staging_table} table_engine={report.table_engine} "
        f"column_count={report.column_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
