"""Provision the fixed read-only ClickHouse BI service account."""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
import os
import sys
from typing import Any

from jobs.serving import clickhouse_schema


BI_READER_USER = "bi_reader"
BI_PASSWORD_ENVIRONMENT = "CLICKHOUSE_BI_PASSWORD"
BI_DATABASE = clickhouse_schema.DEFAULT_DATABASE
BI_TABLE = clickhouse_schema.TARGET_TABLE


class ClickHouseBIReaderError(RuntimeError):
    """Raised when BI reader provisioning cannot converge safely."""


@dataclass(frozen=True)
class BIReaderProvisionResult:
    """Bounded result for one BI reader provisioning attempt."""

    user: str
    database: str
    table: str
    authentication: str
    grant: str
    status: str


def _sql_string(value: str) -> str:
    """Quote one SQL string literal without exposing it in diagnostics."""
    return "'" + value.replace("'", "''") + "'"


def build_create_user_query(password: str) -> str:
    """Build idempotent creation SQL for the fixed BI reader."""
    return (
        f"CREATE USER IF NOT EXISTS {BI_READER_USER} "
        f"IDENTIFIED WITH sha256_password BY {_sql_string(password)}"
    )


def build_alter_user_query(password: str) -> str:
    """Build explicit password-convergence SQL for an existing BI reader."""
    return (
        f"ALTER USER {BI_READER_USER} "
        f"IDENTIFIED WITH sha256_password BY {_sql_string(password)}"
    )


def build_select_grant_query() -> str:
    """Build the only data-access grant owned by this provisioning command."""
    return f"GRANT SELECT ON {BI_DATABASE}.{BI_TABLE} TO {BI_READER_USER}"


def _require_bi_password(environ: Mapping[str, str]) -> str:
    password = environ.get(BI_PASSWORD_ENVIRONMENT, "")
    if not password:
        raise ClickHouseBIReaderError(
            f"{BI_PASSWORD_ENVIRONMENT} must be present and non-empty"
        )
    return password


def _command(client: clickhouse_schema.ClickHouseClient, query: str, operation: str) -> None:
    try:
        client.command(query)
    except Exception as exc:
        raise ClickHouseBIReaderError(f"could not {operation}") from exc


def provision_bi_reader(
    config: clickhouse_schema.ClickHouseConfig,
    *,
    bi_password: str,
    client_factory: Callable[[clickhouse_schema.ClickHouseConfig], Any] = clickhouse_schema.connect_client,
) -> BIReaderProvisionResult:
    """Converge authentication and one exact table-scoped SELECT grant."""
    if not bi_password:
        raise ClickHouseBIReaderError(
            f"{BI_PASSWORD_ENVIRONMENT} must be present and non-empty"
        )
    try:
        client = client_factory(config)
    except Exception as exc:
        raise ClickHouseBIReaderError("could not connect for BI reader provisioning") from exc

    _command(client, build_create_user_query(bi_password), "create BI reader user")
    _command(client, build_alter_user_query(bi_password), "converge BI reader password")
    _command(client, build_select_grant_query(), "grant BI reader SELECT access")
    return BIReaderProvisionResult(
        user=BI_READER_USER,
        database=BI_DATABASE,
        table=BI_TABLE,
        authentication="sha256_password",
        grant="SELECT",
        status="provisioned",
    )


def run_provision(
    *,
    environ: Mapping[str, str] | None = None,
) -> BIReaderProvisionResult:
    """Provision the BI reader from environment-backed control-plane settings."""
    environment = os.environ if environ is None else environ
    bi_password = _require_bi_password(environment)
    config = clickhouse_schema.ClickHouseConfig.from_environment(environment)
    return provision_bi_reader(config, bi_password=bi_password)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse the single supported provisioning operation."""
    parser = argparse.ArgumentParser(description="Provision the ClickHouse BI reader")
    parser.add_argument("operation", choices=("provision",))
    return parser.parse_args(argv)


def print_result(result: BIReaderProvisionResult, output: Callable[[str], None]) -> None:
    """Print bounded provisioning metadata without credentials."""
    output(
        f"status={result.status} user={result.user} database={result.database} "
        f"table={result.table} authentication={result.authentication} "
        f"grant={result.grant}"
    )


def main(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
) -> int:
    """Run the explicit BI reader provisioning CLI."""
    parse_args(argv)
    try:
        print_result(run_provision(environ=environ), print)
    except (
        ClickHouseBIReaderError,
        clickhouse_schema.ClickHouseControlPlaneError,
    ) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
