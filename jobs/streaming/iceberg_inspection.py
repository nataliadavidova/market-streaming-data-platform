"""Inspect an existing Iceberg table without mutating storage."""

import argparse
import os
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from jobs.streaming.iceberg_trade_streaming_job import (
    build_iceberg_trade_spark_session,
    parse_args as parse_streaming_args,
)
from jobs.streaming.iceberg_bronze import CANONICAL_BRONZE_TABLE_NAME


# Compatibility alias retained for the inspector CLI and existing callers.
DEFAULT_TABLE = CANONICAL_BRONZE_TABLE_NAME
DEFAULT_MAX_ROWS = 100


class IcebergInspectionError(RuntimeError):
    """Report a failed or invalid read-only Iceberg inspection."""


def validate_table_identifier(table_identifier: str) -> str:
    """Validate a simple Spark SQL identifier made of safe dotted segments."""
    if not table_identifier or table_identifier != table_identifier.strip():
        raise IcebergInspectionError(
            "table identifier must be a non-empty dotted identifier without whitespace"
        )

    segments = table_identifier.split(".")
    if not 1 <= len(segments) <= 3:
        raise IcebergInspectionError(
            "table identifier must contain between one and three identifier segments"
        )

    for segment in segments:
        if not segment or not (
            segment[0].isalpha() or segment[0] == "_"
        ) or not all(character.isalnum() or character == "_" for character in segment):
            raise IcebergInspectionError(
                "table identifier segments may contain only letters, numbers, and underscores"
            )

    return table_identifier


def _positive_integer(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a positive integer") from error
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def build_inspection_queries(
    table_identifier: str,
    *,
    max_rows: int = DEFAULT_MAX_ROWS,
) -> dict[str, str]:
    """Build the bounded, read-only SQL used by the inspector."""
    table = validate_table_identifier(table_identifier)
    if max_rows <= 0:
        raise ValueError("max_rows must be positive")

    limit = int(max_rows)
    return {
        "identity": f"DESCRIBE TABLE EXTENDED {table}",
        "schema": f"DESCRIBE TABLE {table}",
        "row_count": f"SELECT COUNT(*) AS row_count FROM {table}",
        "snapshots_count": (
            f"SELECT COUNT(*) AS metadata_count FROM {table}.snapshots"
        ),
        "snapshots": (
            f"SELECT * FROM {table}.snapshots "
            f"ORDER BY committed_at DESC, snapshot_id DESC LIMIT {limit}"
        ),
        "history_count": f"SELECT COUNT(*) AS metadata_count FROM {table}.history",
        "history": (
            f"SELECT * FROM {table}.history "
            f"ORDER BY made_current_at DESC, snapshot_id DESC LIMIT {limit}"
        ),
        "files_count": f"SELECT COUNT(*) AS metadata_count FROM {table}.files",
        "files": (
            f"SELECT * FROM {table}.files "
            f"ORDER BY file_path LIMIT {limit}"
        ),
        "partitions_count": (
            f"SELECT COUNT(*) AS metadata_count FROM {table}.partitions"
        ),
        "partitions": (
            f"SELECT * FROM {table}.partitions LIMIT {limit}"
        ),
    }


def _first_value(frame: Any, column: str) -> Any:
    rows = frame.collect()
    if not rows:
        raise IcebergInspectionError(
            f"inspection query returned no {column} result"
        )
    row = rows[0]
    if isinstance(row, Mapping):
        return row[column]
    return row[column]


def _show(frame: Any, *, max_rows: int) -> None:
    frame.show(max_rows, truncate=False)


def _print_section(title: str, output: Callable[[str], None]) -> None:
    output(f"\n== {title} ==")


def inspect_iceberg_table(
    spark: Any,
    table_identifier: str,
    *,
    max_rows: int = DEFAULT_MAX_ROWS,
    output: Callable[[str], None] = print,
) -> None:
    """Print bounded read-only table and Iceberg metadata information."""
    table = validate_table_identifier(table_identifier)
    queries = build_inspection_queries(table, max_rows=max_rows)

    try:
        _print_section("Table identity and existence", output)
        _show(spark.sql(queries["identity"]), max_rows=max_rows)

        _print_section("Schema", output)
        _show(spark.sql(queries["schema"]), max_rows=max_rows)

        _print_section("Row count", output)
        row_count = _first_value(spark.sql(queries["row_count"]), "row_count")
        output(f"row_count={row_count}")
        output("A full table count can be expensive on a large table.")

        for title, count_key, data_key in (
            ("Snapshots", "snapshots_count", "snapshots"),
            ("History", "history_count", "history"),
            ("Data files", "files_count", "files"),
        ):
            _print_section(title, output)
            count = _first_value(spark.sql(queries[count_key]), "metadata_count")
            output(f"total_rows={count}; showing_at_most={max_rows}")
            _show(spark.sql(queries[data_key]), max_rows=max_rows)

        _print_section("Partition information", output)
        partition_count = _first_value(
            spark.sql(queries["partitions_count"]),
            "metadata_count",
        )
        partition_frame = spark.sql(queries["partitions"])
        partition_columns = set(getattr(partition_frame, "columns", ()))
        if partition_count == 0:
            output("no partition metadata rows")
        elif "partition" in partition_columns:
            output(
                f"partitioned table; total_rows={partition_count}; "
                f"showing_at_most={max_rows}"
            )
        else:
            output(
                "unpartitioned table (aggregate table statistics); "
                f"total_rows={partition_count}; showing_at_most={max_rows}"
            )
        _show(partition_frame, max_rows=max_rows)
    except IcebergInspectionError:
        raise
    except Exception as error:
        raise IcebergInspectionError(
            f"read-only Iceberg inspection failed for {table}: "
            f"{type(error).__name__}: {error}"
        ) from error


def parse_args(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
) -> argparse.Namespace:
    """Parse the inspector's table and bounded-display options."""
    environment = os.environ if environ is None else environ
    parser = argparse.ArgumentParser(
        description="Inspect an existing Iceberg table without changing it",
    )
    parser.add_argument(
        "--table",
        default=environment.get("ICEBERG_BRONZE_TABLE", DEFAULT_TABLE),
        help="safe dotted Iceberg table identifier",
    )
    parser.add_argument(
        "--max-rows",
        type=_positive_integer,
        default=DEFAULT_MAX_ROWS,
        help="maximum metadata rows to display per section (default: 100)",
    )
    return parser.parse_args(argv)


def run_inspection(
    *,
    table_identifier: str,
    max_rows: int = DEFAULT_MAX_ROWS,
    environ: Mapping[str, str] | None = None,
) -> None:
    """Build the existing Iceberg-enabled Spark session and inspect one table."""
    environment = os.environ if environ is None else environ
    streaming_args = parse_streaming_args(
        ["--table-name", validate_table_identifier(table_identifier)],
        environ=environment,
    )
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
    inspection_error: BaseException | None = None
    try:
        inspect_iceberg_table(
            spark,
            table_identifier,
            max_rows=max_rows,
        )
    except BaseException as error:
        inspection_error = error
        raise
    finally:
        try:
            spark.stop()
        except BaseException as stop_error:
            if inspection_error is None:
                raise
            inspection_error.add_note(
                f"Spark session cleanup also failed: {stop_error!r}"
            )


def main(argv: Sequence[str] | None = None) -> None:
    """Run the read-only Iceberg inspection CLI."""
    args = parse_args(argv)
    run_inspection(
        table_identifier=args.table,
        max_rows=args.max_rows,
    )


if __name__ == "__main__":
    main()
