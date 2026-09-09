"""Orchestrate one snapshot-bound Silver-to-ClickHouse serving rebuild."""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
import os
import sys
from typing import Any

from jobs.serving import clickhouse_schema
from jobs.serving import clickhouse_staging_loader as loader
from jobs.serving import silver_staging_fingerprint as fingerprint
from jobs.serving.silver_source_validation import (
    DEFAULT_SILVER_TABLE,
    SilverSourceValidationError,
    resolve_current_snapshot_id,
)


class ClickHouseRebuildError(RuntimeError):
    """Raised when the stop-on-failure serving rebuild cannot complete."""


@dataclass(frozen=True)
class RebuildResult:
    """Bounded result assembled from the existing serving primitives."""

    snapshot_id: int
    source_row_count: int
    staging_row_count: int
    exact_copy: bool
    pre_target_row_count: int
    post_target_row_count: int
    status: str


def rebuild_serving(
    spark: Any,
    config: clickhouse_schema.ClickHouseConfig,
    *,
    resolve_snapshot: Callable[[Any, str], int] = resolve_current_snapshot_id,
    load_snapshot: Callable[..., loader.StagingLoadSummary] = loader.load_silver_snapshot_to_staging,
    validate_snapshot: Callable[..., fingerprint.SilverStagingFingerprintResult] = fingerprint.validate_staging_against_snapshot,
    exchange_tables: Callable[..., clickhouse_schema.ServingExchangeResult] = clickhouse_schema.exchange_serving_tables,
    connect_client: Callable[..., clickhouse_schema.ClickHouseClient] = clickhouse_schema.connect_client,
    source_table: str = DEFAULT_SILVER_TABLE,
) -> RebuildResult:
    """Run load, exact-copy gate, and one atomic exchange for snapshot A."""
    snapshot_id = resolve_snapshot(spark, source_table)
    try:
        load_result = load_snapshot(
            spark,
            config,
            snapshot_id=snapshot_id,
            source_table=source_table,
        )
        validation_result = validate_snapshot(spark, config, snapshot_id)
        if not validation_result.exact_copy:
            raise ClickHouseRebuildError("exact-copy validation did not pass")
        exchange_result = exchange_tables(connect_client(config), config)
    except ClickHouseRebuildError:
        raise
    except Exception as exc:
        raise ClickHouseRebuildError("serving rebuild failed") from exc
    return RebuildResult(
        snapshot_id=snapshot_id,
        source_row_count=load_result.source_row_count,
        staging_row_count=validation_result.staging_row_count,
        exact_copy=validation_result.exact_copy,
        pre_target_row_count=exchange_result.pre_target_row_count,
        post_target_row_count=exchange_result.post_target_row_count,
        status="published",
    )


def run_rebuild(
    *,
    environ: Mapping[str, str] | None = None,
) -> RebuildResult:
    """Own one shared Spark session for the one-command rebuild."""
    environment = os.environ if environ is None else environ
    config = clickhouse_schema.ClickHouseConfig.from_environment(environment)
    spark = loader._build_spark(environment)
    try:
        return rebuild_serving(spark, config)
    finally:
        spark.stop()


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse the single supported rebuild operation."""
    parser = argparse.ArgumentParser(description="Rebuild ClickHouse Silver serving tables")
    parser.add_argument("operation", choices=("rebuild",))
    return parser.parse_args(argv)


def print_result(result: RebuildResult, output: Callable[[str], None]) -> None:
    """Print only bounded deterministic workflow fields."""
    output(
        f"snapshot_id={result.snapshot_id} "
        f"source_row_count={result.source_row_count} "
        f"staging_row_count={result.staging_row_count} "
        f"exact_copy={str(result.exact_copy).lower()} "
        f"pre_target_row_count={result.pre_target_row_count} "
        f"post_target_row_count={result.post_target_row_count} "
        f"status={result.status}"
    )


def main(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
) -> int:
    """Run the one-command serving rebuild."""
    parse_args(argv)
    try:
        print_result(run_rebuild(environ=environ), print)
    except (
        ClickHouseRebuildError,
        clickhouse_schema.ClickHouseControlPlaneError,
        loader.ClickHouseStagingLoadError,
        fingerprint.SilverStagingFingerprintError,
        SilverSourceValidationError,
    ) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
