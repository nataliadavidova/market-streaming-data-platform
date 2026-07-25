"""Unit tests for the read-only Iceberg inspection workflow."""

import pytest

from jobs.streaming.iceberg_bronze import CANONICAL_BRONZE_TABLE_NAME
from jobs.streaming.iceberg_inspection import (
    DEFAULT_TABLE,
    IcebergInspectionError,
    build_inspection_queries,
    inspect_iceberg_table,
    parse_args,
    validate_table_identifier,
)


class RecordingFrame:
    def __init__(
        self,
        rows: list[object] | None = None,
        *,
        columns: list[str] | None = None,
    ) -> None:
        self.rows = [] if rows is None else rows
        self.columns = [] if columns is None else columns
        self.show_calls: list[tuple[int, bool]] = []

    def collect(self) -> list[object]:
        return self.rows

    def show(self, n: int, *, truncate: bool) -> None:
        self.show_calls.append((n, truncate))


class RecordingSpark:
    def __init__(self, frames: dict[str, RecordingFrame]) -> None:
        self.frames = frames
        self.queries: list[str] = []

    def sql(self, query: str) -> RecordingFrame:
        self.queries.append(query)
        for key, frame in self.frames.items():
            if key in query:
                return frame
        raise AssertionError(f"unexpected query: {query}")


def _inspection_frames(
    *,
    partition_count: int = 0,
    partition_columns: list[str] | None = None,
    partition_rows: list[object] | None = None,
) -> dict[str, RecordingFrame]:
    table = "market_catalog.market.bronze_trades"
    return {
        "DESCRIBE TABLE EXTENDED": RecordingFrame(),
        "DESCRIBE TABLE ": RecordingFrame(),
        "COUNT(*) AS row_count": RecordingFrame([{"row_count": 7}]),
        "snapshots": RecordingFrame([{"metadata_count": 2}]),
        "history": RecordingFrame([{"metadata_count": 2}]),
        "files": RecordingFrame([{"metadata_count": 3}]),
        f"COUNT(*) AS metadata_count FROM {table}.partitions": RecordingFrame(
            [{"metadata_count": partition_count}]
        ),
        f"SELECT * FROM {table}.partitions": RecordingFrame(
            partition_rows,
            columns=partition_columns,
        ),
    }


def test_validate_table_identifier_accepts_canonical_name() -> None:
    assert (
        validate_table_identifier("market_catalog.market.bronze_trades")
        == "market_catalog.market.bronze_trades"
    )


@pytest.mark.parametrize(
    "table_identifier",
    [
        "market_catalog.market.bronze_trades; DROP TABLE x",
        "market_catalog.market.bronze trades",
        "market_catalog.market.`bronze_trades`",
        "market_catalog.market.bronze_trades()",
        "market_catalog.market.bronze_trades -- comment",
        "",
        "catalog.namespace.table.extra",
    ],
)
def test_validate_table_identifier_rejects_injection_like_values(
    table_identifier: str,
) -> None:
    with pytest.raises(IcebergInspectionError):
        validate_table_identifier(table_identifier)


def test_parse_args_uses_canonical_environment_default() -> None:
    args = parse_args([], environ={})
    assert DEFAULT_TABLE == CANONICAL_BRONZE_TABLE_NAME
    assert args.table == CANONICAL_BRONZE_TABLE_NAME
    assert args.max_rows == 100


def test_build_inspection_queries_are_read_only_and_stably_ordered() -> None:
    queries = build_inspection_queries(
        "market_catalog.market.bronze_trades",
        max_rows=25,
    )

    assert "ORDER BY committed_at DESC, snapshot_id DESC" in queries["snapshots"]
    assert "ORDER BY made_current_at DESC, snapshot_id DESC" in queries["history"]
    assert "ORDER BY file_path" in queries["files"]
    assert queries["partitions"] == (
        "SELECT * FROM market_catalog.market.bronze_trades.partitions LIMIT 25"
    )
    assert all(
        not any(
            operation in query.upper()
            for operation in (
                "CREATE",
                "ALTER",
                "DROP",
                "INSERT",
                "UPDATE",
                "DELETE",
                "MERGE",
                "CALL",
            )
        )
        for query in queries.values()
    )


def test_inspection_forwards_table_and_handles_unpartitioned_table() -> None:
    frames = _inspection_frames(
        partition_count=1,
        partition_columns=[
            "record_count",
            "file_count",
            "total_data_file_size_in_bytes",
            "last_updated_at",
            "last_updated_snapshot_id",
        ],
        partition_rows=[
            {
                "record_count": 7,
                "file_count": 1,
                "total_data_file_size_in_bytes": 5128,
                "last_updated_at": "2026-07-20T14:34:02.513Z",
                "last_updated_snapshot_id": 8232280423536300118,
            }
        ],
    )
    spark = RecordingSpark(frames)
    output: list[str] = []

    inspect_iceberg_table(
        spark,
        "market_catalog.market.bronze_trades",
        max_rows=10,
        output=output.append,
    )

    assert spark.queries[0] == (
        "DESCRIBE TABLE EXTENDED market_catalog.market.bronze_trades"
    )
    assert any("row_count=7" in line for line in output)
    assert any(
        "unpartitioned table (aggregate table statistics)" in line
        for line in output
    )
    assert frames["DESCRIBE TABLE EXTENDED"].show_calls == [(10, False)]
    assert frames["DESCRIBE TABLE "].show_calls == [(10, False)]
    assert frames["snapshots"].show_calls == [(10, False)]
    assert frames["history"].show_calls == [(10, False)]
    assert frames["files"].show_calls == [(10, False)]
    assert frames[
        "SELECT * FROM market_catalog.market.bronze_trades.partitions"
    ].show_calls == [(10, False)]


def test_inspection_handles_partition_rows() -> None:
    frames = _inspection_frames(
        partition_count=1,
        partition_columns=["partition", "record_count"],
        partition_rows=[{"partition": "BTCUSDT", "record_count": 7}],
    )
    spark = RecordingSpark(frames)
    output: list[str] = []

    inspect_iceberg_table(
        spark,
        "market_catalog.market.bronze_trades",
        output=output.append,
    )

    assert any("total_rows=1" in line for line in output)


def test_inspection_handles_empty_partitions_result_without_error() -> None:
    frames = _inspection_frames(
        partition_count=0,
        partition_columns=["partition", "record_count"],
        partition_rows=[],
    )
    spark = RecordingSpark(frames)
    output: list[str] = []

    inspect_iceberg_table(
        spark,
        "market_catalog.market.bronze_trades",
        output=output.append,
    )

    assert any("no partition metadata rows" in line for line in output)
    assert frames[
        "SELECT * FROM market_catalog.market.bronze_trades.partitions"
    ].show_calls == [(100, False)]


def test_inspection_wraps_spark_sql_errors_with_context() -> None:
    class FailingSpark:
        def sql(self, _query: str) -> object:
            raise RuntimeError("catalog unavailable")

    with pytest.raises(IcebergInspectionError, match="catalog unavailable") as info:
        inspect_iceberg_table(
            FailingSpark(),
            "market_catalog.market.bronze_trades",
        )

    assert isinstance(info.value.__cause__, RuntimeError)


def test_inspection_does_not_hide_missing_count_result() -> None:
    class EmptyCountSpark:
        def sql(self, query: str) -> RecordingFrame:
            if "DESCRIBE" in query:
                return RecordingFrame()
            return RecordingFrame()

    with pytest.raises(IcebergInspectionError, match="no row_count result"):
        inspect_iceberg_table(
            EmptyCountSpark(),
            "market_catalog.market.bronze_trades",
        )


def test_inspection_queries_include_bounded_metadata_limits() -> None:
    queries = build_inspection_queries(
        "market_catalog.market.bronze_trades",
        max_rows=3,
    )
    assert queries["snapshots"].endswith("LIMIT 3")
    assert queries["history"].endswith("LIMIT 3")
    assert queries["files"].endswith("LIMIT 3")
    assert queries["partitions"].endswith("LIMIT 3")


def _patch_run_inspection_dependencies(
    monkeypatch: pytest.MonkeyPatch,
    inspection: object,
    spark: object,
    inspect_fn: object,
) -> None:
    monkeypatch.setattr(
        inspection,
        "build_iceberg_trade_spark_session",
        lambda **_: spark,
    )
    monkeypatch.setattr(
        inspection,
        "parse_streaming_args",
        lambda _argv, environ: type(
            "Args",
            (),
            {
                "app_name": "app",
                "catalog_name": "catalog",
                "catalog_uri": "http://catalog",
                "warehouse": "s3://warehouse",
                "s3_endpoint": "http://minio",
                "s3_region": "us-east-1",
                "s3_access_key": "key",
                "s3_secret_key": "secret",
                "s3_path_style_access": True,
                "s3a_ssl_enabled": False,
            },
        )(),
    )
    monkeypatch.setattr(inspection, "inspect_iceberg_table", inspect_fn)


def test_inspection_success_stop_failure_propagates_stop_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import jobs.streaming.iceberg_inspection as inspection

    class SparkSentinel:
        def __init__(self) -> None:
            self.stop_calls = 0

        def stop(self) -> None:
            self.stop_calls += 1
            raise RuntimeError("stop failed")

    fake_spark = SparkSentinel()
    _patch_run_inspection_dependencies(
        monkeypatch,
        inspection,
        fake_spark,
        lambda *_args, **_kwargs: None,
    )

    with pytest.raises(RuntimeError, match="stop failed"):
        inspection.run_inspection(
            table_identifier="market_catalog.market.bronze_trades",
            environ={},
        )

    assert fake_spark.stop_calls == 1


def test_inspection_failure_stop_failure_preserves_primary_error_and_note(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import jobs.streaming.iceberg_inspection as inspection

    class SparkSentinel:
        def __init__(self) -> None:
            self.stop_calls = 0

        def stop(self) -> None:
            self.stop_calls += 1
            raise RuntimeError("stop failed")

    def fail(*_args: object, **_kwargs: object) -> None:
        try:
            raise RuntimeError("catalog unavailable")
        except RuntimeError as cause:
            raise IcebergInspectionError("inspection failed") from cause

    fake_spark = SparkSentinel()
    _patch_run_inspection_dependencies(
        monkeypatch,
        inspection,
        fake_spark,
        fail,
    )

    with pytest.raises(IcebergInspectionError, match="inspection failed") as info:
        inspection.run_inspection(
            table_identifier="market_catalog.market.bronze_trades",
            environ={},
        )

    assert isinstance(info.value.__cause__, RuntimeError)
    assert any(
        "Spark session cleanup also failed" in note
        for note in getattr(info.value, "__notes__", [])
    )
    assert fake_spark.stop_calls == 1


def test_inspection_session_cleanup_can_be_verified_by_run_wrapper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import jobs.streaming.iceberg_inspection as inspection

    class SparkSentinel:
        def __init__(self) -> None:
            self.stop_calls = 0

        def sql(self, _query: str) -> RecordingFrame:
            return RecordingFrame([{"row_count": 0}])

        def stop(self) -> None:
            self.stop_calls += 1

    spark = SparkSentinel()
    monkeypatch.setattr(inspection, "build_iceberg_trade_spark_session", lambda **_: spark)
    monkeypatch.setattr(
        inspection,
        "parse_streaming_args",
        lambda _argv, environ: type(
            "Args",
            (),
            {
                "app_name": "app",
                "catalog_name": "catalog",
                "catalog_uri": "http://catalog",
                "warehouse": "s3://warehouse",
                "table_name": "catalog.namespace.table",
                "s3_endpoint": "http://minio",
                "s3_region": "us-east-1",
                "s3_access_key": "key",
                "s3_secret_key": "secret",
                "s3_path_style_access": True,
                "s3a_ssl_enabled": False,
            },
        )(),
    )
    monkeypatch.setattr(inspection, "inspect_iceberg_table", lambda *args, **kwargs: None)

    inspection.run_inspection(
        table_identifier="market_catalog.market.bronze_trades",
        environ={},
    )

    assert spark.stop_calls == 1


def test_inspection_session_stops_after_inspection_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import jobs.streaming.iceberg_inspection as inspection

    class SparkSentinel:
        stop_calls = 0

        def stop(self) -> None:
            self.stop_calls += 1

    spark = SparkSentinel()
    monkeypatch.setattr(inspection, "build_iceberg_trade_spark_session", lambda **_: spark)
    monkeypatch.setattr(
        inspection,
        "parse_streaming_args",
        lambda _argv, environ: type(
            "Args",
            (),
            {
                "app_name": "app",
                "catalog_name": "catalog",
                "catalog_uri": "http://catalog",
                "warehouse": "s3://warehouse",
                "table_name": "catalog.namespace.table",
                "s3_endpoint": "http://minio",
                "s3_region": "us-east-1",
                "s3_access_key": "key",
                "s3_secret_key": "secret",
                "s3_path_style_access": True,
                "s3a_ssl_enabled": False,
            },
        )(),
    )

    def fail(*_args: object, **_kwargs: object) -> None:
        raise IcebergInspectionError("inspection failed")

    monkeypatch.setattr(inspection, "inspect_iceberg_table", fail)

    with pytest.raises(IcebergInspectionError, match="inspection failed"):
        inspection.run_inspection(
            table_identifier="market_catalog.market.bronze_trades",
            environ={},
        )

    assert spark.stop_calls == 1
