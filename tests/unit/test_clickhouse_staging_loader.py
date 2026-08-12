"""Unit tests for the snapshot-bound ClickHouse staging loader."""

from dataclasses import dataclass
import pytest

from jobs.serving import clickhouse_schema
from jobs.serving import clickhouse_staging_loader as loader
from jobs.serving.silver_source_validation import SilverSourceSummary


def _environment(**overrides: str) -> dict[str, str]:
    values = {
        "CLICKHOUSE_USER": "market_loader",
        "CLICKHOUSE_PASSWORD": "secret-value",
    }
    values.update(overrides)
    return values


def _summary(snapshot_id: int = 456, row_count: int = 2) -> SilverSourceSummary:
    return SilverSourceSummary(
        table_name=loader.DEFAULT_SILVER_TABLE,
        snapshot_id=snapshot_id,
        row_count=row_count,
        distinct_symbol_count=1,
        symbols=("BTCUSDT",),
        per_symbol_counts=(("BTCUSDT", row_count),),
        null_counts=tuple(
            (column, 0)
            for column in loader.EXPECTED_SILVER_COLUMNS
        ),
        displayed_symbols=("BTCUSDT",),
        displayed_per_symbol_counts=(("BTCUSDT", row_count),),
        omitted_symbol_count=0,
    )


class FakeFrame:
    def __init__(self, name: str) -> None:
        self.name = name
        self.columns = list(loader.EXPECTED_SILVER_COLUMNS)
        self.write = FakeWriter()


class FakeWriter:
    def __init__(self) -> None:
        self.formats: list[str] = []
        self.options: dict[str, str] = {}
        self.modes: list[str] = []
        self.saved = False

    def format(self, value: str) -> "FakeWriter":
        self.formats.append(value)
        return self

    def option(self, key: str, value: str) -> "FakeWriter":
        self.options[key] = value
        return self

    def mode(self, value: str) -> "FakeWriter":
        self.modes.append(value)
        return self

    def save(self) -> None:
        self.saved = True


@dataclass
class FakeClient:
    name: str = "client"


def _patch_source(monkeypatch: pytest.MonkeyPatch, frame: FakeFrame, events: list[str]) -> None:
    monkeypatch.setattr(
        loader,
        "resolve_current_snapshot_id",
        lambda *_args: (events.append("resolve") or 456),
    )
    monkeypatch.setattr(
        loader,
        "read_silver_snapshot",
        lambda *_args: (events.append("read") or frame),
    )
    monkeypatch.setattr(
        loader,
        "validate_silver_schema",
        lambda _frame: events.append("schema"),
    )
    monkeypatch.setattr(
        loader,
        "validate_required_nulls",
        lambda _frame: (events.append("nulls") or {column: 0 for column in loader.EXPECTED_SILVER_COLUMNS}),
    )
    monkeypatch.setattr(
        loader,
        "compute_source_summary",
        lambda _frame, **_: (events.append("summary") or _summary()),
    )


def _patch_control_plane(
    monkeypatch: pytest.MonkeyPatch,
    events: list[str],
    *,
    client: FakeClient | None = None,
    count: int = 2,
) -> FakeClient:
    control_client = client or FakeClient()
    monkeypatch.setattr(
        clickhouse_schema,
        "ensure_schema",
        lambda _client, _config: events.append("schema_control"),
    )
    monkeypatch.setattr(
        clickhouse_schema,
        "truncate_staging",
        lambda _client, _config: events.append("truncate"),
    )
    monkeypatch.setattr(
        clickhouse_schema,
        "staging_row_count",
        lambda _client, _config: (events.append("count") or count),
    )
    return control_client


def test_load_order_and_dataframe_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    frame = FakeFrame("snapshot-a")
    events: list[str] = []
    _patch_source(monkeypatch, frame, events)
    client = _patch_control_plane(monkeypatch, events)
    written: list[object] = []
    monkeypatch.setattr(
        loader,
        "write_snapshot_to_staging",
        lambda dataframe, _config: (events.append("write") or written.append(dataframe)),
    )

    summary = loader.load_silver_snapshot_to_staging(
        object(),
        clickhouse_schema.ClickHouseConfig.from_environment(_environment()),
        client_factory=lambda _config: (events.append("connect") or client),
    )

    assert events == [
        "resolve", "read", "schema", "nulls", "summary",
        "connect", "schema_control", "truncate", "write", "count",
    ]
    assert written == [frame]
    assert summary.snapshot_id == 456
    assert summary.source_row_count == 2
    assert summary.staging_row_count == 2
    assert summary.load_status == "transport_row_count_verified"


def test_snapshot_binding_keeps_a_after_current_advances_to_b(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame_a = FakeFrame("snapshot-a")
    frame_b = FakeFrame("snapshot-b")
    current_snapshot = {"id": 456}
    requested_ids: list[int] = []

    def resolve(*_args: object) -> int:
        snapshot_id = current_snapshot["id"]
        current_snapshot["id"] = 789
        return snapshot_id

    def read(_spark: object, snapshot_id: int, _table: str) -> FakeFrame:
        requested_ids.append(snapshot_id)
        return {456: frame_a, 789: frame_b}[snapshot_id]

    monkeypatch.setattr(loader, "resolve_current_snapshot_id", resolve)
    monkeypatch.setattr(loader, "read_silver_snapshot", read)
    monkeypatch.setattr(loader, "validate_silver_schema", lambda _frame: None)
    monkeypatch.setattr(loader, "validate_required_nulls", lambda _frame: {})
    monkeypatch.setattr(loader, "compute_source_summary", lambda frame, **_: _summary(456 if frame is frame_a else 789, 1 if frame is frame_a else 2))
    monkeypatch.setattr(clickhouse_schema, "ensure_schema", lambda *_args: None)
    monkeypatch.setattr(clickhouse_schema, "truncate_staging", lambda *_args: None)
    monkeypatch.setattr(clickhouse_schema, "staging_row_count", lambda *_args: 1)
    written: list[object] = []
    monkeypatch.setattr(loader, "write_snapshot_to_staging", lambda frame, _config: written.append(frame))

    summary = loader.load_silver_snapshot_to_staging(
        object(),
        clickhouse_schema.ClickHouseConfig.from_environment(_environment()),
        client_factory=lambda _config: FakeClient(),
    )

    assert requested_ids == [456]
    assert written == [frame_a]
    assert summary.snapshot_id == 456
    assert summary.source_row_count == 1


def test_source_schema_failure_prevents_control_plane_and_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = FakeFrame("invalid-schema")
    monkeypatch.setattr(loader, "resolve_current_snapshot_id", lambda *_: 456)
    monkeypatch.setattr(loader, "read_silver_snapshot", lambda *_: frame)
    monkeypatch.setattr(
        loader,
        "validate_silver_schema",
        lambda _frame: (_ for _ in ()).throw(loader.SilverSourceValidationError("schema mismatch")),
    )
    called: list[str] = []
    monkeypatch.setattr(clickhouse_schema, "ensure_schema", lambda *_: called.append("schema"))
    monkeypatch.setattr(loader, "write_snapshot_to_staging", lambda *_: called.append("write"))

    with pytest.raises(loader.SilverSourceValidationError):
        loader.load_silver_snapshot_to_staging(
            object(),
            clickhouse_schema.ClickHouseConfig.from_environment(_environment()),
            client_factory=lambda _config: FakeClient(),
        )
    assert called == []


def test_null_failure_prevents_control_plane_and_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = FakeFrame("null-invalid")
    monkeypatch.setattr(loader, "resolve_current_snapshot_id", lambda *_: 456)
    monkeypatch.setattr(loader, "read_silver_snapshot", lambda *_: frame)
    monkeypatch.setattr(loader, "validate_silver_schema", lambda _frame: None)
    monkeypatch.setattr(
        loader,
        "validate_required_nulls",
        lambda _frame: (_ for _ in ()).throw(loader.SilverSourceValidationError("NULL validation failed")),
    )
    called: list[str] = []
    monkeypatch.setattr(clickhouse_schema, "ensure_schema", lambda *_: called.append("schema"))
    monkeypatch.setattr(loader, "write_snapshot_to_staging", lambda *_: called.append("write"))

    with pytest.raises(loader.SilverSourceValidationError):
        loader.load_silver_snapshot_to_staging(
            object(),
            clickhouse_schema.ClickHouseConfig.from_environment(_environment()),
            client_factory=lambda _config: FakeClient(),
        )
    assert called == []


@pytest.mark.parametrize("failure_step", ["schema", "truncate"])
def test_control_plane_failures_prevent_jdbc_write(
    monkeypatch: pytest.MonkeyPatch,
    failure_step: str,
) -> None:
    frame = FakeFrame("snapshot-a")
    events: list[str] = []
    _patch_source(monkeypatch, frame, events)
    monkeypatch.setattr(
        clickhouse_schema,
        "ensure_schema",
        lambda *_: (_ for _ in ()).throw(clickhouse_schema.ClickHouseControlPlaneError("control failure"))
        if failure_step == "schema" else events.append("schema_control"),
    )
    monkeypatch.setattr(
        clickhouse_schema,
        "truncate_staging",
        lambda *_: (_ for _ in ()).throw(clickhouse_schema.ClickHouseControlPlaneError("truncate failure"))
        if failure_step == "truncate" else events.append("truncate"),
    )
    monkeypatch.setattr(loader, "write_snapshot_to_staging", lambda *_: events.append("write"))

    with pytest.raises(loader.ClickHouseStagingLoadError):
        loader.load_silver_snapshot_to_staging(
            object(),
            clickhouse_schema.ClickHouseConfig.from_environment(_environment()),
            client_factory=lambda _config: FakeClient(),
        )
    assert "write" not in events


def test_jdbc_write_uses_http_append_and_no_overwrite_or_truncate() -> None:
    frame = FakeFrame("snapshot-a")
    config = clickhouse_schema.ClickHouseConfig.from_environment(_environment())

    loader.write_snapshot_to_staging(frame, config)

    assert frame.write.formats == ["jdbc"]
    assert frame.write.options == {
        "url": "jdbc:clickhouse://localhost:18123/market_analytics",
        "dbtable": "market_analytics.silver_trades_staging",
        "driver": "com.clickhouse.jdbc.Driver",
        "user": "market_loader",
        "password": "secret-value",
    }
    assert frame.write.modes == ["append"]
    assert frame.write.saved is True


def test_serving_spark_packages_keep_iceberg_and_add_jdbc_driver() -> None:
    packages = f"{loader.SPARK_ICEBERG_PACKAGES},{loader.CLICKHOUSE_JDBC_PACKAGE}"

    assert loader.CLICKHOUSE_JDBC_PACKAGE == "com.clickhouse:clickhouse-jdbc:0.8.6"
    assert loader.SPARK_ICEBERG_PACKAGES in packages
    assert packages.endswith(loader.CLICKHOUSE_JDBC_PACKAGE)


def test_duplicate_rows_are_passed_to_writer_without_transformation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = FakeFrame("duplicate-logical-rows")
    events: list[str] = []
    _patch_source(monkeypatch, frame, events)
    _patch_control_plane(monkeypatch, events)
    written: list[object] = []
    monkeypatch.setattr(loader, "write_snapshot_to_staging", lambda dataframe, _config: written.append(dataframe))

    loader.load_silver_snapshot_to_staging(
        object(),
        clickhouse_schema.ClickHouseConfig.from_environment(_environment()),
        client_factory=lambda _config: FakeClient(),
    )

    assert written == [frame]
    assert not hasattr(loader, "exchange_tables")


def test_row_count_mismatch_fails_after_write_without_publication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = FakeFrame("snapshot-a")
    events: list[str] = []
    _patch_source(monkeypatch, frame, events)
    _patch_control_plane(monkeypatch, events, count=1)
    monkeypatch.setattr(loader, "write_snapshot_to_staging", lambda *_: events.append("write"))

    with pytest.raises(loader.ClickHouseStagingLoadError, match="row count"):
        loader.load_silver_snapshot_to_staging(
            object(),
            clickhouse_schema.ClickHouseConfig.from_environment(_environment()),
            client_factory=lambda _config: FakeClient(),
        )
    assert events[-1] == "count"
    assert not hasattr(loader, "exchange_tables")


def test_cli_success_is_bounded_and_hides_password(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    summary = _summary()
    monkeypatch.setattr(loader, "run_staging_load", lambda **_: loader.StagingLoadSummary(
        source_table=summary.table_name,
        snapshot_id=summary.snapshot_id,
        source_row_count=summary.row_count,
        staging_table=loader.STAGING_TABLE,
        staging_row_count=summary.row_count,
        database="market_analytics",
        load_status="transport_row_count_verified",
    ))

    assert loader.main(["load"], environ=_environment()) == 0
    output = capsys.readouterr()
    assert "status=transport_row_count_verified" in output.out
    assert "snapshot_id=456" in output.out
    assert "secret-value" not in output.out + output.err


@pytest.mark.parametrize(
    "error",
    [
        loader.SilverSourceValidationError("schema mismatch"),
        clickhouse_schema.ClickHouseControlPlaneError("schema mismatch"),
        loader.ClickHouseStagingLoadError("JDBC write failed"),
        loader.ClickHouseStagingLoadError("row count mismatch"),
    ],
)
def test_cli_failures_are_nonzero_and_bounded(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    error: Exception,
) -> None:
    monkeypatch.setattr(loader, "run_staging_load", lambda **_: (_ for _ in ()).throw(error))

    assert loader.main(["load"], environ=_environment()) == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert "error:" in output.err
    assert "secret-value" not in output.out + output.err
