"""Unit tests for the one-command ClickHouse serving rebuild orchestration."""

import pytest

from jobs.serving import clickhouse_rebuild as rebuild
from jobs.serving import clickhouse_schema
from jobs.serving import clickhouse_staging_loader as loader
from jobs.serving import silver_staging_fingerprint as fingerprint


def _config() -> clickhouse_schema.ClickHouseConfig:
    return clickhouse_schema.ClickHouseConfig(
        database="market_analytics",
        user="market_loader",
        password="secret-value",
    )


def _load_result(snapshot_id: int = 456) -> loader.StagingLoadSummary:
    return loader.StagingLoadSummary(
        source_table="market_catalog.market.silver_trades",
        snapshot_id=snapshot_id,
        source_row_count=184,
        staging_table="silver_trades_staging",
        staging_row_count=184,
        database="market_analytics",
        load_status="transport_row_count_verified",
    )


def _validation_result(snapshot_id: int = 456, exact_copy: bool = True) -> fingerprint.SilverStagingFingerprintResult:
    return fingerprint.SilverStagingFingerprintResult(
        source_table="market_catalog.market.silver_trades",
        snapshot_id=snapshot_id,
        staging_table="market_analytics.silver_trades_staging",
        source_row_count=184,
        staging_row_count=184,
        source_distinct_symbol_count=3,
        staging_distinct_symbol_count=3,
        source_symbols=("BTCUSDT", "ETHUSDT", "SOLUSDT"),
        staging_symbols=("BTCUSDT", "ETHUSDT", "SOLUSDT"),
        source_per_symbol_counts=(("BTCUSDT", 164), ("ETHUSDT", 13), ("SOLUSDT", 7)),
        staging_per_symbol_counts=(("BTCUSDT", 164), ("ETHUSDT", 13), ("SOLUSDT", 7)),
        source_fingerprint="a" * 64,
        staging_fingerprint="a" * 64,
        exact_copy=exact_copy,
        validation_status="exact_copy_verified" if exact_copy else "mismatch",
    )


def _exchange_result() -> clickhouse_schema.ServingExchangeResult:
    return clickhouse_schema.ServingExchangeResult(
        database="market_analytics",
        target_table="silver_trades",
        staging_table="silver_trades_staging",
        pre_target_row_count=0,
        pre_staging_row_count=184,
        post_target_row_count=184,
        post_staging_row_count=0,
        exchange_status="exchanged",
    )


def test_rebuild_order_and_single_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []
    current_snapshot = {"id": 456}

    def resolve(_spark: object, _table: str) -> int:
        events.append("resolve")
        current_snapshot["id"] = 789
        return 456

    def load(_spark: object, _config: object, **kwargs: object) -> loader.StagingLoadSummary:
        events.append(f"load:{kwargs['snapshot_id']}")
        return _load_result(int(kwargs["snapshot_id"]))

    def validate(_spark: object, _config: object, snapshot_id: int) -> fingerprint.SilverStagingFingerprintResult:
        events.append(f"validate:{snapshot_id}")
        return _validation_result(snapshot_id)

    def exchange(_client: object, _config: object) -> clickhouse_schema.ServingExchangeResult:
        events.append("exchange")
        return _exchange_result()

    result = rebuild.rebuild_serving(
        object(),
        _config(),
        resolve_snapshot=resolve,
        load_snapshot=load,
        validate_snapshot=validate,
        exchange_tables=exchange,
        connect_client=lambda _config: events.append("connect") or object(),
    )

    assert current_snapshot["id"] == 789
    assert events == ["resolve", "load:456", "validate:456", "connect", "exchange"]
    assert result.snapshot_id == 456
    assert result.exact_copy is True
    assert result.status == "published"


def test_loader_failure_stops_before_validation_and_exchange() -> None:
    events: list[str] = []

    def load(*_args: object, **_kwargs: object) -> loader.StagingLoadSummary:
        events.append("load")
        raise loader.ClickHouseStagingLoadError("load failed")

    with pytest.raises(rebuild.ClickHouseRebuildError, match="serving rebuild failed"):
        rebuild.rebuild_serving(
            object(),
            _config(),
            resolve_snapshot=lambda *_: 456,
            load_snapshot=load,
            validate_snapshot=lambda *_: events.append("validate") or _validation_result(),
            exchange_tables=lambda *_: events.append("exchange") or _exchange_result(),
            connect_client=lambda _config: object(),
        )
    assert events == ["load"]


def test_exact_copy_false_stops_before_exchange() -> None:
    events: list[str] = []

    with pytest.raises(rebuild.ClickHouseRebuildError, match="exact-copy"):
        rebuild.rebuild_serving(
            object(),
            _config(),
            resolve_snapshot=lambda *_: 456,
            load_snapshot=lambda *_args, **_kwargs: _load_result(),
            validate_snapshot=lambda *_: _validation_result(exact_copy=False),
            exchange_tables=lambda *_: events.append("exchange") or _exchange_result(),
            connect_client=lambda _config: object(),
        )
    assert events == []


def test_validation_failure_stops_before_exchange() -> None:
    events: list[str] = []

    def validate(*_args: object) -> fingerprint.SilverStagingFingerprintResult:
        events.append("validate")
        raise fingerprint.SilverStagingFingerprintError("fingerprint failed")

    with pytest.raises(rebuild.ClickHouseRebuildError, match="serving rebuild failed"):
        rebuild.rebuild_serving(
            object(),
            _config(),
            resolve_snapshot=lambda *_: 456,
            load_snapshot=lambda *_args, **_kwargs: _load_result(),
            validate_snapshot=validate,
            exchange_tables=lambda *_: events.append("exchange") or _exchange_result(),
            connect_client=lambda _config: object(),
        )
    assert events == ["validate"]


def test_exchange_failure_is_not_retried() -> None:
    calls = 0

    def exchange(*_args: object) -> clickhouse_schema.ServingExchangeResult:
        nonlocal calls
        calls += 1
        raise clickhouse_schema.ClickHouseControlPlaneError("exchange failed")

    with pytest.raises(rebuild.ClickHouseRebuildError, match="serving rebuild failed"):
        rebuild.rebuild_serving(
            object(),
            _config(),
            resolve_snapshot=lambda *_: 456,
            load_snapshot=lambda *_args, **_kwargs: _load_result(),
            validate_snapshot=lambda *_: _validation_result(),
            exchange_tables=exchange,
            connect_client=lambda _config: object(),
        )
    assert calls == 1


def test_success_result_uses_existing_primitive_results() -> None:
    result = rebuild.rebuild_serving(
        object(),
        _config(),
        resolve_snapshot=lambda *_: 456,
        load_snapshot=lambda *_args, **_kwargs: _load_result(),
        validate_snapshot=lambda *_: _validation_result(),
        exchange_tables=lambda *_: _exchange_result(),
        connect_client=lambda _config: object(),
    )

    assert result.source_row_count == 184
    assert result.staging_row_count == 184
    assert result.pre_target_row_count == 0
    assert result.post_target_row_count == 184


def test_cli_success_is_bounded_and_hides_credentials(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    result = rebuild.RebuildResult(
        snapshot_id=456,
        source_row_count=184,
        staging_row_count=184,
        exact_copy=True,
        pre_target_row_count=0,
        post_target_row_count=184,
        status="published",
    )
    monkeypatch.setattr(rebuild, "run_rebuild", lambda **_: result)

    assert rebuild.main(["rebuild"], environ={"CLICKHOUSE_PASSWORD": "secret-value"}) == 0
    output = capsys.readouterr()
    assert "snapshot_id=456" in output.out
    assert "exact_copy=true" in output.out
    assert "status=published" in output.out
    assert "secret-value" not in output.out + output.err


@pytest.mark.parametrize(
    "error",
    [
        rebuild.ClickHouseRebuildError("exact-copy validation did not pass"),
        clickhouse_schema.ClickHouseControlPlaneError("exchange failed"),
    ],
)
def test_cli_failure_is_nonzero_and_bounded(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    error: Exception,
) -> None:
    monkeypatch.setattr(rebuild, "run_rebuild", lambda **_: (_ for _ in ()).throw(error))

    assert rebuild.main(["rebuild"], environ={"CLICKHOUSE_PASSWORD": "secret-value"}) == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert "error:" in output.err
    assert "secret-value" not in output.out + output.err


def test_cli_rejects_missing_credentials(capsys: pytest.CaptureFixture[str]) -> None:
    assert rebuild.main(["rebuild"], environ={}) == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert "CLICKHOUSE_USER" in output.err
