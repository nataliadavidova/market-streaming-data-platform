"""Tests for the explicit canonical Bronze schema migration."""

from pathlib import Path
from types import SimpleNamespace

import pytest

import jobs.streaming.iceberg_bronze_migration as migration
from jobs.streaming.iceberg_bronze import (
    BRONZE_TRADE_COLUMNS,
    CANONICAL_BRONZE_TABLE_NAME as SHARED_CANONICAL_BRONZE_TABLE_NAME,
    QUALITY_BRONZE_COLUMNS as SHARED_QUALITY_BRONZE_COLUMNS,
)
from jobs.streaming.iceberg_bronze_migration import (
    BronzeMigrationResult,
    BronzeSchemaState,
    CANONICAL_BRONZE_TABLE_NAME,
    IcebergBronzeMigrationError,
    QUALITY_BRONZE_COLUMNS,
    bronze_quality_migration_sql,
    inspect_bronze_schema_state,
    migrate_bronze_table_to_quality_contract,
    validate_bronze_table_name,
)


class RecordingResult:
    def __init__(self, rows: list[object]) -> None:
        self.rows = rows

    def collect(self) -> list[object]:
        return self.rows


def schema_rows(columns: tuple[tuple[str, str], ...]) -> list[dict[str, str]]:
    return [{"col_name": name, "data_type": data_type} for name, data_type in columns]


class RecordingSpark:
    def __init__(self, rows: list[object]) -> None:
        self.rows = rows
        self.queries: list[str] = []

    def sql(self, query: str) -> RecordingResult:
        self.queries.append(query)
        return RecordingResult(self.rows)


class TransitionSpark:
    def __init__(self, initial_rows: list[object], final_rows: list[object] | None = None) -> None:
        self.rows = initial_rows
        self.final_rows = initial_rows if final_rows is None else final_rows
        self.queries: list[str] = []

    def sql(self, query: str) -> RecordingResult:
        self.queries.append(query)
        if query.startswith("ALTER TABLE"):
            self.rows = self.final_rows
        return RecordingResult(self.rows)


def test_canonical_identifier_is_exact() -> None:
    assert validate_bronze_table_name(CANONICAL_BRONZE_TABLE_NAME) == (
        CANONICAL_BRONZE_TABLE_NAME
    )
    assert CANONICAL_BRONZE_TABLE_NAME == SHARED_CANONICAL_BRONZE_TABLE_NAME
    assert QUALITY_BRONZE_COLUMNS is SHARED_QUALITY_BRONZE_COLUMNS


@pytest.mark.parametrize(
    "table_name",
    [
        "market_catalog.market.other",
        " market_catalog.market.bronze_trades",
        "market_catalog.market.bronze_trades ",
        "MARKET_CATALOG.MARKET.BRONZE_TRADES",
        "market_catalog.`market`.bronze_trades",
        "market_catalog.market.bronze_trades; DROP TABLE x",
    ],
)
def test_noncanonical_target_is_rejected_before_sql(table_name: str) -> None:
    with pytest.raises(IcebergBronzeMigrationError):
        validate_bronze_table_name(table_name)


def test_legacy_schema_is_recognized() -> None:
    spark = RecordingSpark(schema_rows(BRONZE_TRADE_COLUMNS))

    assert inspect_bronze_schema_state(spark) is BronzeSchemaState.LEGACY_13_COLUMN
    assert spark.queries == [f"DESCRIBE TABLE {CANONICAL_BRONZE_TABLE_NAME}"]


def test_quality_schema_is_recognized() -> None:
    spark = RecordingSpark(schema_rows(QUALITY_BRONZE_COLUMNS))

    assert inspect_bronze_schema_state(spark) is BronzeSchemaState.QUALITY_15_COLUMN


@pytest.mark.parametrize(
    "columns",
    [
        QUALITY_BRONZE_COLUMNS[:-1],
        QUALITY_BRONZE_COLUMNS[:13] + (QUALITY_BRONZE_COLUMNS[14], QUALITY_BRONZE_COLUMNS[13]),
        QUALITY_BRONZE_COLUMNS + (("extra", "STRING"),),
        tuple((name, data_type) for name, data_type in QUALITY_BRONZE_COLUMNS if name != "symbol"),
        tuple(
            (name, "DECIMAL(20,5)" if name == "price" else data_type)
            for name, data_type in QUALITY_BRONZE_COLUMNS
        ),
        tuple(
            (name, "ARRAY<INT>" if name == "validation_errors" else data_type)
            for name, data_type in QUALITY_BRONZE_COLUMNS
        ),
    ],
)
def test_schema_variations_are_incompatible(
    columns: tuple[tuple[str, str], ...],
) -> None:
    spark = RecordingSpark(schema_rows(columns))

    assert inspect_bronze_schema_state(spark) is BronzeSchemaState.INCOMPATIBLE


def test_describe_blank_and_service_rows_are_ignored() -> None:
    rows = schema_rows(QUALITY_BRONZE_COLUMNS)
    rows.extend(
        [
            {"col_name": "", "data_type": ""},
            {"col_name": "# Partition Information", "data_type": ""},
            {"col_name": "partition", "data_type": "string"},
        ]
    )
    spark = RecordingSpark(rows)

    assert inspect_bronze_schema_state(spark) is BronzeSchemaState.QUALITY_15_COLUMN


def test_migration_sql_is_exact() -> None:
    assert bronze_quality_migration_sql() == """ALTER TABLE market_catalog.market.bronze_trades
ADD COLUMNS (
is_valid BOOLEAN,
validation_errors ARRAY<STRING>
)"""


def test_legacy_state_executes_one_alter_and_validates_afterward() -> None:
    spark = TransitionSpark(
        schema_rows(BRONZE_TRADE_COLUMNS),
        schema_rows(QUALITY_BRONZE_COLUMNS),
    )

    report = migrate_bronze_table_to_quality_contract(spark)

    assert report.result is BronzeMigrationResult.MIGRATED
    assert report.initial_state is BronzeSchemaState.LEGACY_13_COLUMN
    assert report.final_state is BronzeSchemaState.QUALITY_15_COLUMN
    assert spark.queries.count(bronze_quality_migration_sql()) == 1
    assert spark.queries.count(f"DESCRIBE TABLE {CANONICAL_BRONZE_TABLE_NAME}") == 2


def test_already_migrated_state_executes_no_alter() -> None:
    spark = RecordingSpark(schema_rows(QUALITY_BRONZE_COLUMNS))

    report = migrate_bronze_table_to_quality_contract(spark)

    assert report.result is BronzeMigrationResult.ALREADY_MIGRATED
    assert all(not query.startswith("ALTER TABLE") for query in spark.queries)


def test_incompatible_state_executes_no_alter() -> None:
    spark = RecordingSpark(schema_rows(QUALITY_BRONZE_COLUMNS[:-1]))

    with pytest.raises(IcebergBronzeMigrationError, match="incompatible"):
        migrate_bronze_table_to_quality_contract(spark)

    assert all(not query.startswith("ALTER TABLE") for query in spark.queries)


def test_post_alter_mismatch_fails() -> None:
    spark = TransitionSpark(
        schema_rows(BRONZE_TRADE_COLUMNS),
        schema_rows(BRONZE_TRADE_COLUMNS),
    )

    with pytest.raises(IcebergBronzeMigrationError, match="did not produce"):
        migrate_bronze_table_to_quality_contract(spark)


def test_migration_sql_failure_preserves_cause() -> None:
    class FailingSpark(RecordingSpark):
        def sql(self, query: str) -> RecordingResult:
            self.queries.append(query)
            if query.startswith("ALTER TABLE"):
                raise ValueError("ALTER unsupported")
            return RecordingResult(schema_rows(BRONZE_TRADE_COLUMNS))

    with pytest.raises(IcebergBronzeMigrationError) as raised:
        migrate_bronze_table_to_quality_contract(FailingSpark([]))

    assert isinstance(raised.value.__cause__, ValueError)


def test_injected_spark_is_not_stopped() -> None:
    spark = RecordingSpark(schema_rows(QUALITY_BRONZE_COLUMNS))

    migrate_bronze_table_to_quality_contract(spark)

    assert not hasattr(spark, "stop")


class OwnedSpark(TransitionSpark):
    def __init__(self, rows: list[object]) -> None:
        super().__init__(rows)
        self.stop_calls = 0
        self.stop_error: BaseException | None = None

    def stop(self) -> None:
        self.stop_calls += 1
        if self.stop_error is not None:
            raise self.stop_error


def _patch_owned_runner(monkeypatch: pytest.MonkeyPatch, spark: OwnedSpark) -> None:
    monkeypatch.setattr(migration, "build_iceberg_trade_spark_session", lambda **_: spark)


def test_owned_spark_stops_exactly_once(monkeypatch: pytest.MonkeyPatch) -> None:
    spark = OwnedSpark(schema_rows(QUALITY_BRONZE_COLUMNS))
    _patch_owned_runner(monkeypatch, spark)

    migration.run_migration(environ={})

    assert spark.stop_calls == 1


def test_primary_migration_error_remains_primary_when_cleanup_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spark = OwnedSpark(schema_rows(QUALITY_BRONZE_COLUMNS))
    spark.stop_error = RuntimeError("cleanup failed")
    _patch_owned_runner(monkeypatch, spark)
    original = IcebergBronzeMigrationError("migration failed")

    def fail_migration(_spark: object) -> object:
        raise original

    monkeypatch.setattr(migration, "migrate_bronze_table_to_quality_contract", fail_migration)

    with pytest.raises(IcebergBronzeMigrationError) as raised:
        migration.run_migration(environ={})

    assert raised.value is original
    assert any("cleanup failed" in note for note in raised.value.__notes__)
    assert spark.stop_calls == 1


def test_successful_migration_with_cleanup_failure_propagates_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spark = OwnedSpark(schema_rows(QUALITY_BRONZE_COLUMNS))
    spark.stop_error = RuntimeError("cleanup failed")
    _patch_owned_runner(monkeypatch, spark)

    with pytest.raises(RuntimeError, match="cleanup failed"):
        migration.run_migration(environ={})

    assert spark.stop_calls == 1


def test_cli_reports_already_migrated(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    report = migration.MigrationReport(
        CANONICAL_BRONZE_TABLE_NAME,
        BronzeSchemaState.QUALITY_15_COLUMN,
        BronzeSchemaState.QUALITY_15_COLUMN,
        BronzeMigrationResult.ALREADY_MIGRATED,
    )
    monkeypatch.setattr(migration, "run_migration", lambda: report)

    migration.main([])

    output = capsys.readouterr().out
    assert "initial_state=QUALITY_15_COLUMN" in output
    assert "alter_table_executed=False" in output
    assert "result=ALREADY_MIGRATED" in output


def test_migration_module_has_no_streaming_or_destructive_operations() -> None:
    source = Path(migration.__file__).read_text()

    for forbidden in (
        "DROP TABLE",
        "CREATE OR REPLACE",
        "writeStream",
        "checkpointLocation",
        "readStream",
        "kafka",
    ):
        assert forbidden not in source
