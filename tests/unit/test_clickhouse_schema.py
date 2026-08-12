"""Unit tests for the ClickHouse serving-schema control plane."""

from dataclasses import dataclass
import re

import pytest

from jobs.serving import clickhouse_schema as schema


class FakeResult:
    def __init__(self, rows: list[object]) -> None:
        self.result_rows = rows


@dataclass
class RecordingClient:
    database_engine: str = "Atomic"
    table_engine: str = "MergeTree"
    partition_key: str = "toYYYYMM(event_time)"
    sorting_key: str = "exchange, symbol, event_time, trade_id, kafka_topic, kafka_partition, kafka_offset"
    target_columns: list[tuple[str, str]] | None = None
    staging_columns: list[tuple[str, str]] | None = None
    missing_table: str | None = None

    def __post_init__(self) -> None:
        self.commands: list[str] = []
        self.queries: list[str] = []
        if self.target_columns is None:
            self.target_columns = list(schema.TABLE_COLUMNS)
        if self.staging_columns is None:
            self.staging_columns = list(schema.TABLE_COLUMNS)

    def command(self, query: str) -> None:
        self.commands.append(query)

    def query(self, query: str) -> FakeResult:
        if "count() AS row_count" in query:
            return FakeResult([[184]])
        self.queries.append(query)
        if "system.databases" in query:
            return FakeResult([[schema.DEFAULT_DATABASE, self.database_engine]])
        if "system.tables" in query:
            table = schema.TARGET_TABLE if "silver_trades'" in query else schema.STAGING_TABLE
            if table == self.missing_table:
                return FakeResult([])
            return FakeResult([[table, self.table_engine, self.partition_key, self.sorting_key]])
        if "system.columns" in query:
            columns = self.target_columns if "silver_trades'" in query else self.staging_columns
            return FakeResult(columns or [])
        raise AssertionError(f"unexpected query: {query}")


def _environment(**overrides: str) -> dict[str, str]:
    values = {
        "CLICKHOUSE_USER": "market_loader",
        "CLICKHOUSE_PASSWORD": "secret-value",
    }
    values.update(overrides)
    return values


def _contains_destructive_sql(query: str) -> bool:
    return bool(
        re.search(
            r"\b(?:DROP|TRUNCATE|ALTER|RENAME)\s|\bEXCHANGE\s+TABLE\b|\bDELETE\s+",
            query,
            flags=re.IGNORECASE,
        )
    )


def test_config_uses_approved_defaults_and_hides_password() -> None:
    config = schema.ClickHouseConfig.from_environment(_environment())

    assert config.host == "localhost"
    assert config.http_port == 18123
    assert config.database == "market_analytics"
    assert "secret-value" not in repr(config)


@pytest.mark.parametrize("port", ["0", "65536", "not-a-port"])
def test_config_rejects_invalid_http_port(port: str) -> None:
    with pytest.raises(schema.ClickHouseControlPlaneError, match="HTTP_PORT"):
        schema.ClickHouseConfig.from_environment(_environment(CLICKHOUSE_HTTP_PORT=port))


def test_config_rejects_unsafe_database_and_missing_credentials() -> None:
    with pytest.raises(schema.ClickHouseControlPlaneError, match="safe"):
        schema.ClickHouseConfig.from_environment(
            _environment(CLICKHOUSE_DATABASE="market_analytics; DROP DATABASE x")
        )
    with pytest.raises(schema.ClickHouseControlPlaneError, match="USER"):
        schema.ClickHouseConfig.from_environment(_environment(CLICKHOUSE_USER=""))
    with pytest.raises(schema.ClickHouseControlPlaneError, match="PASSWORD") as error:
        schema.ClickHouseConfig.from_environment(_environment(CLICKHOUSE_PASSWORD=""))
    assert "secret-value" not in str(error.value)


def test_client_uses_http_settings_and_bootstrap_database_without_logging_password() -> None:
    calls: list[dict[str, object]] = []

    def factory(**kwargs: object) -> RecordingClient:
        calls.append(kwargs)
        return RecordingClient()

    config = schema.ClickHouseConfig.from_environment(
        _environment(CLICKHOUSE_HTTP_PORT="19001")
    )
    schema.connect_client(config, client_factory=factory)

    assert calls == [
        {
            "host": "localhost",
            "port": 19001,
            "username": "market_loader",
            "password": "secret-value",
            "database": "default",
        }
    ]


def test_ddl_contains_exact_database_and_identical_table_contracts() -> None:
    database_query = schema.build_create_database_query()
    target_query = schema.build_create_table_query(schema.TARGET_TABLE)
    staging_query = schema.build_create_table_query(schema.STAGING_TABLE)

    assert database_query == (
        "CREATE DATABASE IF NOT EXISTS market_analytics ENGINE = Atomic"
    )
    assert target_query.replace("silver_trades", "TABLE_NAME") == staging_query.replace(
        "silver_trades_staging", "TABLE_NAME"
    )
    for column, data_type in schema.TABLE_COLUMNS:
        assert f"{column} {data_type}" in target_query
    assert "ENGINE = MergeTree" in target_query
    assert "PARTITION BY toYYYYMM(event_time)" in target_query
    assert "ORDER BY (" in target_query
    assert not _contains_destructive_sql(target_query)
    assert "ReplacingMergeTree" not in target_query
    assert "Nullable" not in target_query


def test_staging_truncate_query_is_exact_and_target_safe() -> None:
    assert schema.build_truncate_staging_query() == (
        "TRUNCATE TABLE market_analytics.silver_trades_staging"
    )
    assert "silver_trades " not in schema.build_truncate_staging_query()
    assert "DROP" not in schema.build_truncate_staging_query()
    assert "ALTER" not in schema.build_truncate_staging_query()
    assert "DELETE" not in schema.build_truncate_staging_query()
    assert "EXCHANGE" not in schema.build_truncate_staging_query()


def test_truncate_staging_and_count_use_only_staging() -> None:
    client = RecordingClient()
    config = schema.ClickHouseConfig.from_environment(_environment())

    schema.truncate_staging(client, config)
    assert client.commands == [
        "TRUNCATE TABLE market_analytics.silver_trades_staging"
    ]
    assert schema.staging_row_count(client, config) == 184
    assert all("silver_trades" not in query or "silver_trades_staging" in query for query in client.commands)


def test_ensure_creates_in_order_and_validates_metadata_twice() -> None:
    client = RecordingClient()
    config = schema.ClickHouseConfig.from_environment(_environment())

    first = schema.ensure_schema(client, config)
    second = schema.ensure_schema(client, config)

    assert first == second
    assert [query.split()[1:5] for query in client.commands[:3]] == [
        ["DATABASE", "IF", "NOT", "EXISTS"],
        ["TABLE", "IF", "NOT", "EXISTS"],
        ["TABLE", "IF", "NOT", "EXISTS"],
    ]
    assert len(client.commands) == 6
    assert all(not _contains_destructive_sql(query) for query in client.commands)
    assert len(client.queries) == 10


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("database_engine", "AtomicWrites", "database engine mismatch"),
        ("table_engine", "ReplacingMergeTree", "table engine mismatch"),
        ("partition_key", "toYYYYMM(symbol)", "partition key mismatch"),
        ("sorting_key", "symbol,exchange", "sorting key mismatch"),
    ],
)
def test_ensure_rejects_wrong_metadata_without_destructive_sql(
    field: str,
    value: str,
    message: str,
) -> None:
    client = RecordingClient(**{field: value})

    with pytest.raises(schema.ClickHouseControlPlaneError, match=message):
        schema.ensure_schema(
            client,
            schema.ClickHouseConfig.from_environment(_environment()),
        )
    assert all(not _contains_destructive_sql(query) for query in client.commands)


@pytest.mark.parametrize(
    "columns",
    [
        list(schema.TABLE_COLUMNS[:-1]),
        [("symbol", "String"), *list(schema.TABLE_COLUMNS[1:])],
        [(name, "UInt64" if name == "price" else data_type) for name, data_type in schema.TABLE_COLUMNS],
        [(name, "Nullable(Decimal(38,18))" if name == "price" else data_type) for name, data_type in schema.TABLE_COLUMNS],
    ],
)
def test_ensure_rejects_column_contract_mismatch(columns: list[tuple[str, str]]) -> None:
    client = RecordingClient(target_columns=columns)

    with pytest.raises(schema.ClickHouseControlPlaneError, match="columns mismatch"):
        schema.ensure_schema(
            client,
            schema.ClickHouseConfig.from_environment(_environment()),
        )


def test_ensure_rejects_target_staging_mismatch() -> None:
    client = RecordingClient(staging_columns=list(schema.TABLE_COLUMNS[:-1]))

    with pytest.raises(schema.ClickHouseControlPlaneError, match="columns mismatch"):
        schema.ensure_schema(
            client,
            schema.ClickHouseConfig.from_environment(_environment()),
        )


@pytest.mark.parametrize("missing_table", [schema.TARGET_TABLE, schema.STAGING_TABLE])
def test_ensure_rejects_missing_table_metadata(missing_table: str) -> None:
    client = RecordingClient(missing_table=missing_table)

    with pytest.raises(schema.ClickHouseControlPlaneError, match="metadata missing"):
        schema.ensure_schema(
            client,
            schema.ClickHouseConfig.from_environment(_environment()),
        )


def test_cli_success_is_bounded_and_does_not_print_password(capsys: pytest.CaptureFixture[str]) -> None:
    client = RecordingClient()
    result = schema.main(
        ["ensure"],
        environ=_environment(),
        client_factory=lambda **_: client,
    )

    output = capsys.readouterr()
    assert result == 0
    assert "database=market_analytics" in output.out
    assert "column_count=12" in output.out
    assert "secret-value" not in output.out + output.err


@pytest.mark.parametrize(
    "failure",
    [
        schema.ClickHouseControlPlaneError("bad configuration"),
        schema.ClickHouseControlPlaneError("schema mismatch"),
    ],
)
def test_cli_failure_is_nonzero_and_bounded(
    failure: Exception,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def failing_factory(**_: object) -> RecordingClient:
        raise failure

    result = schema.main(
        ["ensure"],
        environ=_environment(),
        client_factory=failing_factory,
    )

    output = capsys.readouterr()
    assert result == 1
    assert output.out == ""
    assert "secret-value" not in output.err
