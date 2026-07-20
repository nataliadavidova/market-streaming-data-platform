"""Unit tests for the Bronze Iceberg trade table contract."""

from jobs.streaming.iceberg_bronze import (
    bronze_trade_namespace_ddl,
    bronze_trade_table_ddl,
    bronze_trade_table_name,
    ensure_bronze_trade_table,
)


EXPECTED_BRONZE_TRADE_TABLE_DDL = """CREATE TABLE IF NOT EXISTS market_catalog.market.bronze_trades (
exchange STRING,
symbol STRING,
trade_id STRING,
price DECIMAL(38, 18),
quantity DECIMAL(38, 18),
event_time_ms BIGINT,
ingested_at_ms BIGINT,
kafka_key STRING,
kafka_topic STRING,
kafka_partition INT,
kafka_offset BIGINT,
kafka_timestamp TIMESTAMP,
raw_json STRING
)
USING iceberg"""


class RecordingSparkSqlExecutor:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def sql(self, query: str) -> object:
        self.queries.append(query)
        return object()


def test_bronze_trade_table_name_returns_full_name() -> None:
    assert (
        bronze_trade_table_name(
            catalog_name="market_catalog",
            namespace="market",
            table_name="bronze_trades",
        )
        == "market_catalog.market.bronze_trades"
    )


def test_bronze_trade_namespace_ddl_returns_exact_sql() -> None:
    assert (
        bronze_trade_namespace_ddl(
            catalog_name="market_catalog",
            namespace="market",
        )
        == "CREATE NAMESPACE IF NOT EXISTS market_catalog.market"
    )


def test_bronze_trade_table_ddl_returns_exact_unpartitioned_sql() -> None:
    ddl = bronze_trade_table_ddl(
        catalog_name="market_catalog",
        namespace="market",
        table_name="bronze_trades",
    )

    assert ddl == EXPECTED_BRONZE_TRADE_TABLE_DDL
    assert "price DECIMAL(38, 18)" in ddl
    assert "quantity DECIMAL(38, 18)" in ddl
    assert ddl.endswith("USING iceberg")
    assert "LOCATION" not in ddl
    assert "PARTITIONED BY" not in ddl
    assert "TBLPROPERTIES" not in ddl


def test_bronze_trade_ddl_interpolates_custom_names() -> None:
    namespace_ddl = bronze_trade_namespace_ddl(
        catalog_name="analytics_catalog",
        namespace="bronze",
    )
    table_ddl = bronze_trade_table_ddl(
        catalog_name="analytics_catalog",
        namespace="bronze",
        table_name="trades",
    )

    assert namespace_ddl == "CREATE NAMESPACE IF NOT EXISTS analytics_catalog.bronze"
    assert table_ddl.startswith(
        "CREATE TABLE IF NOT EXISTS analytics_catalog.bronze.trades"
    )


def test_ensure_bronze_trade_table_executes_namespace_then_table_ddl() -> None:
    spark = RecordingSparkSqlExecutor()

    result = ensure_bronze_trade_table(
        spark,
        catalog_name="market_catalog",
        namespace="market",
        table_name="bronze_trades",
    )

    assert result is None
    assert spark.queries == [
        "CREATE NAMESPACE IF NOT EXISTS market_catalog.market",
        EXPECTED_BRONZE_TRADE_TABLE_DDL,
    ]
