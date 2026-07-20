"""Define the Bronze Iceberg table contract for parsed market trades."""

from typing import Protocol


class SparkSqlExecutor(Protocol):
    def sql(self, query: str) -> object:
        ...


BRONZE_TRADE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("exchange", "STRING"),
    ("symbol", "STRING"),
    ("trade_id", "STRING"),
    ("price", "DECIMAL(38, 18)"),
    ("quantity", "DECIMAL(38, 18)"),
    ("event_time_ms", "BIGINT"),
    ("ingested_at_ms", "BIGINT"),
    ("kafka_key", "STRING"),
    ("kafka_topic", "STRING"),
    ("kafka_partition", "INT"),
    ("kafka_offset", "BIGINT"),
    ("kafka_timestamp", "TIMESTAMP"),
    ("raw_json", "STRING"),
)


def bronze_trade_table_name(
    *,
    catalog_name: str,
    namespace: str,
    table_name: str,
) -> str:
    return f"{catalog_name}.{namespace}.{table_name}"


def bronze_trade_namespace_ddl(
    *,
    catalog_name: str,
    namespace: str,
) -> str:
    return f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.{namespace}"


def bronze_trade_table_ddl(
    *,
    catalog_name: str,
    namespace: str,
    table_name: str,
) -> str:
    qualified_table_name = bronze_trade_table_name(
        catalog_name=catalog_name,
        namespace=namespace,
        table_name=table_name,
    )
    column_definitions = ",\n".join(
        f"{column_name} {column_type}"
        for column_name, column_type in BRONZE_TRADE_COLUMNS
    )

    return (
        f"CREATE TABLE IF NOT EXISTS {qualified_table_name} (\n"
        f"{column_definitions}\n"
        ")\n"
        "USING iceberg"
    )


def ensure_bronze_trade_table(
    spark: SparkSqlExecutor,
    *,
    catalog_name: str,
    namespace: str,
    table_name: str,
) -> None:
    spark.sql(
        bronze_trade_namespace_ddl(
            catalog_name=catalog_name,
            namespace=namespace,
        )
    )
    spark.sql(
        bronze_trade_table_ddl(
            catalog_name=catalog_name,
            namespace=namespace,
            table_name=table_name,
        )
    )
