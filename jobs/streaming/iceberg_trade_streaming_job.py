"""Assemble the application flow from Kafka trades to an Iceberg table."""

from typing import Protocol

from pyspark.sql import SparkSession

from jobs.streaming.iceberg_catalog import configure_iceberg_rest_catalog
from jobs.streaming.iceberg_sink import start_bronze_trade_stream
from jobs.streaming.kafka_source import read_raw_trade_kafka_stream
from jobs.streaming.s3a_checkpoint import configure_s3a_checkpoint_storage
from jobs.streaming.trades import parse_raw_trade_kafka_messages


class SparkSessionBuilderLike(Protocol):
    def appName(self, app_name: str) -> "SparkSessionBuilderLike":
        ...

    def config(
        self,
        key: str,
        value: str,
    ) -> "SparkSessionBuilderLike":
        ...

    def getOrCreate(self) -> object:
        ...


def build_iceberg_trade_spark_session(
    *,
    app_name: str,
    catalog_name: str,
    catalog_uri: str,
    warehouse: str,
    s3_endpoint: str,
    s3_region: str,
    s3_access_key: str,
    s3_secret_key: str,
    s3_path_style_access: bool = True,
    s3a_ssl_enabled: bool = False,
    builder: SparkSessionBuilderLike | None = None,
) -> object:
    """Build one Spark session configured for Iceberg and S3A checkpoints."""
    session_builder = builder if builder is not None else SparkSession.builder
    session_builder.appName(app_name)
    session_builder.config("spark.sql.session.timeZone", "UTC")
    configure_iceberg_rest_catalog(
        session_builder,
        catalog_name=catalog_name,
        catalog_uri=catalog_uri,
        warehouse=warehouse,
        s3_endpoint=s3_endpoint,
        s3_region=s3_region,
        s3_access_key=s3_access_key,
        s3_secret_key=s3_secret_key,
        s3_path_style_access=s3_path_style_access,
    )
    configure_s3a_checkpoint_storage(
        session_builder,
        endpoint=s3_endpoint,
        region=s3_region,
        access_key=s3_access_key,
        secret_key=s3_secret_key,
        path_style_access=s3_path_style_access,
        ssl_enabled=s3a_ssl_enabled,
    )
    return session_builder.getOrCreate()


def verify_iceberg_table_exists(
    spark: object,
    *,
    table_name: str,
) -> None:
    """Fail fast unless the configured Iceberg table can be described."""
    spark.sql(f"DESCRIBE TABLE EXTENDED {table_name}")


def run_iceberg_trade_stream(
    *,
    bootstrap_servers: str,
    topic: str,
    app_name: str,
    catalog_name: str,
    catalog_uri: str,
    warehouse: str,
    table_name: str,
    s3_endpoint: str,
    s3_region: str,
    s3_access_key: str,
    s3_secret_key: str,
    checkpoint_location: str,
    query_name: str,
    processing_time: str | None = None,
    s3_path_style_access: bool = True,
    s3a_ssl_enabled: bool = False,
) -> None:
    """Run the Kafka-to-Iceberg trade stream until the query terminates."""
    spark = build_iceberg_trade_spark_session(
        app_name=app_name,
        catalog_name=catalog_name,
        catalog_uri=catalog_uri,
        warehouse=warehouse,
        s3_endpoint=s3_endpoint,
        s3_region=s3_region,
        s3_access_key=s3_access_key,
        s3_secret_key=s3_secret_key,
        s3_path_style_access=s3_path_style_access,
        s3a_ssl_enabled=s3a_ssl_enabled,
    )
    query = None

    try:
        verify_iceberg_table_exists(spark, table_name=table_name)
        raw_stream = read_raw_trade_kafka_stream(
            spark,
            bootstrap_servers=bootstrap_servers,
            topic=topic,
        )
        parsed_trades = parse_raw_trade_kafka_messages(raw_stream)
        query = start_bronze_trade_stream(
            parsed_trades,
            table_name=table_name,
            checkpoint_location=checkpoint_location,
            query_name=query_name,
            processing_time=processing_time,
        )
        query.awaitTermination()
    finally:
        try:
            if query is not None and query.isActive:
                query.stop()
        finally:
            spark.stop()
