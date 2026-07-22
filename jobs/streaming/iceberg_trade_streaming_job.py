"""Assemble the application flow from Kafka trades to an Iceberg table."""

import argparse
import os
from collections.abc import Mapping, Sequence
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


def _parse_boolean(value: str) -> bool:
    normalized_value = value.strip().lower()
    if normalized_value in {"true", "1", "yes", "on"}:
        return True
    if normalized_value in {"false", "0", "no", "off"}:
        return False
    raise argparse.ArgumentTypeError(f"invalid boolean value: {value!r}")


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


def parse_args(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
) -> argparse.Namespace:
    """Parse host-executable arguments with environment-backed defaults."""
    environment = os.environ if environ is None else environ
    processing_time_value = environment.get("ICEBERG_TRADE_PROCESSING_TIME")
    processing_time = (
        processing_time_value
        if processing_time_value is not None and processing_time_value.strip()
        else None
    )

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bootstrap-servers",
        default=environment.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument(
        "--topic",
        default=environment.get("KAFKA_TOPIC_TRADES_RAW", "market.trades.raw"),
    )
    parser.add_argument(
        "--app-name",
        default=environment.get(
            "ICEBERG_TRADE_APP_NAME",
            "market-iceberg-trade-streaming",
        ),
    )
    parser.add_argument(
        "--catalog-name",
        default=environment.get("ICEBERG_CATALOG_NAME", "market_catalog"),
    )
    parser.add_argument(
        "--catalog-uri",
        default=environment.get(
            "ICEBERG_REST_HOST_URI",
            "http://localhost:8181",
        ),
    )
    parser.add_argument(
        "--warehouse",
        default=environment.get(
            "ICEBERG_WAREHOUSE",
            "s3://market-lake/warehouse",
        ),
    )
    parser.add_argument(
        "--table-name",
        default=environment.get(
            "ICEBERG_BRONZE_TABLE",
            "market_catalog.market.bronze_trades",
        ),
    )
    parser.add_argument(
        "--s3-endpoint",
        default=environment.get("S3_HOST_ENDPOINT", "http://localhost:9000"),
    )
    parser.add_argument(
        "--s3-region",
        default=environment.get("S3_REGION", "us-east-1"),
    )
    parser.add_argument(
        "--s3-access-key",
        default=environment.get("S3_ACCESS_KEY", "minioadmin"),
    )
    parser.add_argument(
        "--s3-secret-key",
        default=environment.get("S3_SECRET_KEY", "minioadmin"),
    )
    parser.add_argument(
        "--checkpoint-location",
        default=environment.get(
            "ICEBERG_TRADE_CHECKPOINT_LOCATION",
            "s3a://market-lake/checkpoints/market/bronze-trades",
        ),
    )
    parser.add_argument(
        "--query-name",
        default=environment.get(
            "ICEBERG_TRADE_QUERY_NAME",
            "market-iceberg-bronze-trades",
        ),
    )
    parser.add_argument(
        "--processing-time",
        default=processing_time,
    )
    parser.add_argument(
        "--s3-path-style-access",
        action=argparse.BooleanOptionalAction,
        default=_parse_boolean(environment.get("S3_PATH_STYLE_ACCESS", "true")),
    )
    parser.add_argument(
        "--s3a-ssl-enabled",
        action=argparse.BooleanOptionalAction,
        default=_parse_boolean(environment.get("S3A_SSL_ENABLED", "false")),
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    """Run the configured Iceberg trade streaming application."""
    args = parse_args(argv)
    run_iceberg_trade_stream(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        app_name=args.app_name,
        catalog_name=args.catalog_name,
        catalog_uri=args.catalog_uri,
        warehouse=args.warehouse,
        table_name=args.table_name,
        s3_endpoint=args.s3_endpoint,
        s3_region=args.s3_region,
        s3_access_key=args.s3_access_key,
        s3_secret_key=args.s3_secret_key,
        checkpoint_location=args.checkpoint_location,
        query_name=args.query_name,
        processing_time=args.processing_time,
        s3_path_style_access=args.s3_path_style_access,
        s3a_ssl_enabled=args.s3a_ssl_enabled,
    )


if __name__ == "__main__":
    main()
