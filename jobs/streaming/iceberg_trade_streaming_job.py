"""Assemble the application flow from Kafka trades to an Iceberg table."""

import argparse
import os
import signal
import threading
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from types import FrameType
from typing import Protocol

from pyspark.sql import SparkSession

from jobs.streaming.iceberg_catalog import configure_iceberg_rest_catalog
from jobs.streaming.iceberg_sink import start_bronze_trade_stream
from jobs.streaming.kafka_source import read_raw_trade_kafka_stream
from jobs.streaming.s3a_checkpoint import configure_s3a_checkpoint_storage
from jobs.streaming.trades import parse_raw_trade_kafka_messages


_SHUTDOWN_POLL_INTERVAL_SECONDS = 1.0


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


def _request_shutdown(
    shutdown_event: threading.Event,
    signum: int,
    frame: FrameType | None,
) -> None:
    del signum, frame
    shutdown_event.set()


def _await_query_until_shutdown(
    query: object,
    *,
    shutdown_event: threading.Event,
    poll_interval_seconds: float = _SHUTDOWN_POLL_INTERVAL_SECONDS,
) -> None:
    while query.isActive and not shutdown_event.is_set():
        if query.awaitTermination(poll_interval_seconds):
            return


def _add_exception_note(
    primary: BaseException,
    error: BaseException,
    label: str,
) -> None:
    note = f"{label}: {type(error).__name__}: {error}"
    nested_notes = getattr(error, "__notes__", [])
    if nested_notes:
        note = "\n".join([note, *nested_notes])
    primary.add_note(note)


@contextmanager
def _installed_shutdown_handlers(
    shutdown_event: threading.Event,
    *,
    signal_module=signal,
) -> Iterator[None]:
    previous_sigint = signal_module.getsignal(signal_module.SIGINT)
    previous_sigterm = signal_module.getsignal(signal_module.SIGTERM)

    def handler(signum: int, frame: FrameType | None) -> None:
        _request_shutdown(shutdown_event, signum, frame)

    signal_module.signal(signal_module.SIGINT, handler)
    try:
        signal_module.signal(signal_module.SIGTERM, handler)
    except BaseException as registration_error:
        try:
            signal_module.signal(signal_module.SIGINT, previous_sigint)
        except BaseException as restoration_error:
            _add_exception_note(
                registration_error,
                restoration_error,
                "SIGINT handler restoration failed",
            )
        raise

    body_error: BaseException | None = None
    try:
        yield
    except BaseException as error:
        body_error = error
        raise
    finally:
        restoration_error: BaseException | None = None
        try:
            signal_module.signal(signal_module.SIGINT, previous_sigint)
        except BaseException as error:
            restoration_error = error
        try:
            signal_module.signal(signal_module.SIGTERM, previous_sigterm)
        except BaseException as error:
            if restoration_error is None:
                restoration_error = error
            else:
                _add_exception_note(
                    restoration_error,
                    error,
                    "SIGTERM handler restoration failed",
                )
        if restoration_error is not None:
            if body_error is not None:
                _add_exception_note(
                    body_error,
                    restoration_error,
                    "signal handler restoration failed",
                )
            else:
                raise restoration_error


def _stop_query_and_spark(query: object | None, spark: object) -> None:
    query_stop_error: BaseException | None = None
    if query is not None and query.isActive:
        try:
            query.stop()
        except BaseException as error:
            query_stop_error = error

    try:
        spark.stop()
    except BaseException as spark_stop_error:
        if query_stop_error is not None:
            _add_exception_note(
                query_stop_error,
                spark_stop_error,
                "spark.stop failed",
            )
        else:
            raise

    if query_stop_error is not None:
        raise query_stop_error


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
    cleanup_completed = False

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
        shutdown_event = threading.Event()
        with _installed_shutdown_handlers(shutdown_event):
            primary_error: BaseException | None = None
            primary_traceback = None
            try:
                _await_query_until_shutdown(
                    query,
                    shutdown_event=shutdown_event,
                )
            except BaseException as error:
                primary_error = error
                primary_traceback = error.__traceback__

            try:
                _stop_query_and_spark(query, spark)
            except BaseException as cleanup_error:
                if primary_error is None:
                    primary_error = cleanup_error
                    primary_traceback = cleanup_error.__traceback__
                else:
                    _add_exception_note(
                        primary_error,
                        cleanup_error,
                        "streaming cleanup failed",
                    )
            cleanup_completed = True

            if primary_error is not None:
                raise primary_error.with_traceback(primary_traceback)
    except BaseException as primary_error:
        if not cleanup_completed:
            primary_traceback = primary_error.__traceback__
            try:
                _stop_query_and_spark(query, spark)
            except BaseException as cleanup_error:
                _add_exception_note(
                    primary_error,
                    cleanup_error,
                    "streaming cleanup failed",
                )
            raise primary_error.with_traceback(primary_traceback)
        raise


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
