"""Executable Spark job for streaming raw Kafka trades to a temporary console sink."""

import argparse
from collections.abc import Sequence

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

from jobs.streaming.kafka_source import read_raw_trade_kafka_stream
from jobs.streaming.trades import parse_raw_trade_kafka_messages

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "market.trades.raw"
DEFAULT_APP_NAME = "market-trade-streaming"


def build_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def start_console_trade_query(
    spark: SparkSession,
    *,
    bootstrap_servers: str,
    topic: str,
) -> StreamingQuery:
    raw_stream = read_raw_trade_kafka_stream(
        spark,
        bootstrap_servers=bootstrap_servers,
        topic=topic,
    )
    parsed_trades = parse_raw_trade_kafka_messages(raw_stream)

    return (
        parsed_trades.writeStream.format("console")
        .outputMode("append")
        .option("truncate", "false")
        .start()
    )


def run_trade_stream(
    *,
    bootstrap_servers: str,
    topic: str,
    app_name: str,
) -> None:
    spark = build_spark_session(app_name)
    query: StreamingQuery | None = None

    try:
        query = start_console_trade_query(
            spark,
            bootstrap_servers=bootstrap_servers,
            topic=topic,
        )
        query.awaitTermination()
    finally:
        try:
            if query is not None and query.isActive:
                query.stop()
        finally:
            spark.stop()


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP_SERVERS,
    )
    parser.add_argument(
        "--topic",
        default=DEFAULT_TOPIC,
    )
    parser.add_argument(
        "--app-name",
        default=DEFAULT_APP_NAME,
    )
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    run_trade_stream(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        app_name=args.app_name,
    )


if __name__ == "__main__":
    main()
