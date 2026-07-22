"""Executable assembly for the Binance-to-Kafka producer."""

import asyncio
import argparse
import logging
import os
import sys
from collections.abc import Mapping, Sequence
from time import monotonic

from jobs.producer.binance_publisher import run_binance_trade_publisher
from jobs.producer.config import load_producer_config
from jobs.producer.confluent import build_kafka_client
from jobs.producer.publisher import KafkaPublisher

logger = logging.getLogger(__name__)


DEFAULT_CONFIG_PATH = "config/market_symbols.yaml"
KAFKA_BOOTSTRAP_SERVERS_ENV = "KAFKA_BOOTSTRAP_SERVERS"
DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
FINAL_KAFKA_FLUSH_TIMEOUT_SECONDS = 5.0


class KafkaFinalizationError(RuntimeError):
    """Raised when Kafka messages remain queued after finalization."""


async def run_configured_binance_producer(
    config_path: str,
    bootstrap_servers: str,
    topic_override: str | None = None,
) -> None:
    config = load_producer_config(config_path)
    if topic_override:
        config = config.model_copy(
            update={
                "kafka": config.kafka.model_copy(
                    update={"raw_topic": topic_override}
                )
            }
        )
    client = build_kafka_client(bootstrap_servers)

    try:
        publisher = KafkaPublisher(
            topic=config.kafka.raw_topic,
            client=client,
        )

        await run_binance_trade_publisher(config, publisher)
    finally:
        flush_started_at = monotonic()
        try:
            remaining_messages = client.flush(FINAL_KAFKA_FLUSH_TIMEOUT_SECONDS)
        finally:
            logger.info(
                "Final Kafka flush duration %.3f seconds with timeout %.3f seconds",
                monotonic() - flush_started_at,
                FINAL_KAFKA_FLUSH_TIMEOUT_SECONDS,
            )
        if remaining_messages:
            raise KafkaFinalizationError(
                "Kafka finalization timed out with "
                f"{remaining_messages} message(s) still queued"
            )


def parse_args(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
) -> argparse.Namespace:
    environment = os.environ if environ is None else environ
    environment_topic = environment.get("KAFKA_TOPIC_TRADES_RAW")
    if environment_topic is not None:
        environment_topic = environment_topic.strip() or None

    parser = argparse.ArgumentParser(description="Run the Binance Kafka producer")
    parser.add_argument("--topic", default=environment_topic)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args([] if argv is None else argv)
    bootstrap_servers = os.environ.get(
        KAFKA_BOOTSTRAP_SERVERS_ENV,
        DEFAULT_KAFKA_BOOTSTRAP_SERVERS,
    )
    try:
        run_kwargs = {
            "config_path": DEFAULT_CONFIG_PATH,
            "bootstrap_servers": bootstrap_servers,
        }
        if args.topic is not None:
            run_kwargs["topic_override"] = args.topic
        asyncio.run(
            run_configured_binance_producer(
                **run_kwargs,
            )
        )
    except KeyboardInterrupt:
        return


if __name__ == "__main__":
    main(sys.argv[1:])
