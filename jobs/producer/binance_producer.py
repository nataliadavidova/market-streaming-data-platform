"""Executable assembly for the Binance-to-Kafka producer."""

import asyncio
import os

from jobs.producer.binance_publisher import run_binance_trade_publisher
from jobs.producer.config import load_producer_config
from jobs.producer.confluent import build_kafka_client
from jobs.producer.publisher import KafkaPublisher


DEFAULT_CONFIG_PATH = "config/market_symbols.yaml"
KAFKA_BOOTSTRAP_SERVERS_ENV = "KAFKA_BOOTSTRAP_SERVERS"
DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"


async def run_configured_binance_producer(
    config_path: str,
    bootstrap_servers: str,
) -> None:
    config = load_producer_config(config_path)
    client = build_kafka_client(bootstrap_servers)

    try:
        publisher = KafkaPublisher(
            topic=config.kafka.raw_topic,
            client=client,
        )

        await run_binance_trade_publisher(config, publisher)
    finally:
        client.flush()


def main() -> None:
    bootstrap_servers = os.environ.get(
        KAFKA_BOOTSTRAP_SERVERS_ENV,
        DEFAULT_KAFKA_BOOTSTRAP_SERVERS,
    )
    try:
        asyncio.run(
            run_configured_binance_producer(
                DEFAULT_CONFIG_PATH,
                bootstrap_servers,
            )
        )
    except KeyboardInterrupt:
        return


if __name__ == "__main__":
    main()
