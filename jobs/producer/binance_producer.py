"""Executable assembly for the Binance-to-Kafka producer."""

import asyncio
import argparse
import logging
import os
import signal
import sys
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from time import monotonic
from typing import Iterator

from jobs.producer.binance_publisher import run_binance_trade_publisher
from jobs.producer.config import load_producer_config
from jobs.producer.confluent import build_kafka_client
from jobs.producer.publisher import KafkaPublisher
from jobs.producer.websocket import WebSocketConnect

logger = logging.getLogger(__name__)


DEFAULT_CONFIG_PATH = "config/market_symbols.yaml"
KAFKA_BOOTSTRAP_SERVERS_ENV = "KAFKA_BOOTSTRAP_SERVERS"
DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
FINAL_KAFKA_FLUSH_TIMEOUT_SECONDS = 5.0


def _configure_runtime_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


class KafkaFinalizationError(RuntimeError):
    """Raised when Kafka messages remain queued after finalization."""


@contextmanager
def _installed_sigterm_handler(
    loop: asyncio.AbstractEventLoop,
    task: asyncio.Task,
    state: dict[str, bool | int],
    *,
    signal_module=signal,
) -> Iterator[None]:
    previous_handler = signal_module.getsignal(signal_module.SIGTERM)
    installed = False

    def request_shutdown() -> None:
        state["requested"] = True
        state["signum"] = signal_module.SIGTERM
        task.cancel()

    try:
        loop.add_signal_handler(signal_module.SIGTERM, request_shutdown)
        installed = True
        yield
    finally:
        if installed:
            loop.remove_signal_handler(signal_module.SIGTERM)
        signal_module.signal(signal_module.SIGTERM, previous_handler)


def _flush_kafka(client: object) -> None:
    logger.info(
        "FINAL_KAFKA_FLUSH_STARTED timeout=%.1f",
        FINAL_KAFKA_FLUSH_TIMEOUT_SECONDS,
    )
    flush_started_at = monotonic()
    try:
        remaining_messages = client.flush(FINAL_KAFKA_FLUSH_TIMEOUT_SECONDS)
    except BaseException:
        logger.exception("FINAL_KAFKA_FLUSH_FAILED")
        raise
    finally:
        logger.info(
            "Final Kafka flush duration %.3f seconds with timeout %.3f seconds",
            monotonic() - flush_started_at,
            FINAL_KAFKA_FLUSH_TIMEOUT_SECONDS,
        )

    logger.info("FINAL_KAFKA_FLUSH_RESULT remaining=%d", remaining_messages)
    if remaining_messages:
        logger.error("FINAL_KAFKA_FLUSH_FAILED")
        raise KafkaFinalizationError(
            "Kafka finalization timed out with "
            f"{remaining_messages} message(s) still queued"
        )

    logger.info("FINAL_KAFKA_FLUSH_SUCCEEDED")


async def run_configured_binance_producer(
    config_path: str,
    bootstrap_servers: str,
    topic_override: str | None = None,
    *,
    connect: WebSocketConnect | None = None,
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

    state: dict[str, bool | int] = {"requested": False}
    try:
        try:
            publisher = KafkaPublisher(topic=config.kafka.raw_topic, client=client)
            loop = asyncio.get_running_loop()
            task = asyncio.current_task()
            if task is None:  # pragma: no cover - always present under asyncio.run
                raise RuntimeError(
                    "producer runtime requires a running asyncio task"
                )

            with _installed_sigterm_handler(loop, task, state):
                try:
                    await run_binance_trade_publisher(
                        config,
                        publisher,
                        connect=connect,
                    )
                except asyncio.CancelledError:
                    if not state["requested"]:
                        raise
                    logger.info("PRODUCER_SHUTDOWN_REQUESTED signal=SIGTERM")
        except BaseException as runtime_error:
            try:
                _flush_kafka(client)
            except BaseException as flush_error:
                runtime_error.add_note(f"final Kafka flush failed: {flush_error}")
            raise
        else:
            _flush_kafka(client)
            signal_name = "SIGTERM" if state["requested"] else "NORMAL"
            logger.info("PRODUCER_SHUTDOWN_COMPLETED signal=%s", signal_name)
    except asyncio.CancelledError:
        raise


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
        logger.info("PRODUCER_SHUTDOWN_COMPLETED signal=SIGINT")
        return


if __name__ == "__main__":
    _configure_runtime_logging()
    main(sys.argv[1:])
