# market-streaming-data-platform

A portfolio Data Engineering project for a real-time market data platform.

The Version 1 target flow is:

`Market API/WebSocket -> Kafka -> Spark Structured Streaming -> Iceberg on S3-compatible storage -> ClickHouse -> dashboard + basic DQ checks`

The repository is still in the bootstrap phase. The current work is focused on the Python market-data producer, local Kafka workflow, and the first executable Binance-to-Kafka path.

## Technology Stack

Current and planned technologies:

- Python 3.11
- Binance WebSocket
- Kafka
- Spark Structured Streaming
- Iceberg on S3-compatible storage
- ClickHouse
- Docker Compose for local services
- GitHub Actions for unit-test CI

Spark Structured Streaming, Iceberg, ClickHouse, dashboard, and production reliability behavior are not implemented yet.

## Current Implementation

Implemented foundation:

- Typed producer config loading from `config/market_symbols.yaml`
- `TradeEvent` modeling and deterministic JSON serialization
- Binance combined trade-stream URL construction from `ProducerConfig`
- One-shot Binance WebSocket receive-and-parse path that returns one `TradeEvent`
- Receive-boundary ingestion timestamp capture immediately after WebSocket `recv()`
- Reusable Binance receiver session and sequential publish loop
- Kafka message preparation and injectable publisher wrapper
- Executable Binance-to-Kafka producer: `python -m jobs.producer.binance_producer`
- Application-owned final Kafka flush and clean top-level `SIGINT` handling for the executable producer
- Bounded final Kafka shutdown flush with explicit failure when messages remain queued
- Local Kafka service configuration
- One-event synthetic Kafka producer smoke-check for `market.trades.raw`
- Bounded manual end-to-end smoke-check from real Binance WebSocket to Kafka consumer

Not implemented yet:

- WebSocket shutdown timeout tuning and stage-level shutdown instrumentation
- SIGTERM handling, second-interrupt escalation, and broader shutdown orchestration
- Retry, reconnect, delivery callbacks, logging, and metrics
- Producer container execution and throughput tuning
- Spark, Iceberg, ClickHouse, dashboard, and data-quality jobs

## Documentation

- [Architecture](docs/architecture.md)
- [Roadmap](docs/roadmap.md)
- [Kafka smoke-check runbook](docs/runbooks/kafka-smoke-check.md)
- [Binance one-shot smoke-check runbook](docs/runbooks/binance-one-shot-smoke-check.md)
- [Binance-to-Kafka end-to-end smoke-check runbook](docs/runbooks/binance-kafka-e2e-smoke-check.md)

## Local Development

Conda environment:

```bash
conda activate market-streaming
```

Install locally:

```bash
python -m pip install -e ".[dev]"
```

Run tests:

```bash
make test
```

Current test status: 92 unit tests pass locally with `make test`.

## Manual Smoke Checks

Manual smoke checks are kept out of CI because they depend on local services or external network availability.

- [Kafka smoke-check](docs/runbooks/kafka-smoke-check.md): starts local Kafka, publishes one synthetic `TradeEvent`, consumes one message from `market.trades.raw`, and shuts Kafka down.
- [Binance one-shot smoke-check](docs/runbooks/binance-one-shot-smoke-check.md): connects to Binance, receives and parses one live trade event, then closes the one-shot connection.
- [Binance-to-Kafka end-to-end smoke-check](docs/runbooks/binance-kafka-e2e-smoke-check.md): runs `python -m jobs.producer.binance_producer`, receives a fresh live Binance trade, publishes it to Kafka, and consumes it with a fresh latest-offset consumer group.

The Kafka check uses a synthetic event. The Binance one-shot check receives one live event without Kafka. The end-to-end check verifies the executable live Binance-to-Kafka data path. All checks are manual and excluded from CI.

The executable producer reads `KAFKA_BOOTSTRAP_SERVERS`; for host-local Kafka use:

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 python -m jobs.producer.binance_producer
```

The producer is a permanent process. Clean operator `SIGINT` handling and bounded application-owned final Kafka flush are implemented. Retry/reconnect, delivery acknowledgement handling, WebSocket shutdown tuning, SIGTERM handling, and throughput optimization are not implemented yet.
