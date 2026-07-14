# market-streaming-data-platform

A portfolio Data Engineering project for a real-time market data platform.

The Version 1 target flow is:

`Market API/WebSocket -> Kafka -> Spark Structured Streaming -> Iceberg on S3-compatible storage -> ClickHouse -> dashboard + basic DQ checks`

The repository is still in the bootstrap phase. The current work is focused on the Python market-data producer foundation and local Kafka smoke checks.

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

Spark Structured Streaming, Iceberg, ClickHouse, dashboard, and the continuous Binance producer loop are not implemented yet.

## Current Implementation

Implemented foundation:

- Typed producer config loading from `config/market_symbols.yaml`
- `TradeEvent` modeling and deterministic JSON serialization
- Binance combined trade-stream URL construction from `ProducerConfig`
- One-shot Binance WebSocket receive-and-parse path that returns one `TradeEvent`
- Receive-boundary ingestion timestamp capture immediately after WebSocket `recv()`
- Kafka message preparation and injectable publisher wrapper
- Local Kafka service configuration
- One-event synthetic Kafka producer smoke-check for `market.trades.raw`

Not implemented yet:

- Long-lived Binance WebSocket receive loop
- Live Binance-to-Kafka publication
- Retry, reconnect, graceful shutdown, logging, and metrics
- Spark, Iceberg, ClickHouse, dashboard, and data-quality jobs

## Documentation

- [Architecture](docs/architecture.md)
- [Roadmap](docs/roadmap.md)

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

Current test status: 63 unit tests pass locally with `make test`.

## Manual Smoke Checks

The local Kafka producer slice currently supports a bounded smoke-check with a synthetic trade event:

```bash
make kafka-up
make kafka-create-topic
make kafka-smoke-publish-one
make kafka-consume-one
make kafka-down
```

Successful consume output should include a JSON message with `"trade_id":"smoke-test-1"` and:

```text
Processed a total of 1 messages
```

This local smoke-check uses a synthetic event. Continuous live Binance-to-Kafka streaming is not implemented yet.
