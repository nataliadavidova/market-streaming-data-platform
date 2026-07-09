# market-streaming-data-platform

A portfolio Data Engineering project for a real-time market data platform.

## Version 1 MVP Architecture

Planned Version 1 flow:

`Market API/WebSocket -> Kafka -> Spark Structured Streaming -> Iceberg on S3-compatible storage -> ClickHouse -> dashboard + basic DQ checks`

This repository is currently in the bootstrap phase. Spark, Iceberg, ClickHouse, and dashboard components are not implemented yet.

## Current Implementation

Current implemented pieces:

- Typed producer config loading from `config/market_symbols.yaml`
- `TradeEvent` modeling, Binance trade parsing, and Kafka message preparation
- Local Kafka service configuration
- One-event synthetic producer smoke-check for `market.trades.raw`

Live Binance WebSocket streaming is not implemented yet.

## Local Kafka Producer Smoke Check

The local Kafka producer slice currently supports a bounded smoke-check with a synthetic trade event:

- Local Kafka service through Docker Compose
- `market.trades.raw` Kafka topic
- One-event producer smoke publisher
- Bounded consume-one check

Run the local smoke-check:

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

This is a local smoke-check using a synthetic event. Live Binance WebSocket streaming is not implemented yet.

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
python -m pytest
```

Current test status: 1 unit test for `load_config`.
