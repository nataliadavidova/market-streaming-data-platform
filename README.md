# market-streaming-data-platform

A portfolio Data Engineering project for a real-time market data platform.

## Version 1 MVP Architecture

Planned Version 1 flow:

`Market API/WebSocket -> Kafka -> Spark Structured Streaming -> Iceberg on S3-compatible storage -> ClickHouse -> dashboard + basic DQ checks`

This repository is currently in the bootstrap phase. Kafka, Spark, Iceberg, ClickHouse, and dashboard components are not implemented yet.

## Current Implementation

The current implemented piece is a Python config loader:

- Module: `jobs.producer.config`
- Function: `load_config(config_path)`
- Config file: `config/market_symbols.yaml`

The config file currently defines Binance trade symbols and the raw Kafka topic name for future producer work.

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
