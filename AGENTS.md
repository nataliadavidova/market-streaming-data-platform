# AGENTS.md

## Project

This repository is `market-streaming-data-platform`.

It is a portfolio Data Engineering project focused on building a real-time market data platform.

## Working style

Work in very small steps.

Before changing files:
1. Inspect the current repository state.
2. Explain what will be changed.
3. Explain why the change is needed.
4. Make the smallest useful change.
5. Run the smallest relevant check.
6. Show the result and the diff.

Do not make large refactors unless explicitly requested.

Do not modify many unrelated files in one step.

## Branching

Default base branch:

`main`

Create a short-lived feature branch for each small task.
Do not work directly on `main` unless explicitly instructed.

## Versioned roadmap

### Version 1 — Market streaming MVP

Architecture:

`Market API/WebSocket → Kafka → Spark Structured Streaming → Iceberg on S3-compatible storage → ClickHouse → dashboard + basic DQ checks`

Main goals:
- Python producer reads market trades from an external API/WebSocket.
- Producer writes raw events to Kafka.
- Spark Structured Streaming reads Kafka.
- Spark parses, validates, and normalizes events.
- Data is written to Iceberg tables.
- Aggregates are written to ClickHouse.
- Basic data quality checks are added.
- A simple dashboard or analytical SQL layer is added.

### Version 2 — CDC + Greenplum MVP

Architecture:

`PostgreSQL operational source DB → Debezium CDC → Kafka → Greenplum DWH`

### Version 3 — dbt / marts / docs / basic lineage

Add dbt models, tests, docs, marts, and basic lineage.

### Version 4 — production-like reliability

Add Schema Registry, DLQ/quarantine topics, monitoring, consumer lag alerts, checkpointing, watermarking, idempotency, security/secrets, CI/CD, lineage/catalog/governance.

### Version 5 — ML / MLOps

Add feature tables, feature store, MLflow, model training, prediction table/API.

### Version 6 — cloud / infra

Add Terraform, cloud resources, deployment strategy, and optional Kubernetes.

## Current project state

The project is currently in the bootstrap phase.

Current Python package:

`jobs`

Current config file:

`config/market_symbols.yaml`

Current producer modules:

- `jobs/producer/config.py`
- `jobs/producer/events.py`
- `jobs/producer/binance.py`
- `jobs/producer/kafka.py`
- `jobs/producer/publisher.py`

Current implemented functions and models:

- `load_config(config_path)`: reads YAML config using PyYAML and returns a Python dictionary.
- `load_producer_config(config_path)`: reads and validates producer config using Pydantic models.
- `TradeEvent`: internal producer trade event contract. It validates `exchange`, `symbol`, `trade_id`, `price`, `quantity`, `event_time_ms`, and `ingested_at_ms`. `price` and `quantity` use `Decimal`.
- `TradeEvent.to_json_message()`: serializes a trade event to deterministic JSON for future Kafka publishing. `Decimal` values are preserved as JSON strings.
- `parse_binance_trade_message(raw_message, ingested_at_ms)`: parses a Binance trade message into `TradeEvent`. It maps `s` to `symbol`, `t` to `trade_id`, `p` to `price`, `q` to `quantity`, and `T` to `event_time_ms`.
- `KafkaMessage`: typed key/value message contract for future Kafka publishing.
- `prepare_trade_event_kafka_message(event)`: prepares deterministic Kafka key/value payloads from a `TradeEvent` without connecting to Kafka. Kafka key example: `binance:BTCUSDT`. The value is `TradeEvent.to_json_message()`.
- `KafkaProducerClient`: protocol for injectable Kafka-like clients used by the publisher wrapper.
- `KafkaPublisher`: wrapper that publishes prepared `KafkaMessage` objects to a configured topic by UTF-8 encoding the key and value. It uses an injectable client, so unit tests do not require a real Kafka broker.

## Python environment

Local Conda environment:

`market-streaming`

Python version target:

`>=3.11,<3.12`

The project is installed locally in editable mode with:

`python -m pip install -e .`

## Packaging

The project uses `pyproject.toml`.

Only `jobs*` should be discovered as Python packages.

Do not package these directories as Python modules:
- `config/`
- `docker/`
- `infra/`
- `sql/`
- `docs/`
- `tests/`

## Git rules

Before changing files, check:

`git status --short`

Do not commit unless explicitly asked.

Do not add ignored files.

`__pycache__/`, `.env`, `.idea/`, local data, checkpoints, and service volumes should stay ignored.

## Coding conventions

Python files should start with a short module-level docstring explaining what the file does.

## Immediate next likely step

Next likely small step:

- Add a concrete Kafka client adapter using a real Kafka Python library, still without implementing the full Binance WebSocket loop.

Current test suite:

- 31 unit tests cover raw config loading, valid producer config validation, invalid producer config validation, `TradeEvent` validation, `TradeEvent` JSON serialization, Binance trade parsing, Kafka message contract preparation, and Kafka publisher wrapper behavior.
- `make test` passes locally.
