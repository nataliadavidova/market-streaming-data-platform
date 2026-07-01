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

## Current branch

Work on:

`feature/project-bootstrap`

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

Current producer module:

`jobs/producer/config.py`

Current implemented function:

`load_config(config_path)`

It reads YAML config using PyYAML and returns a Python dictionary.

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

## Immediate next likely step

Add a minimal unit test for:

`jobs.producer.config.load_config`

The test should verify that:
- YAML config can be loaded.
- `exchange` equals `binance`.
- symbols include `BTCUSDT`, `ETHUSDT`, and `SOLUSDT`.
- raw Kafka topic equals `market.trades.raw`.

Keep the test small.
