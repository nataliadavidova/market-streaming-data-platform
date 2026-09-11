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
- Silver data is published to a ClickHouse serving copy.
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

## Current durable project boundaries

- The current local path is `Binance WebSocket -> Kafka -> Spark Structured Streaming -> Bronze Iceberg -> Silver Iceberg -> ClickHouse -> bi_reader -> Metabase`.
- Kafka separates the Binance producer from Spark processing. Iceberg metadata uses the REST catalog and S3FileIO; MinIO stores local Iceberg and Hadoop S3A checkpoint objects.
- Bronze is the continuous quality-classified streaming layer with the exact canonical 15-column contract. Use the versioned quality-v2 checkpoint for the canonical live job and dedicated resources for destructive smoke tests.
- Silver reads canonical `market_catalog.market.bronze_trades`, keeps `is_valid = true` rows only, and uses bounded full replacement. It does not mutate, deduplicate, or infer a historical source epoch.
- Silver is the authoritative source of truth. ClickHouse is a reproducible bounded serving copy. The serving workflow validates staging before one atomic exchange; see `docs/silver-clickhouse-dashboard-mvp.md` and `docs/runbooks/clickhouse-serving-refresh.md`.
- `(topic, partition, offset)` is source/topic-epoch-local. Future source epochs, replay, deduplication, incremental Silver, and continuous serving remain deferred.
- The producer and Spark jobs have explicit graceful-shutdown contracts. Their operational procedures live in `docs/runbooks/`.
- GitHub Actions runs `make test` on pull requests and pushes to `main`.

## Python environment

Local Conda environment:

`market-streaming`

Python version target:

`>=3.11,<3.12`

The project is installed locally in editable mode with:

`python -m pip install -e .`

Runtime Kafka client dependency:

`confluent-kafka>=2,<3`

Runtime WebSocket client dependency:

`websockets>=15,<16`

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
