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

The latest completed milestone is live Binance → Kafka → Spark → Iceberg ingestion with S3A checkpoint evidence.

Current Python package:

`jobs`

Current config file:

`config/market_symbols.yaml`

Current architecture boundaries:

- Kafka separates the Binance producer from Spark processing.
- Iceberg table metadata is managed through the REST catalog and S3FileIO.
- Spark progress is stored through Hadoop S3A checkpoints.
- MinIO stores Iceberg data, metadata, and checkpoint objects for local smoke runs.
- Production Bronze must not be used for destructive smoke tests; use a dedicated topic, table, and checkpoint.

Current local service config:

- `docker-compose.yml` defines local Kafka, MinIO, and Iceberg REST services. Kafka runs single-node KRaft with host listener `localhost:9092` and Docker-network listener `kafka:29092`.
- `docker compose config` has passed for the local services.
- Makefile targets cover explicit Kafka/Iceberg lifecycle, topic checks, and `iceberg-trade-stream`.
- GitHub Actions CI runs `make test` on pull requests and pushes to `main`.

Latest repository state:

- Latest commit: `f0004fc Enable Binance producer runtime logging`.
- Focused producer tests: 25 passed.
- Full suite: 185 passed.
- The current documentation slice is not yet committed.

Verified runtime evidence:

- A real Binance WebSocket trade reached Kafka through the production receiver/parser and was written to a dedicated Iceberg table with an advancing S3A checkpoint.
- A controlled long-running producer run produced 641 records at Kafka offsets `0..640`; Spark wrote 641 rows with no missing or duplicate offsets in that run.
- Spark restart with the same checkpoint resumed saved Kafka progress; the tested previously committed record was not replayed and a new record was written once.
- Spark application-level SIGINT and SIGTERM, and producer SIGINT and SIGTERM, completed cleanly in the tested scenarios.
- Producer SIGTERM observability showed three dedicated-topic records, return code `0`, final flush `remaining=0`, required INFO markers in order, observed shutdown duration `3.615s` with WebSocket context exit about `2.002s`, no forced cleanup, and no orphan process.

These are controlled smokes. They do not establish universal exactly-once, no-loss, no-duplicate, reconnect, arbitrary-crash, Kubernetes, or throughput guarantees.

Current producer modules:

- `jobs/producer/config.py`
- `jobs/producer/events.py`
- `jobs/producer/binance.py`
- `jobs/producer/kafka.py`
- `jobs/producer/publisher.py`
- `jobs/producer/confluent.py`
- `jobs/producer/smoke_publish_one.py`
- `jobs/producer/websocket.py`
- `jobs/producer/binance_publisher.py`
- `jobs/producer/binance_producer.py`

Current implemented functions and models:

- `load_config(config_path)`: reads YAML config using PyYAML and returns a Python dictionary.
- `load_producer_config(config_path)`: reads and validates producer config using Pydantic models.
- `TradeEvent`: internal producer trade event contract using `Decimal` for `price` and `quantity`.
- `TradeEvent.to_json_message()`: serializes deterministic JSON while preserving decimal values as strings.
- Binance URL and parser helpers build combined `@trade` streams and map Binance payloads into `TradeEvent`.
- Reusable WebSocket/Binance receiver sessions capture receive-boundary timestamps and support repeated receives.
- `prepare_trade_event_kafka_message(event)`: prepares the UTF-8-compatible key/value contract.
- `KafkaPublisher` and `ConfluentKafkaProducerClient`: injectable publisher and concrete Kafka adapter boundaries.
- `build_kafka_client(bootstrap_servers)`: creates the concrete Confluent Kafka client.
- `receive_and_publish_one_binance_trade(receiver, publisher)`: receives one event, prepares one message, and publishes it.
- `run_binance_trade_publish_loop(receiver, publisher)`: permanently repeats sequential receive/publish operations.
- `run_binance_trade_publisher(config, publisher)`: owns the Binance receiver context around that loop.
- `run_configured_binance_producer(config_path, bootstrap_servers, topic_override=None)`: loads config, applies an immutable topic override, builds the client/publisher, installs the SIGTERM lifecycle, runs the producer, and finalizes Kafka.
- `python -m jobs.producer.binance_producer`: executable command with `--topic` → `KAFKA_TOPIC_TRADES_RAW` → YAML precedence, `KAFKA_BOOTSTRAP_SERVERS` with `localhost:9092` fallback, and standalone INFO logging.

Producer shutdown contract:

- SIGINT keeps the top-level `KeyboardInterrupt` path and returns normally after successful cleanup.
- SIGTERM is handled by an asyncio loop callback that records the request and cancels the main task; the callback does not call WebSocket or Kafka code.
- Cancellation unwinds the WebSocket context before the one bounded five-second final Kafka flush.
- Finalization markers include `FINAL_KAFKA_FLUSH_STARTED`, `FINAL_KAFKA_FLUSH_RESULT`, `FINAL_KAFKA_FLUSH_SUCCEEDED`/`FAILED`, and `PRODUCER_SHUTDOWN_COMPLETED`.
- Runtime and finalization exceptions propagate; cleanup errors must not replace an earlier runtime exception.

Spark/Iceberg contract:

- `jobs/streaming/iceberg_trade_streaming_job.py` reads Kafka, parses the typed Bronze contract, writes through the native Iceberg streaming sink, and uses a query-specific S3A checkpoint.
- Iceberg uses the REST catalog plus S3FileIO; MinIO stores data and metadata objects locally.
- Graceful Spark shutdown uses a shutdown event, timed `awaitTermination` polling, `query.stop()` before `spark.stop()`, and handler restoration after cleanup.

Known limitations and backlog:

- Reconnect settings exist in configuration, but a reconnect loop is not implemented.
- Delivery callbacks and explicit delivery acknowledgement observability are not implemented.
- Kafka idempotent producer mode is not enabled.
- Per-message Kafka flush remains enabled; throughput benchmarking and optimization are pending.
- There is no general end-to-end exactly-once or business-key deduplication guarantee.
- SIGKILL and arbitrary crash-timing safety are not proven.
- Kubernetes deployment/termination, ClickHouse serving, dashboard, and network-partition recovery remain future work.

Other Markdown status:

- `README.md` records the current verified milestone and operational boundaries.
- `docs/architecture.md` and `docs/roadmap.md` retain the broader target architecture and sequencing.
- Existing runbooks under `docs/runbooks/` include historical procedures and may contain pre-milestone wording; they were intentionally not changed in this documentation slice.

Next stage:

- This documentation milestone is the current slice.
- After it, make a read-only decision between Binance reconnect, delivery observability/callbacks, and producer throughput/per-message flush.
- Do not combine those three reliability areas in one slice.

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

## Immediate next likely step

After this documentation slice, perform a read-only decision between Binance reconnect, delivery observability/callbacks, and producer throughput/per-message flush. Keep those three reliability areas separate; do not implement them together.

Current test suite:

- 25 focused producer tests pass.
- 185 tests pass in the full suite.
- Tests are not automatically rerun for documentation-only changes unless explicitly requested.
