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

The latest completed live-ingestion milestone is Binance → Kafka → Spark → Iceberg with S3A checkpoint evidence; the newest storage milestone is the isolated persisted Bronze quality contract.

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

The read-only Iceberg inspection workflow is implemented through `make iceberg-inspect` and `jobs.streaming.iceberg_inspection`. It reports existing Bronze table identity, schema, row count, snapshots, history, data files, and partition metadata without creating or mutating tables, starting a streaming query, or reading checkpoints.

The non-persisted Bronze quality classifier is implemented in `jobs.streaming.bronze_quality`. It preserves each raw Kafka-like row and its audit fields, safely classifies JSON, identity, decimal, timestamp, and Kafka-coordinate issues with `is_valid` and ordered `validation_errors`, but it is not connected to the Iceberg sink or production streaming path.

The isolated persisted Bronze quality contract is implemented in `jobs.streaming.iceberg_quality_contract`. It accepts the classifier's exact 15-column output, validates the isolated table schema and incoming DataFrame schema, and performs a static Iceberg append to `market_catalog.market.bronze_trades_quality_smoke`. The canonical table is explicitly rejected; no streaming query, checkpoint, or live write path is involved.

Current local service config:

- `docker-compose.yml` defines local Kafka, MinIO, and Iceberg REST services. Kafka runs single-node KRaft with host listener `localhost:9092` and Docker-network listener `kafka:29092`.
- `docker compose config` has passed for the local services.
- Makefile targets cover explicit Kafka/Iceberg lifecycle, topic checks, and `iceberg-trade-stream`.
- GitHub Actions CI runs `make test` on pull requests and pushes to `main`.

Latest repository state:

- Reconnect lifecycle observability commit: `515b1e1 Add reconnect lifecycle observability`.
- Iceberg inspection implementation commit: `2d6ec09 Add Iceberg inspection workflow`.
- Delivery-result observation commit: `52124a8 Observe Kafka delivery results`.
- Bronze quality classification commit: `fba550e Add Bronze quality classification`.
- Isolated persisted Bronze quality contract commit: `63f910c Add isolated Bronze quality contract`.
- Reconnect implementation commit: `89ec8dd Add Binance producer reconnect`.
- Focused reconnect lifecycle tests: 19 passed in `test_binance_publisher.py`.
- Focused Iceberg inspection tests: 20 passed in `tests/unit/test_streaming_iceberg_inspection.py`.
- Focused Bronze quality tests: 19 passed in `tests/unit/test_streaming_bronze_quality.py`.
- Existing trade parser tests: 2 passed in `tests/unit/test_streaming_trades.py`.
- Full suite at the previous quality milestone: 241 passed.
- Isolated quality-contract tests: 18 passed in `tests/unit/test_streaming_iceberg_quality_contract.py`.
- Full suite after the isolated contract: 259 passed.

The controlled persisted quality-contract smoke passed with Spark 4.1.2, Iceberg 1.11.0, the Iceberg REST catalog, and MinIO. The isolated table was `market_catalog.market.bronze_trades_quality_smoke` at `s3://market-lake/warehouse/market/bronze_trades_quality_smoke`; it had 15 columns, 3 rows, 1 snapshot, and 3 data files. Outcomes were one valid row, one `MALFORMED_JSON` row, and one `INVALID_PRICE` row. The canonical `market_catalog.market.bronze_trades` remained at 13 columns, row count 1, snapshot count 1, latest snapshot `8232280423536300118`, and one data file before and after the smoke. The isolated table's catalog entry was dropped afterward; physical object purge was not separately proven, and services were stopped. No live stream or checkpoint was touched.

Verified runtime evidence:

- A real Binance WebSocket trade reached Kafka through the production receiver/parser and was written to a dedicated Iceberg table with an advancing S3A checkpoint.
- A controlled long-running producer run produced 641 records at Kafka offsets `0..640`; Spark wrote 641 rows with no missing or duplicate offsets in that run.
- Spark restart with the same checkpoint resumed saved Kafka progress; the tested previously committed record was not replayed and a new record was written once.
- Spark application-level SIGINT and SIGTERM, and producer SIGINT and SIGTERM, completed cleanly in the tested scenarios.
- Producer SIGTERM observability showed three dedicated-topic records, return code `0`, final flush `remaining=0`, required INFO markers in order, observed shutdown duration `3.615s` with WebSocket context exit about `2.002s`, no forced cleanup, and no orphan process.
- A controlled local two-session reconnect smoke published trade `990000000001` at Kafka offset `0`, observed a normal close, accepted session 2 after `5.005s`, published trade `990000000002` at offset `1`, logged recovery, remained alive, then handled SIGTERM with final flush `remaining=0`, exit code `0`, and no third session.
- The reconnect observability smoke emitted `BINANCE_RECONNECT_ATTEMPT attempt=1 delay_seconds=5.0 failure_type=ConnectionClosedOK`, measured `5.004438s` from session close to session 2 acceptance, emitted `BINANCE_RECONNECT_RECOVERED attempt=1 recovery_after_seconds=5.024`, and completed SIGTERM with exit code `0`, final flush `remaining=0`, and no third session. These logs are process-local evidence, not durable monitoring.
- A real local-Kafka delivery-result smoke used the production publisher and adapter; `publish_message(..., flush=True)` returned after callback success, and the exact key/value was read back with the dedicated topic end offset advancing by one.
- A controlled Iceberg inspection smoke passed against Spark 4.1.2, Iceberg 1.11.0, the Iceberg REST catalog, and MinIO. The existing Bronze table was inspected twice without a new snapshot or data file. Its unpartitioned `partitions` relation returned one aggregate statistics row without a `partition` column, which the inspector reports explicitly.
- Static Spark quality validation passed with `spark.sql.ansi.enabled=true`: valid, malformed-JSON, and invalid-decimal rows were all classified in one DataFrame execution without changing the Iceberg schema or write path.

These are controlled smokes. They do not establish universal exactly-once, no-loss, no-duplicate, replay/backfill, arbitrary-crash, Kubernetes, or throughput guarantees.

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
- `run_configured_binance_producer(config_path, bootstrap_servers, topic_override=None, *, connect=None)`: loads config, applies an immutable topic override, builds the client/publisher, installs the SIGTERM lifecycle, runs the producer, and finalizes Kafka; `connect` is an injectable WebSocket seam for controlled tests.
- `python -m jobs.producer.binance_producer`: executable command with `--topic` → `KAFKA_TOPIC_TRADES_RAW` → YAML precedence, `KAFKA_BOOTSTRAP_SERVERS` with `localhost:9092` fallback, and standalone INFO logging.

Producer shutdown contract:

- SIGINT keeps the top-level `KeyboardInterrupt` path and returns normally after successful cleanup.
- SIGTERM is handled by an asyncio loop callback that records the request and cancels the main task; the callback does not call WebSocket or Kafka code.
- Cancellation unwinds the WebSocket context before the one bounded five-second final Kafka flush.
- Finalization markers include `FINAL_KAFKA_FLUSH_STARTED`, `FINAL_KAFKA_FLUSH_RESULT`, `FINAL_KAFKA_FLUSH_SUCCEEDED`/`FAILED`, and `PRODUCER_SHUTDOWN_COMPLETED`.
- Runtime and finalization exceptions propagate; cleanup errors must not replace an earlier runtime exception.

Spark/Iceberg contract:

- `jobs/streaming/iceberg_trade_streaming_job.py` reads Kafka, parses the typed Bronze contract, writes through the native Iceberg streaming sink, and uses a query-specific S3A checkpoint.
- `classify_raw_trade_kafka_messages(kafka_df)` is a separate non-persisted transformation; it is not called by the streaming job, Iceberg sink, checkpoint path, or any writer.
- Iceberg uses the REST catalog plus S3FileIO; MinIO stores data and metadata objects locally.
- Graceful Spark shutdown uses a shutdown event, timed `awaitTermination` polling, `query.stop()` before `spark.stop()`, and handler restoration after cleanup.
- `jobs/streaming/iceberg_inspection.py` provides a bounded, read-only table inspection CLI. It validates safe dotted identifiers, uses the existing Iceberg-enabled Spark configuration, and stops its owned Spark session while preserving inspection errors when cleanup also fails.

Known limitations and backlog:

- The reconnect loop retries only classified WebSocket connection-establishment and receive transport failures; parser, configuration, programming, and Kafka publication failures remain fatal.
- Reconnect does not replay or backfill trades missed while the Binance connection is unavailable.
- Reconnect lifecycle logs expose incident-local attempts, configured delay, retryable failure type, and monotonic duration through the first successful Kafka publication; attempt and timing state reset after recovery.
- These logs do not provide persistent counters, process uptime, run/session identifiers, periodic summaries, metrics export, dashboards, alerts, or external health checks.
- Default-path per-message delivery callback observation is implemented; callback failure or a missing callback result raises `KafkaDeliveryError`.
- Broader delivery acknowledgement policy, undelivered-message logging, delivery metrics, and monitoring are not implemented.
- Kafka idempotent producer mode is not enabled.
- Per-message Kafka flush remains synchronous with no explicit timeout; batching and throughput optimization are pending, and the flush return value is not a queue policy.
- `flush=False` remains an unconfirmed enqueue-style compatibility path and is not used by production.
- Polling, backpressure, replay, backfill, and deduplication remain future work.
- There is no general end-to-end exactly-once or business-key deduplication guarantee.
- SIGKILL and arbitrary crash-timing safety are not proven.
- Kubernetes deployment/termination, ClickHouse serving, dashboard, and network-partition recovery remain future work.

Other Markdown status:

- `README.md` records the current verified milestone and operational boundaries.
- `docs/architecture.md` and `docs/roadmap.md` retain the broader target architecture and sequencing.
- Existing runbooks under `docs/runbooks/` contain operational procedures and historical evidence; update current capability statements without rewriting historical results.

Next stage:

- The non-persisted Bronze quality classification slice is complete; its earlier persisted-contract design guidance is now historical.
- The isolated persisted Bronze quality contract slice is complete. The next storage slice is canonical Bronze quality migration: evolve the canonical table from 13 to 15 columns with explicit `ALTER TABLE`, connect the classifier to the live path, use a new versioned checkpoint rather than reusing the old 13-column checkpoint, and validate controlled start/restart behavior. Do not claim this migration is implemented.
- Reconnect, default-path delivery-result observation, and reconnect lifecycle logging are complete in the tested scope. Next, make a read-only decision between producer throughput/per-message flush and broader monitoring; keep these reliability areas separate.
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

Reconnect, default-path delivery-result observation, reconnect lifecycle logging, read-only Iceberg inspection, non-persisted Bronze quality classification, and isolated persisted quality-contract validation are implemented and tested. The next storage slice is canonical Bronze quality migration. Keep producer throughput/per-message flush, broader monitoring, historical backfill, Silver, and maintenance separate from that storage work.

## Historical pre-52124a8 next step

Reconnect is implemented and live-smoke tested. Perform a read-only decision between delivery observability/callbacks, producer throughput/per-message flush, and monitoring. Keep those reliability areas separate; do not implement them together.

Current test suite:

- 19 focused Bronze quality tests pass in `tests/unit/test_streaming_bronze_quality.py`.
- 2 existing trade parser tests pass in `tests/unit/test_streaming_trades.py`.
- 20 focused Iceberg inspection tests pass in `tests/unit/test_streaming_iceberg_inspection.py`.
- 19 focused reconnect lifecycle tests pass in `tests/unit/test_binance_publisher.py`.
- 259 tests pass in the full suite after the isolated persisted contract.
- Tests are not automatically rerun for documentation-only changes unless explicitly requested.
