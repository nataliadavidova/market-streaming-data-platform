# market-streaming-data-platform

A portfolio Data Engineering project for a real-time market data platform.

The Version 1 target flow is:

`Market API/WebSocket -> Kafka -> Spark Structured Streaming -> Iceberg on S3-compatible storage -> ClickHouse -> dashboard + basic DQ checks`

The implemented analytical path is now:

`Binance WebSocket -> production Binance producer -> Kafka -> Spark Structured Streaming -> Bronze quality classifier -> 15-column Iceberg Bronze table -> deterministic Silver Iceberg -> Parquet/metadata in MinIO`

Spark Kafka progress is persisted separately through:

`Spark checkpoint -> Hadoop S3A -> MinIO`

The REST catalog stores the current Iceberg metadata pointer. ClickHouse, dashboard serving, and the broader Version 1 target are still ahead on the roadmap.

## Technology Stack

Current and planned technologies:

- Python 3.11
- Binance WebSocket
- Kafka
- Spark Structured Streaming
- Apache Iceberg
- MinIO as local S3-compatible storage
- Iceberg REST catalog
- ClickHouse (planned)
- Docker Compose for local services
- GitHub Actions for unit-test CI

## Current Status

- Binance WebSocket producer: implemented and live-smoke tested.
- Producer CLI topic override: implemented and live-smoke tested.
- Local Kafka infrastructure: implemented and smoke-tested.
- Kafka broker persistence: implemented with a named `kafka_data` volume and smoke-tested across the normal Compose `down -> up` lifecycle.
- Spark Kafka source: implemented.
- Spark typed Bronze parser: implemented.
- Bronze quality classification: implemented, statically Spark-tested, and connected to the canonical live stream.
- Isolated persisted Bronze quality contract: implemented and controlled-smoke tested.
- Canonical Bronze quality schema and live integration: implemented and controlled start/restart tested.
- Iceberg REST catalog and S3FileIO configuration: implemented.
- Iceberg Bronze table: implemented.
- Native Iceberg streaming sink: implemented.
- S3A checkpointing: implemented and restart-tested.
- Spark graceful SIGINT: live-tested.
- Spark graceful SIGTERM: live-tested.
- Producer graceful SIGINT: live-tested.
- Producer graceful SIGTERM: live-tested.
- Producer shutdown/final-flush INFO logging: implemented and live-tested.
- Reconnect lifecycle observability: implemented and controlled-smoke tested.
- Bronze Iceberg -> deterministic Silver Iceberg: implemented; Silver contains only valid trades and is reproducibly rebuilt.

This is a verified ingestion milestone, not a claim that the whole Version 1 platform is complete.

## Current Implementation

Implemented foundation:

- Typed producer config loading from `config/market_symbols.yaml`.
- `TradeEvent` modeling and deterministic JSON serialization with decimal values preserved as strings.
- Binance combined trade-stream URL construction, receive, and parsing for `BTCUSDT`, `ETHUSDT`, and `SOLUSDT`.
- Reusable Binance WebSocket receiver session and sequential publish loop.
- Automatic reconnect around complete WebSocket sessions for classified connection and receive transport failures.
- Capped exponential reconnect backoff from 5 to 60 seconds, reset after the first successful Kafka publication in a recovered session.
- Process-local reconnect lifecycle logs expose the incident attempt number, configured delay, retryable failure type, and monotonic disconnected duration after successful recovery.
- Parser, configuration, programming, and Kafka publication failures remain fail-fast; cancellation is not reconnectable.
- Kafka message preparation, `KafkaPublisher`, and the Confluent Kafka adapter.
- Default `KafkaPublisher.publish_message(..., flush=True)` observes the per-message Kafka delivery callback and raises `KafkaDeliveryError` for a reported failure or a missing callback result.
- Kafka delivery failures remain outside the Binance WebSocket reconnect path.
- Executable Binance-to-Kafka producer: `python -m jobs.producer.binance_producer`.
- CLI topic override with precedence `--topic -> KAFKA_TOPIC_TRADES_RAW -> config.kafka.raw_topic`.
- Default Kafka bootstrap behavior: `KAFKA_BOOTSTRAP_SERVERS`, falling back to `localhost:9092`.
- Spark Structured Streaming Kafka source and Bronze quality classifier.
- The live classifier preserves each raw Kafka row and audit fields, safely parses trade fields, and appends `is_valid` plus ordered `validation_errors` to the canonical 15-column Bronze contract.
- Isolated persisted quality-contract helper: validates a classified 15-column DataFrame, creates or validates `market_catalog.market.bronze_trades_quality_smoke`, and performs a static Iceberg append. It rejects the canonical Bronze table, uses no checkpoint, and does not alter the live streaming path.
- Iceberg REST catalog, S3FileIO, Bronze table contract, and native Iceberg streaming sink.
- Query-specific S3A checkpoint configuration.
- Graceful Spark signal handling with timed polling, `query.stop()` before `spark.stop()`, and restored signal handlers.
- Graceful producer SIGINT/SIGTERM handling with WebSocket cleanup and one bounded final Kafka flush.
- Runtime INFO logging for producer lifecycle markers.

## Running the producer

Run the configured long-running producer:

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
python -m jobs.producer.binance_producer
```

For a smoke run or another isolated destination, override only the topic:

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
python -m jobs.producer.binance_producer \
  --topic market.trades.example
```

Topic precedence is:

`--topic -> KAFKA_TOPIC_TRADES_RAW -> config.kafka.raw_topic`

Without an override, the producer uses `kafka.raw_topic` from `config/market_symbols.yaml`. The CLI override allows a dedicated topic without editing that tracked YAML file. No other producer CLI options are implied by this interface.

## Kafka broker persistence

Local Kafka stores broker state in the named Compose volume `kafka_data`, mounted at `/var/lib/kafka/data` with `KAFKA_LOG_DIRS=/var/lib/kafka/data`. The ordinary `make kafka-down` -> `make kafka-up` lifecycle therefore preserves local topics, offsets, and records. This is local single-broker persistence, not replication or disaster recovery.

The previous configuration left Kafka state in container-local `/tmp/kafka-logs`; that old log was not recoverable. The legacy and quality-v1 Spark checkpoints remain preserved and are not reused by the current quality-v2 Kafka timeline.

## Running Spark -> Iceberg

Start the existing application from the repository root:

```bash
make iceberg-trade-stream
```

The target runs:

`Binance BTCUSDT/ETHUSDT/SOLUSDT -> Kafka -> Bronze quality classifier -> canonical 15-column Iceberg table -> quality-v2 S3A checkpoint`

Before reading Kafka or starting the query, the job requires the exact canonical 15-column schema. Its first start explicitly uses Kafka `startingOffsets=latest`; subsequent starts resume from `s3a://market-lake/checkpoints/market/bronze-trades-quality-v2`. The legacy and quality-v1 checkpoints are retained but rejected by the quality-v2 job.

Deployment values are supplied through the existing environment contract. See [.env.example](.env.example) for the Kafka, Iceberg REST catalog, MinIO/S3, table, checkpoint, query-name, and application-name groups.

The target does not start infrastructure or bootstrap tables automatically. Start Kafka, MinIO, and Iceberg REST explicitly, and use a dedicated topic/table/checkpoint for smoke tests.

## Inspecting Iceberg

Use the read-only Bronze inspection workflow to review the current table identity and location, schema, row count, Iceberg snapshots and history, data files, and partition metadata:

```bash
make iceberg-up
make iceberg-inspect
make iceberg-down
```

The inspector reads an existing table without creating or modifying it, starting a streaming query, or inspecting/changing the Spark checkpoint. `COUNT(*)` is appropriate for the current small local dataset, but can be expensive on a large table. See the [Iceberg inspection runbook](docs/runbooks/iceberg-inspection.md) for prerequisites, output interpretation, and the controlled runtime evidence.

## Bronze quality classification

The live Spark transformation accepts raw Kafka records, preserves every row with its raw JSON and Kafka audit metadata, safely parses trade fields, and persists `is_valid` plus ordered `validation_errors` in canonical Bronze. Invalid records are classified rather than filtered. See the [Bronze quality migration and cutover runbook](docs/runbooks/bronze-quality-migration.md) for the schema, checkpoint, and restart boundaries.

## Persisted Bronze quality contract

The classifier can now be persisted through a strictly isolated static contract:

`classified 15-column DataFrame -> exact schema validation -> static Iceberg append`

The smoke table is `market_catalog.market.bronze_trades_quality_smoke`. It contains the existing 13 Bronze fields plus `is_valid` and `validation_errors`. The helper validates both the existing table schema and incoming DataFrame contract before appending, rejects the canonical table as a target, and uses no streaming query or checkpoint. That isolated workflow does not alter the canonical table; its separate migration is documented below.

## Migrating canonical Bronze

The explicit, idempotent migration workflow is:

```bash
make iceberg-up
make iceberg-migrate-bronze-quality
make iceberg-inspect
make iceberg-down
```

The migration targets only `market_catalog.market.bronze_trades`. It recognizes `LEGACY_13_COLUMN`, `QUALITY_15_COLUMN`, and `INCOMPATIBLE` states; runs one additive `ALTER TABLE` only for the exact legacy schema; validates the final 15-column schema; and never drops, recreates, overwrites, truncates, or backfills rows. A second run reports `ALREADY_MIGRATED` without another ALTER. See the [Bronze quality migration runbook](docs/runbooks/bronze-quality-migration.md).

The canonical table and live writer now share the exact 15-column quality contract. The live job uses the versioned `quality-v2` checkpoint and query name; it does not reuse or delete either historical checkpoint.

The local streaming MVP has completed a controlled real Binance smoke across BTCUSDT, ETHUSDT, and SOLUSDT. See the [Bronze quality migration and live cutover runbook](docs/runbooks/bronze-quality-migration.md) for observed counts and boundaries. Silver is complete; ClickHouse serving and dashboard work remain next.

## Shutdown behavior

### Producer

On a handled `SIGINT` or `SIGTERM`, the producer stops accepting new WebSocket messages, exits the WebSocket context, and performs one bounded final Kafka flush with a five-second timeout. A successful controlled shutdown returns exit code `0`.

The runtime INFO markers are:

```text
PRODUCER_SHUTDOWN_REQUESTED signal=SIGTERM
FINAL_KAFKA_FLUSH_STARTED timeout=5.0
FINAL_KAFKA_FLUSH_RESULT remaining=0
FINAL_KAFKA_FLUSH_SUCCEEDED
PRODUCER_SHUTDOWN_COMPLETED signal=SIGTERM
```

`remaining=0` means that the local Kafka producer queue had no messages left after the final flush. It does not prove absence of loss or duplication under every failure mode.

### Spark application

`SIGINT` or `SIGTERM` sets a shutdown event. The main application flow uses timed `awaitTermination` polling, then calls `query.stop()` before `spark.stop()`. Signal callbacks do not call Spark or Py4J directly. The tested application-level shutdowns returned cleanly without Py4J traceback or forced cleanup.

## Verified runtime checks

The following scenarios have been executed with dedicated runtime resources:

### Live one-event path

- A real Binance WebSocket trade was received by production receiver/parser helpers.
- The event was published to a dedicated Kafka topic.
- Spark parsed it into one Iceberg Bronze row.
- The S3A checkpoint advanced.

### Long-running production producer path

- The real executable `python -m jobs.producer.binance_producer` was used.
- `--topic` directed the run to a dedicated topic.
- One controlled run produced 641 Kafka records at offsets `0..640`.
- Spark wrote 641 Iceberg rows with no missing or duplicate Kafka offsets in that run.
- The checkpoint reached the final Kafka end offset.

### Spark recovery

- The same checkpoint and query identity were reused.
- The restarted application used a new run ID while retaining the same query identity.
- The restarted run resumed from saved Kafka progress.
- The previously committed record was not replayed in the tested scenario.
- A new record was written once.

### Spark shutdown

- Application-level SIGINT and SIGTERM were tested.
- The tested `make` wrapper exited `0`.
- No Py4J traceback, forced cleanup, or orphan process remained.

### Producer SIGTERM

- The real Binance producer wrote three records to a dedicated topic.
- Exact subprocess return code was `0`.
- Observed shutdown duration was `3.615s`; WebSocket context exit was about `2.002s`.
- Final flush reported `remaining=0`.
- All required lifecycle markers appeared in order.
- No forced cleanup or orphan process remained.

### Producer reconnect

- A controlled local two-session smoke emitted one `BINANCE_RECONNECT_ATTEMPT` warning with the configured five-second delay.
- Session 2 was accepted about five seconds after the first close, and `BINANCE_RECONNECT_RECOVERED` reported about five seconds from the first failure through successful Kafka publication.
- Two expected Kafka records were produced; the producer remained alive after recovery, then handled `SIGTERM`, flushed with `remaining=0`, exited `0`, and opened no third session.
- This is process-local lifecycle evidence. The two observed records are not a general exactly-once, no-loss, or no-duplication guarantee.

These are controlled smoke results, not universal delivery or failure-safety guarantees.

### Kafka delivery result observation

- A real local-Kafka smoke used the production `KafkaPublisher` and `ConfluentKafkaProducerClient`.
- `publish_message(..., flush=True)` returned only after the delivery callback reported success.
- The exact published key/value was read back from Kafka and the dedicated topic end offset advanced by one.
- This validates the current per-message callback boundary, not end-to-end exactly-once delivery.

## Local development

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

Latest verified suite: 298 tests passed. Live-quality integration coverage includes 2 Kafka-source tests, 7 S3A-checkpoint tests, 51 streaming-job tests, and 19 Bronze-classifier tests.

## Manual smoke checks

Manual smoke checks are kept out of CI because they depend on local services or external network availability. Existing runbooks cover the foundational Kafka, one-shot Binance, and Binance-to-Kafka checks:

- [Kafka smoke-check](docs/runbooks/kafka-smoke-check.md)
- [Binance one-shot smoke-check](docs/runbooks/binance-one-shot-smoke-check.md)
- [Binance-to-Kafka end-to-end smoke-check](docs/runbooks/binance-kafka-e2e-smoke-check.md)

The broader Binance -> Kafka -> Spark -> Iceberg and checkpoint-recovery results above were executed as dedicated controlled smokes and are summarized here without transient IDs, PIDs, credentials, or temporary logs.

## Limitations and backlog

- Reconnect covers classified WebSocket connection/session transport failures only; it does not replay or backfill trades missed while disconnected.
- WebSocket reconnect is not Kafka recovery, delivery acknowledgement policy, deduplication, or an end-to-end exactly-once guarantee.
- On the default `flush=True` path, callback success is distinct from local queue acceptance and final-flush `remaining=0`; it is not an end-to-end exactly-once guarantee.
- `KafkaDeliveryError` makes callback-reported or missing per-message delivery results explicit, but broader acknowledgement policy and delivery metrics are not implemented.
- Reconnect lifecycle logs are process-local and do not provide persistent counters, uptime, run/session identifiers, metrics export, dashboards, or alerting.
- Kafka idempotent producer mode is not enabled.
- The producer still performs synchronous per-message `flush()` with no explicit timeout, which limits batching and throughput; its return value is not yet used as a queue policy.
- `flush=False` remains an unconfirmed enqueue-style compatibility path and is not used by production.
- There is no general end-to-end exactly-once guarantee.
- The controlled quality-v1 restart observed Kafka offsets `0..4` appended once, but this does not establish universal exactly-once behavior, replay support, or compatibility with the legacy writer/checkpoint.
- The isolated quality smoke produced three rows and three data files. This is controlled contract evidence, not a compaction or file-layout guarantee; physical object purge after catalog cleanup was not separately proven.
- Business-key deduplication is not implemented.
- Monitoring, polling, batching, backpressure, replay, and backfill remain future work.
- SIGKILL and arbitrary crash-timing safety are not proven.
- Kubernetes deployment, readiness, and termination integration are not verified.
- ClickHouse serving and dashboard layers remain future roadmap work.
- Network-partition recovery, rate-limit handling, and long-running throughput stability remain unverified.

## Roadmap and architecture

- [Architecture](docs/architecture.md)
- [Roadmap](docs/roadmap.md)

The architecture and roadmap documents retain the broader target plan; this README records which ingestion and shutdown slices have actually been implemented and smoke-tested.
