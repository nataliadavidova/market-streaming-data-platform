# market-streaming-data-platform

A portfolio Data Engineering project for a real-time market data platform.

The Version 1 target flow is:

`Market API/WebSocket -> Kafka -> Spark Structured Streaming -> Iceberg on S3-compatible storage -> ClickHouse -> dashboard + basic DQ checks`

The implemented ingestion path is now:

`Binance WebSocket -> production Binance producer -> Kafka -> Spark Structured Streaming -> typed Bronze parser -> Iceberg Bronze table -> Parquet/metadata in MinIO`

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
- Spark Kafka source: implemented.
- Spark typed Bronze parser: implemented.
- Iceberg REST catalog and S3FileIO configuration: implemented.
- Iceberg Bronze table: implemented.
- Native Iceberg streaming sink: implemented.
- S3A checkpointing: implemented and restart-tested.
- Spark graceful SIGINT: live-tested.
- Spark graceful SIGTERM: live-tested.
- Producer graceful SIGINT: live-tested.
- Producer graceful SIGTERM: live-tested.
- Producer shutdown/final-flush INFO logging: implemented and live-tested.

This is a verified ingestion milestone, not a claim that the whole Version 1 platform is complete.

## Current Implementation

Implemented foundation:

- Typed producer config loading from `config/market_symbols.yaml`.
- `TradeEvent` modeling and deterministic JSON serialization with decimal values preserved as strings.
- Binance combined trade-stream URL construction, receive, and parsing for `BTCUSDT`, `ETHUSDT`, and `SOLUSDT`.
- Reusable Binance WebSocket receiver session and sequential publish loop.
- Kafka message preparation, `KafkaPublisher`, and the Confluent Kafka adapter.
- Executable Binance-to-Kafka producer: `python -m jobs.producer.binance_producer`.
- CLI topic override with precedence `--topic -> KAFKA_TOPIC_TRADES_RAW -> config.kafka.raw_topic`.
- Default Kafka bootstrap behavior: `KAFKA_BOOTSTRAP_SERVERS`, falling back to `localhost:9092`.
- Spark Structured Streaming Kafka source and typed Bronze parser.
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

## Running Spark -> Iceberg

Start the existing application from the repository root:

```bash
make iceberg-trade-stream
```

The target runs:

`Kafka source -> Bronze parser -> native Iceberg streaming sink -> S3A checkpoint`

Deployment values are supplied through the existing environment contract. See [.env.example](.env.example) for the Kafka, Iceberg REST catalog, MinIO/S3, table, checkpoint, query-name, and application-name groups.

The target does not start infrastructure or bootstrap tables automatically. Start Kafka, MinIO, and Iceberg REST explicitly, and use a dedicated topic/table/checkpoint for smoke tests.

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

These are controlled smoke results, not universal delivery or failure-safety guarantees.

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

Latest verified suite: 185 tests passed. Focused producer tests: 25 passed.

## Manual smoke checks

Manual smoke checks are kept out of CI because they depend on local services or external network availability. Existing runbooks cover the foundational Kafka, one-shot Binance, and Binance-to-Kafka checks:

- [Kafka smoke-check](docs/runbooks/kafka-smoke-check.md)
- [Binance one-shot smoke-check](docs/runbooks/binance-one-shot-smoke-check.md)
- [Binance-to-Kafka end-to-end smoke-check](docs/runbooks/binance-kafka-e2e-smoke-check.md)

The broader Binance -> Kafka -> Spark -> Iceberg and checkpoint-recovery results above were executed as dedicated controlled smokes and are summarized here without transient IDs, PIDs, credentials, or temporary logs.

## Limitations and backlog

- Reconnect settings exist in configuration, but a reconnect loop is not implemented.
- Kafka delivery callbacks and explicit delivery acknowledgement observability are not implemented.
- Kafka idempotent producer mode is not enabled.
- The producer still flushes after every message; throughput benchmarking and optimization are pending.
- There is no general end-to-end exactly-once guarantee.
- Business-key deduplication is not implemented.
- SIGKILL and arbitrary crash-timing safety are not proven.
- Kubernetes deployment, readiness, and termination integration are not verified.
- ClickHouse serving and dashboard layers remain future roadmap work.
- Network-partition recovery, rate-limit handling, and long-running throughput stability remain unverified.

## Roadmap and architecture

- [Architecture](docs/architecture.md)
- [Roadmap](docs/roadmap.md)

The architecture and roadmap documents retain the broader target plan; this README records which ingestion and shutdown slices have actually been implemented and smoke-tested.
