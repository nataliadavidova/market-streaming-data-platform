# Roadmap

This project is currently in the Version 1 bootstrap phase. Later versions are planned but not implemented yet.

## Version 1: Market Streaming MVP

Target flow:

`Market API/WebSocket -> Kafka -> Spark Structured Streaming -> Iceberg on S3-compatible storage -> ClickHouse -> dashboard + basic DQ checks`

Completed:

- Repository bootstrap and Python package structure.
- Typed producer configuration loading from `config/market_symbols.yaml`.
- `TradeEvent` model and deterministic JSON serialization.
- Binance trade payload parsing.
- Binance combined trade-stream URL construction from configured symbols and `ProducerConfig`.
- Single-message WebSocket receiver with receive-boundary timestamp capture.
- Reusable WebSocket receiver session for multiple receives over one connection.
- Binance combined-message parser.
- One-shot Binance receive-and-parse composition returning `TradeEvent`.
- Reusable Binance trade receiver session.
- Per-event Binance-to-Kafka publication operation.
- Permanent sequential Binance publish loop.
- Binance publisher runtime that owns the receiver session around the loop.
- Reusable Kafka client factory.
- Executable Binance producer entrypoint: `python -m jobs.producer.binance_producer`.
- Binance producer `--topic` override with CLI → environment → YAML precedence.
- Application-owned final Kafka flush in the executable producer assembly.
- Timeout-aware `KafkaProducerClient.flush` contract.
- Timeout forwarding in `ConfluentKafkaProducerClient`.
- Bounded application-level final Kafka flush.
- Explicit `KafkaFinalizationError` when messages remain queued after the finalization timeout.
- Clean top-level `SIGINT`/`KeyboardInterrupt` handling for expected operator shutdown.
- Local Kafka service with Docker Compose.
- Local Kafka Makefile commands for service lifecycle, topic creation, synthetic publish, and bounded consume-one checks.
- Synthetic one-event Kafka producer smoke-check.
- Manual live one-shot Binance WebSocket smoke-check.
- Successful bounded live Binance-to-Kafka smoke-check through the executable producer and Kafka consumer.
- Successful bounded live graceful-finalization smoke-check confirming fresh Binance-to-Kafka delivery, producer exit status `0`, and no cancellation or `KeyboardInterrupt` traceback.
- Spark Kafka source and typed Bronze parser.
- Iceberg REST catalog, S3FileIO, Bronze table contract, and native Iceberg streaming sink.
- Query-specific Hadoop S3A checkpoint configuration.
- Dedicated Kafka → Spark → Iceberg smoke with checkpoint progress and recovery verification.
- Graceful Spark SIGINT and SIGTERM shutdown with query-before-Spark cleanup order.
- Graceful producer SIGINT and SIGTERM shutdown with bounded final Kafka flush.
- Runtime INFO logging for producer lifecycle and final-flush markers.
- Focused unit coverage for bounded Kafka finalization.
- Unit-test CI with GitHub Actions.

In progress:

- Documentation consistency for the completed ingestion milestone.

Planned:

- Shutdown-latency investigation.
- WebSocket close-timeout tuning or instrumentation.
- Shutdown-stage timing.
- Per-message flush timeout or removal.
- Delivery or undelivered-message logging and metrics.
- Second-`SIGINT` escalation behavior and bounded escalation policy.
- Retry and reconnect behavior.
- Delivery acknowledgement handling.
- Improve throughput.
- Extended logging and metrics beyond the verified lifecycle markers.
- Producer container and deployment configuration.
- Gap detection, backfill, and deduplication strategy.
- Additional stream validation, normalization, and data-quality checks.
- ClickHouse aggregate writes.
- Dashboard or analytical SQL layer.

## Version 2: CDC + Greenplum MVP

Completed:

- None yet.

In progress:

- None yet.

Planned:

- PostgreSQL operational source database.
- Debezium CDC into Kafka.
- Greenplum data warehouse flow.

## Version 3: dbt / Marts / Docs / Basic Lineage

Completed:

- None yet.

In progress:

- None yet.

Planned:

- dbt models.
- dbt tests and docs.
- Analytical marts.
- Basic lineage documentation.

## Version 4: Production-Like Reliability

Completed:

- GitHub Actions CI for unit tests.

In progress:

- None yet.

Planned:

- Schema Registry.
- DLQ or quarantine topics.
- Monitoring and alerting.
- Consumer lag alerts.
- Additional checkpointing/watermarking reliability controls.
- Idempotency strategy.
- Security and secrets handling.
- Expanded CI/CD.
- Lineage, catalog, and governance features.

## Version 5: ML / MLOps

Completed:

- None yet.

In progress:

- None yet.

Planned:

- Feature tables.
- Feature store.
- MLflow tracking.
- Model training.
- Prediction table or API.

## Version 6: Cloud / Infra / Terraform / Optional Kubernetes

Completed:

- None yet.

In progress:

- None yet.

Planned:

- Terraform-managed cloud resources.
- Cloud deployment strategy.
- Optional Kubernetes deployment.
