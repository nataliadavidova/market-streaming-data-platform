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
- Local Kafka service with Docker Compose.
- Local Kafka Makefile commands for service lifecycle, topic creation, synthetic publish, and bounded consume-one checks.
- Synthetic one-event Kafka producer smoke-check.
- Manual live one-shot Binance WebSocket smoke-check.
- Successful bounded live Binance-to-Kafka smoke-check through the executable producer and Kafka consumer.
- Unit-test CI with GitHub Actions.

In progress:

- Producer hardening for long-running live Binance-to-Kafka execution.

Planned:

- Graceful shutdown and final flush policy.
- Retry and reconnect behavior.
- Delivery acknowledgement handling.
- Remove per-message flush and improve throughput.
- Logging and metrics.
- Producer container and deployment configuration.
- Gap detection, backfill, and deduplication strategy.
- Spark Structured Streaming read from Kafka.
- Stream parsing, validation, normalization, and basic data-quality checks.
- Iceberg writes on S3-compatible storage.
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
- Checkpointing and watermarking.
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
