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
- Binance combined-message parser.
- One-shot Binance receive-and-parse composition returning `TradeEvent`.
- Local Kafka service with Docker Compose.
- Local Kafka Makefile commands for service lifecycle, topic creation, synthetic publish, and bounded consume-one checks.
- Synthetic one-event Kafka producer smoke-check.
- Manual live one-shot Binance WebSocket smoke-check.
- Unit-test CI with GitHub Actions.

In progress:

- Producer foundation for live Binance trade ingestion.

Planned:

- Smallest testable long-lived WebSocket receive primitive.
- Continuous Binance receive loop without reconnecting for every message.
- Retry and reconnect behavior.
- Graceful shutdown.
- Live Binance-to-Kafka publication.
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
