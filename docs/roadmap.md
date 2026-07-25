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
- Explicit non-persisted Bronze quality classification (`fba550e Add Bronze quality classification`): every input row is preserved, raw and Kafka audit evidence remains available, malformed JSON is classified, invalid decimals are safely handled with `try_cast`, and ordered `validation_errors` plus `is_valid` are produced without changing the write path.
- Isolated persisted Bronze quality contract (`63f910c Add isolated Bronze quality contract`): the exact 15-column schema is validated and statically appended to `market_catalog.market.bronze_trades_quality_smoke`; canonical-table targeting is rejected, and no live stream or checkpoint is involved.
- Canonical Bronze quality-schema migration (`f698e3d Add canonical Bronze quality migration`): exact 13- and 15-column schema states are detected, incompatible schemas are rejected, one additive `ALTER TABLE` is validated, and a second run is idempotent. The migration tests passed 28, existing Bronze tests passed 5, and the full suite passed 287.
- Controlled canonical migration smoke passed with Spark 4.1.2, Iceberg 1.11.0, REST catalog, and MinIO. The table moved from 13 to 15 columns while its one row, one snapshot/history entry, latest snapshot `8232280423536300118`, one data file/path, and location remained unchanged. The historical quality fields are `NULL / NULL` (“not evaluated under the quality contract”). No backfill, live stream, Kafka, producer, or checkpoint operation occurred.
- Canonical Bronze live quality integration (`c4228ff Connect Bronze quality live stream`): the live job requires the exact 15-column schema, uses `classify_raw_trade_kafka_messages(...)`, writes through the existing append sink, and uses the versioned quality-v1 checkpoint and query name with explicit first-start `startingOffsets=latest`.
- Live quality validation passed with 298 tests plus controlled initial-start and restart smokes. In the observed run, Kafka partition 0 offsets `0..4` were appended once across the two starts with valid, `MALFORMED_JSON`, `INVALID_PRICE`, and `INVALID_QUANTITY` outcomes.
- Iceberg REST catalog, S3FileIO, Bronze table contract, and native Iceberg streaming sink.
- Query-specific Hadoop S3A checkpoint configuration.
- Read-only Iceberg inspection workflow (`2d6ec09 Add Iceberg inspection workflow`) covering table identity, schema, row count, snapshots, history, data files, and partition metadata through `make iceberg-inspect`.
- Controlled Spark 4.1.2 / Iceberg 1.11.0 / Iceberg REST / MinIO inspection smoke, with 20 focused inspection tests and 222 tests in the full suite at that milestone.
- Bronze quality classification evidence: 19 focused quality tests, 2 existing trade parser tests, 241 tests at that milestone, and static Spark validation with `spark.sql.ansi.enabled=true`.
- Persisted quality-contract evidence: 18 focused tests, 19 classifier tests, 259 tests in the full suite, and a controlled Spark 4.1.2 / Iceberg 1.11.0 / REST catalog / MinIO smoke with 3 classified rows, 1 snapshot, 3 data files, one valid result, one `MALFORMED_JSON` result, and one `INVALID_PRICE` result. The canonical 13-column table was unchanged before and after; the isolated catalog entry was dropped afterward, while physical object purge was not separately proven.
- Dedicated Kafka → Spark → Iceberg smoke with checkpoint progress and recovery verification.
- Graceful Spark SIGINT and SIGTERM shutdown with query-before-Spark cleanup order.
- Graceful producer SIGINT and SIGTERM shutdown with bounded final Kafka flush.
- Runtime INFO logging for producer lifecycle and final-flush markers.
- Per-message Kafka delivery-result callback observation on the default `flush=True` path.
- Explicit `KafkaDeliveryError` for callback-reported failure or a missing callback result.
- Controlled real-Kafka delivery-result smoke.
- Automatic reconnect around complete Binance WebSocket sessions for classified connection and receive transport failures.
- Configured capped exponential reconnect backoff from 5 to 60 seconds, reset after successful publication in a recovered session.
- Controlled two-session reconnect smoke with Kafka offsets `0` and `1`, successful SIGTERM cleanup, and no third session.
- Reconnect lifecycle observability: incident-local attempt number, configured delay, retryable failure type, and monotonic disconnected duration through first successful recovery publication.
- Controlled reconnect observability smoke confirming the lifecycle markers and approximately five-second recovery timing.
- Focused unit coverage for bounded Kafka finalization.
- Unit-test CI with GitHub Actions.

In progress:

- None.

Planned:

- Shutdown-latency investigation.
- WebSocket close-timeout tuning or instrumentation.
- Shutdown-stage timing.
- Per-message flush timeout or removal.
- Delivery or undelivered-message logging and metrics.
- Second-`SIGINT` escalation behavior and bounded escalation policy.
- Broader Kafka delivery acknowledgement policy beyond the per-message callback result.
- Improve throughput.
- Persistent reconnect/delivery counters, aggregate shutdown summaries, periodic health reporting, and metrics export beyond the verified lifecycle markers.
- Producer container and deployment configuration.
- Gap detection, backfill, and deduplication strategy.
- Reconnect monitoring and alerting beyond the tested lifecycle logs.
- Additional stream validation, normalization, and data-quality checks.
- Silver transformation layer.
- Iceberg storage monitoring, maintenance, compaction, and schema-evolution work.
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
