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

The project is currently in the bootstrap phase.

Current Python package:

`jobs`

Current config file:

`config/market_symbols.yaml`

Current local service config:

- `docker-compose.yml`: defines one local Kafka service for development/testing. It uses `apache/kafka:4.0.0`, runs single-node Kafka in KRaft mode, exposes the host listener at `localhost:9092`, and exposes the internal Docker-network listener at `kafka:29092`. It does not use ZooKeeper and does not include a topic creation service yet.
- `docker compose config` has passed for the local Kafka service configuration.
- `docker compose up -d kafka` smoke-check has passed. Kafka started successfully, reached a running/ready/started state in logs, and `docker compose down` shut it down cleanly.
- Manual local Kafka topic setup/check has passed for `market.trades.raw`. The topic was created successfully and described with `PartitionCount: 1` and `ReplicationFactor: 1`.
- Makefile commands have been added and runtime-checked successfully for `kafka-up`, `kafka-down`, `kafka-create-topic`, and `kafka-describe-topic`.
- Makefile command `kafka-consume-one` has been added. It wraps the bounded console consumer check for `market.trades.raw`.
- Makefile command `kafka-smoke-publish-one` has been added. It wraps `python -m jobs.producer.smoke_publish_one`.
- The full local Kafka Makefile workflow runtime-check has passed: `make kafka-up`, `make kafka-create-topic`, `python -m jobs.producer.smoke_publish_one`, `make kafka-consume-one`, and `make kafka-down`. `make kafka-consume-one` read the expected smoke-test message with `trade_id` `smoke-test-1` and exited cleanly with exit code 0.
- The cleaner all-Makefile Kafka workflow runtime-check has passed: `make kafka-up`, `make kafka-create-topic`, `make kafka-smoke-publish-one`, `make kafka-consume-one`, and `make kafka-down`. `make kafka-smoke-publish-one` succeeded. `make kafka-consume-one` read the expected smoke-test message with `trade_id` `smoke-test-1` and exited cleanly with exit code 0.
- The first one-event producer runtime smoke-test against local Kafka has passed. `python -m jobs.producer.smoke_publish_one` succeeded and published one synthetic trade event to `market.trades.raw`.
- The manual bounded Kafka consume/check has passed. `kafka-console-consumer.sh` successfully read the smoke-test message from `market.trades.raw` with `trade_id` `smoke-test-1`, confirming the first small Kafka round-trip: Python producer → Kafka topic → console consumer.
- The Kafka producer synthetic smoke-check is documented in `docs/runbooks/kafka-smoke-check.md`.
- GitHub Actions CI is configured in `.github/workflows/ci.yml` and runs `make test` on pull requests and pushes to `main`.
- Kafka smoke-checks remain manual and are not part of CI yet.
- No topic-init service has been added yet.
- Binance combined trade-stream URL building is implemented, including URL construction from `ProducerConfig`.
- A single-message WebSocket receiver is implemented using `websockets>=15,<16`. It returns `ReceivedWebSocketMessage` with received text and a receive-boundary Unix epoch millisecond timestamp captured immediately after `recv(decode=True)`.
- Binance combined-stream message parsing is implemented. It extracts the `data` payload and delegates to the existing Binance trade parser.
- One-shot Binance receive-and-parse composition is implemented. `receive_one_binance_trade_event(config)` builds the URL, receives one WebSocket message, parses it, and returns a `TradeEvent`.
- The manual live one-shot Binance smoke-check has passed using the corrected timestamp API with `BTCUSDT`, `ETHUSDT`, and `SOLUSDT` configured. It verified the path from `config/market_symbols.yaml` through a real Binance WebSocket connection to one parsed `TradeEvent`, with normal one-shot connection close.
- The Binance one-shot smoke-check is documented in `docs/runbooks/binance-one-shot-smoke-check.md`.
- Documentation is split into `README.md`, `docs/architecture.md`, `docs/roadmap.md`, and smoke-check runbooks under `docs/runbooks/`.
- The long-lived Binance WebSocket connection has not been implemented yet.
- The continuous receive loop has not been implemented yet.
- Retry and reconnect behavior has not been implemented yet.
- Graceful shutdown has not been implemented yet.
- Live Binance-to-Kafka publication has not been implemented yet.
- No Python consumer/read check has been added yet.

Current producer modules:

- `jobs/producer/config.py`
- `jobs/producer/events.py`
- `jobs/producer/binance.py`
- `jobs/producer/kafka.py`
- `jobs/producer/publisher.py`
- `jobs/producer/confluent.py`
- `jobs/producer/smoke_publish_one.py`
- `jobs/producer/websocket.py`

Current implemented functions and models:

- `load_config(config_path)`: reads YAML config using PyYAML and returns a Python dictionary.
- `load_producer_config(config_path)`: reads and validates producer config using Pydantic models.
- `TradeEvent`: internal producer trade event contract. It validates `exchange`, `symbol`, `trade_id`, `price`, `quantity`, `event_time_ms`, and `ingested_at_ms`. `price` and `quantity` use `Decimal`.
- `TradeEvent.to_json_message()`: serializes a trade event to deterministic JSON for future Kafka publishing. `Decimal` values are preserved as JSON strings.
- `parse_binance_trade_message(raw_message, ingested_at_ms)`: parses a Binance trade message into `TradeEvent`. It maps `s` to `symbol`, `t` to `trade_id`, `p` to `price`, `q` to `quantity`, and `T` to `event_time_ms`.
- `build_binance_combined_trade_stream_url(symbols)`: builds the Binance combined `@trade` stream URL for configured symbols, preserving order and lowercasing stream names.
- `build_binance_combined_trade_stream_url_from_config(config)`: builds the Binance combined trade-stream URL from `ProducerConfig`.
- `ReceivedWebSocketMessage`: immutable one-message WebSocket result with `text` and `received_at_ms`.
- `receive_one_websocket_message(url, connect=None, clock=current_time_ms)`: opens one WebSocket connection, receives exactly one text message with `recv(decode=True)`, captures Unix epoch milliseconds immediately after receive, and returns `ReceivedWebSocketMessage`.
- `parse_binance_combined_trade_message(raw_message, ingested_at_ms)`: parses one Binance combined-stream JSON text message, extracts the `data` object, and delegates to `parse_binance_trade_message`.
- `receive_one_binance_trade_event(config, connect=None, clock=current_time_ms)`: one-shot Binance composition that builds the stream URL from `ProducerConfig`, receives one message, parses it, and returns a `TradeEvent`.
- `KafkaMessage`: typed key/value message contract for future Kafka publishing.
- `prepare_trade_event_kafka_message(event)`: prepares deterministic Kafka key/value payloads from a `TradeEvent` without connecting to Kafka. Kafka key example: `binance:BTCUSDT`. The value is `TradeEvent.to_json_message()`.
- `KafkaProducerClient`: protocol for injectable Kafka-like clients used by the publisher wrapper.
- `KafkaPublisher`: wrapper that publishes prepared `KafkaMessage` objects to a configured topic by UTF-8 encoding the key and value. It uses an injectable client, so unit tests do not require a real Kafka broker.
- `ConfluentKafkaProducerClient`: adapter that adapts `confluent_kafka.Producer` to the existing `KafkaProducerClient` protocol. `send(topic, key, value)` delegates to `Producer.produce(topic=topic, key=key, value=value)`, and `flush()` delegates to `Producer.flush()`.
- `build_synthetic_trade_event()`: builds one deterministic synthetic `TradeEvent` for local producer smoke testing.
- `publish_one_synthetic_trade_event(client, topic)`: publishes one synthetic trade event through an injectable Kafka producer client. The default topic is `market.trades.raw`.
- `build_local_kafka_client(bootstrap_servers)`: creates a `ConfluentKafkaProducerClient` for the local Kafka bootstrap server. The default bootstrap server is `localhost:9092`.

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

Next likely small step:

- Design and implement the smallest testable long-lived WebSocket receive primitive. It should open one connection and receive multiple messages without reconnecting for every event. Keep Binance parsing and Kafka publication outside that first primitive, and do not reuse the one-shot helper in a loop that opens a new connection for every message.

Current test suite:

- 63 unit tests cover raw config loading, valid producer config validation, invalid producer config validation, `TradeEvent` validation, `TradeEvent` JSON serialization, Binance URL construction, Binance trade parsing, Binance combined-message parsing, single-message WebSocket receiving with receive-boundary timestamps, one-shot Binance receive-and-parse composition, Kafka message contract preparation, Kafka publisher wrapper behavior, the Confluent Kafka producer adapter, and the one-event producer smoke publisher.
- `make test` passes locally.
