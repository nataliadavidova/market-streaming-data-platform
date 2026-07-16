# Architecture

This project is a portfolio Data Engineering project for a real-time market data platform.

## Project Goal

Build a production-style streaming data platform that ingests market trade data, publishes raw events to Kafka, processes them with streaming jobs, stores durable analytical data, and exposes query-ready outputs with basic data-quality checks.

The project is currently in the bootstrap phase. The implemented foundation focuses on producer contracts, Binance receive/parse behavior, local Kafka development workflow, an executable Binance-to-Kafka producer path, and unit-testable boundaries.

## Target Data Flow

Planned Version 1 flow:

`Market API/WebSocket -> Kafka -> Spark Structured Streaming -> Iceberg on S3-compatible storage -> ClickHouse -> dashboard + basic DQ checks`

This is the target architecture, not the current implementation state.

## Component Responsibilities

- Market API/WebSocket: source of live market trade messages.
- Python producer: reads market trade messages, validates/parses them into internal contracts, and publishes raw events to Kafka.
- Kafka: durable streaming buffer for raw market events.
- Spark Structured Streaming: reads Kafka, parses, validates, normalizes, and aggregates streaming data.
- Iceberg on S3-compatible storage: durable analytical table storage for normalized data.
- ClickHouse: low-latency analytical serving layer for aggregates and dashboard queries.
- Dashboard or SQL layer: user-facing analysis surface.
- Data-quality checks: basic validation for freshness, schema expectations, and event quality.

## Currently Implemented Foundation

Implemented:

- `config/market_symbols.yaml` contains Binance trade stream configuration for `BTCUSDT`, `ETHUSDT`, and `SOLUSDT`.
- `load_config(config_path)` reads YAML config.
- `load_producer_config(config_path)` validates producer config with Pydantic models.
- `TradeEvent` defines the internal producer trade event contract.
- Binance trade parsing maps Binance trade payloads into `TradeEvent`.
- Binance combined trade-stream URLs can be built directly from configured symbols or `ProducerConfig`.
- A reusable WebSocket receiver session opens one connection, supports repeated `receive()` calls, and captures Unix epoch milliseconds immediately after each `recv(decode=True)`.
- Binance combined-stream JSON messages can be parsed into `TradeEvent`.
- One-shot Binance receive-and-parse composition can return one real `TradeEvent`.
- A reusable Binance trade receiver session keeps one WebSocket connection open and returns parsed `TradeEvent` objects through repeated explicit receive calls.
- Kafka message preparation creates deterministic key/value payloads from `TradeEvent`.
- `KafkaPublisher` and `ConfluentKafkaProducerClient` provide injectable publishing boundaries.
- `build_kafka_client(bootstrap_servers)` constructs the concrete Confluent Kafka client from explicit deployment configuration.
- `receive_and_publish_one_binance_trade(receiver, publisher)` performs one receive, one `KafkaMessage` preparation, and one synchronous publish.
- `run_binance_trade_publish_loop(receiver, publisher)` provides permanent sequential repetition over already-created dependencies.
- `run_binance_trade_publisher(config, publisher)` owns the Binance receiver-session lifecycle around the publish loop.
- `python -m jobs.producer.binance_producer` loads config, reads `KAFKA_BOOTSTRAP_SERVERS`, creates the Kafka client and `KafkaPublisher`, and starts the Binance publisher runtime.
- Local Kafka runs through Docker Compose.
- Makefile commands support local Kafka up/down, topic creation, one synthetic producer smoke publish, and bounded consume-one checks.
- GitHub Actions CI runs `make test` on pull requests and pushes to `main`.

Implemented executable producer flow:

`environment/config -> concrete Kafka client -> KafkaPublisher -> Binance runtime runner -> reusable Binance receiver session -> permanent sequential loop -> per-event preparation and publication`

Current lifecycle ownership:

- `main()`: environment lookup, host default for Kafka bootstrap, default config path, and `asyncio.run(...)`.
- Configured application assembly: config loading, Kafka client construction, and `KafkaPublisher` construction.
- Runtime runner: Binance receiver-session lifecycle.
- Publish loop: permanent sequential repetition.
- Per-event operation: one receive and one publish.
- `KafkaPublisher`: topic selection, send, and current per-message flush behavior.

Current operational semantics:

- One reusable Binance WebSocket connection is kept open during the producer runtime.
- Processing is sequential and preserves event order within the application path.
- Kafka publishing is synchronous.
- Per-message flush remains enabled.
- Exceptions propagate naturally.
- External termination currently has no graceful-shutdown guarantee.

Manual checks completed:

- Local Kafka service starts and shuts down cleanly.
- Local `market.trades.raw` topic creation and describe checks have passed.
- Synthetic one-event producer smoke-check has published to local Kafka.
- Bounded local console consume-check has read the synthetic event from Kafka.
- Manual live one-shot Binance smoke-check has connected to Binance, received one real combined-stream trade message, parsed it into `TradeEvent`, and closed normally.
- Manual bounded Binance-to-Kafka smoke-check has run the executable producer, published one fresh real Binance `TradeEvent` to `market.trades.raw`, and consumed it with a fresh latest-offset consumer group.

## Planned Target Architecture

Planned but not implemented:

- Retry and reconnect behavior.
- Graceful shutdown for long-running producer execution.
- Delivery acknowledgement handling.
- Producer container execution.
- Throughput optimization and per-message flush removal.
- Spark Structured Streaming ingestion from Kafka.
- Streaming normalization and data-quality checks.
- Iceberg table writes on S3-compatible storage.
- ClickHouse aggregate loading.
- Dashboard or analytical SQL layer.
- Production-like observability, consumer lag monitoring, and reliability features.

Do not treat planned Spark Streaming, Iceberg, ClickHouse, Debezium, observability, or reliability features as implemented.
