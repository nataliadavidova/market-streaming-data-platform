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
- `KafkaProducerClient.flush(timeout=None)` returns the number of messages still queued after a flush attempt.
- `ConfluentKafkaProducerClient.flush()` preserves no-argument library behavior, while explicit flush timeouts are forwarded to the wrapped Confluent producer.
- `build_kafka_client(bootstrap_servers)` constructs the concrete Confluent Kafka client from explicit deployment configuration.
- `receive_and_publish_one_binance_trade(receiver, publisher)` performs one receive, one `KafkaMessage` preparation, and one synchronous publish.
- `run_binance_trade_publish_loop(receiver, publisher)` provides permanent sequential repetition over already-created dependencies.
- `run_binance_trade_publisher(config, publisher)` owns the Binance receiver-session lifecycle around the publish loop.
- `python -m jobs.producer.binance_producer` loads config, reads `KAFKA_BOOTSTRAP_SERVERS`, creates the Kafka client and `KafkaPublisher`, starts the Binance publisher runtime, finalizes the Kafka client in application assembly, and treats top-level `KeyboardInterrupt` as expected operator shutdown.
- Local Kafka runs through Docker Compose.
- Makefile commands support local Kafka up/down, topic creation, one synthetic producer smoke publish, and bounded consume-one checks.
- GitHub Actions CI runs `make test` on pull requests and pushes to `main`.

Implemented executable producer flow:

`environment/config -> concrete Kafka client -> KafkaPublisher -> Binance runtime runner -> reusable Binance receiver session -> permanent sequential loop -> per-event preparation and publication`

Current lifecycle ownership:

- `main()`: environment lookup, host default for Kafka bootstrap, default config path, `asyncio.run(...)`, and catching only top-level `KeyboardInterrupt`.
- `run_configured_binance_producer(...)`: config loading, concrete Kafka client construction, `KafkaPublisher` construction, invoking the Binance runtime, and finalizing the concrete Kafka client in a `finally` block.
- `run_binance_trade_publisher(...)`: Binance receiver-session lifecycle.
- Publish loop: permanent sequential repetition.
- Per-event operation: one receive and one publish.
- `KafkaPublisher`: topic selection, send, and current per-message flush behavior.

Current Kafka finalization contract:

- Generic Kafka adapter code does not own timeout policy.
- The executable application assembly owns the final Kafka flush because it creates the concrete client.
- Final application flush uses `FINAL_KAFKA_FLUSH_TIMEOUT_SECONDS = 5.0`.
- Zero remaining messages means finalization succeeded.
- Nonzero remaining messages raise `KafkaFinalizationError`.
- If runtime succeeds and finalization succeeds, the application returns normally.
- If runtime succeeds and messages remain queued, `KafkaFinalizationError` propagates.
- If runtime fails and finalization succeeds, the original runtime exception propagates.
- If runtime fails and messages remain queued, `KafkaFinalizationError` is outward and the runtime failure remains in normal Python exception context.
- If `flush` itself raises, that exception propagates through normal `finally` semantics.

Current operational semantics:

- One reusable Binance WebSocket connection is kept open during the producer runtime.
- Processing is sequential and preserves event order within the application path.
- Kafka publishing is synchronous.
- Per-message flush remains enabled. `KafkaPublisher.publish_message(..., flush=True)` still calls `client.flush()` with no explicit timeout after every message, and that return value remains ignored.
- Only the application-level final flush currently uses the 5.0-second timeout.
- Exceptions propagate naturally.
- On `SIGINT`, Python `asyncio.run(...)` cancels the main task, cancellation unwinds the Binance/WebSocket contexts, application assembly performs a final Kafka flush, `asyncio.run(...)` surfaces `KeyboardInterrupt`, and `main()` treats that top-level interruption as expected operator shutdown.
- Total process shutdown is not guaranteed within five seconds because Binance/WebSocket cleanup occurs before final Kafka finalization.
- The implemented shutdown path is not a complete production shutdown framework.

Manual checks completed:

- Local Kafka service starts and shuts down cleanly.
- Local `market.trades.raw` topic creation and describe checks have passed.
- Synthetic one-event producer smoke-check has published to local Kafka.
- Bounded local console consume-check has read the synthetic event from Kafka.
- Manual live one-shot Binance smoke-check has connected to Binance, received one real combined-stream trade message, parsed it into `TradeEvent`, and closed normally.
- Manual bounded Binance-to-Kafka smoke-check has run the executable producer, published one fresh real Binance `TradeEvent` to `market.trades.raw`, consumed it with a fresh latest-offset consumer group, and verified clean `SIGINT` process exit with no cancellation or `KeyboardInterrupt` traceback.

## Planned Target Architecture

Planned but not implemented:

- Retry and reconnect behavior.
- WebSocket close-timeout tuning or instrumentation.
- Shutdown-stage timing.
- Per-message flush timeout, per-message flush removal, and throughput optimization.
- SIGTERM handling and second-interrupt escalation behavior.
- Delivery acknowledgement handling.
- Producer container execution.
- Delivery or undelivered-message logging and metrics.
- Spark Structured Streaming ingestion from Kafka.
- Streaming normalization and data-quality checks.
- Iceberg table writes on S3-compatible storage.
- ClickHouse aggregate loading.
- Dashboard or analytical SQL layer.
- Production-like observability, consumer lag monitoring, and reliability features.

Do not treat planned Spark Streaming, Iceberg, ClickHouse, Debezium, observability, or reliability features as implemented.
