# Architecture

This project is a portfolio Data Engineering project for a real-time market data platform.

## Project Goal

Build a production-style streaming data platform that ingests market trade data, publishes raw events to Kafka, processes them with streaming jobs, stores durable analytical data, and exposes query-ready outputs with basic data-quality checks.

The project remains in the Version 1 bootstrap phase, but the first live ingestion slice is implemented and smoke-tested end to end.

## Current and Target Data Flow

Current verified ingestion flow:

`Binance WebSocket -> production Binance producer -> Kafka -> Spark Structured Streaming -> typed Bronze parser -> Iceberg Bronze table -> Parquet/metadata in MinIO`

Spark processing progress is persisted separately:

`Spark checkpoint -> Hadoop S3A -> MinIO`

The broader target remains:

`... -> Iceberg -> ClickHouse -> dashboard + basic DQ checks`

ClickHouse and dashboard serving are not part of the current implementation.

## Component Responsibilities

- Market API/WebSocket: source of live market trade messages.
- Python producer: reads market trade messages, validates/parses them into internal contracts, and publishes raw events to Kafka.
- Kafka: durable streaming buffer for raw market events.
- Spark Structured Streaming: reads Kafka, parses, validates, and normalizes the typed Bronze contract.
- Iceberg on S3-compatible storage: durable Bronze table storage with snapshots, manifests, and metadata.
- REST catalog + S3FileIO: resolves Iceberg metadata and reads/writes table objects in S3-compatible storage.
- Hadoop S3A: stores Spark checkpoint objects independently from Iceberg table metadata.
- MinIO: local S3-compatible storage for Iceberg data, metadata, and Spark checkpoints.
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
- `run_binance_trade_publisher(...)` owns an outer reconnect loop around complete WebSocket sessions. Classified connection-establishment and receive transport failures exit the current context before cancellation-aware backoff and the next session.
- Backoff is exponential from the configured 5-second initial delay, capped at 60 seconds, and resets only after the first successfully published trade in a recovered session.
- The reconnect owner keeps incident-local `reconnect_attempt` and `disconnected_since` state. Each retryable failure logs `BINANCE_RECONNECT_ATTEMPT` with the attempt, unchanged delay, and failure type; after the first successful Kafka publication in the recovered session it logs `BINANCE_RECONNECT_RECOVERED` with the attempt and monotonic disconnected duration, then resets the state.
- Parser, configuration, programming, and Kafka publication failures are fail-fast; cancellation during receive or backoff propagates through normal cleanup.
- Kafka message preparation creates deterministic key/value payloads from `TradeEvent`.
- `KafkaPublisher` and `ConfluentKafkaProducerClient` provide injectable publishing boundaries.
- `KafkaProducerClient.flush(timeout=None)` returns the number of messages still queued after a flush attempt.
- `ConfluentKafkaProducerClient.flush()` preserves no-argument library behavior, while explicit flush timeouts are forwarded to the wrapped Confluent producer.
- `build_kafka_client(bootstrap_servers)` constructs the concrete Confluent Kafka client from explicit deployment configuration.
- `receive_and_publish_one_binance_trade(receiver, publisher)` performs one receive, one `KafkaMessage` preparation, and one synchronous publish.
- `run_binance_trade_publish_loop(receiver, publisher)` provides permanent sequential repetition over already-created dependencies.
- `run_binance_trade_publisher(config, publisher)` owns the Binance receiver-session lifecycle around the publish loop.
- `python -m jobs.producer.binance_producer` loads config, reads `KAFKA_BOOTSTRAP_SERVERS`, creates the Kafka client and `KafkaPublisher`, starts the Binance publisher runtime, finalizes the Kafka client in application assembly, and treats top-level `KeyboardInterrupt` as expected operator shutdown.
- The producer accepts `--topic` with precedence `--topic -> KAFKA_TOPIC_TRADES_RAW -> config.kafka.raw_topic`; the override is applied through an immutable config copy.
- Local Kafka runs through Docker Compose.
- Makefile commands support local Kafka up/down, topic creation, one synthetic producer smoke publish, and bounded consume-one checks.
- `make iceberg-trade-stream` runs the Spark Kafka source, typed Bronze parser, native Iceberg streaming sink, and query-specific S3A checkpoint.
- A dedicated Kafka topic, Iceberg table, and checkpoint are required for runtime smoke tests; production Bronze is not a smoke target.
- GitHub Actions CI runs `make test` on pull requests and pushes to `main`.

Implemented executable producer flow:

`environment/config -> concrete Kafka client -> KafkaPublisher -> Binance runtime runner -> reusable Binance receiver session -> permanent sequential loop -> per-event preparation and publication`

Current lifecycle ownership:

- `main()`: environment/CLI lookup, host default for Kafka bootstrap, default config path, runtime logging setup under the executable guard, `asyncio.run(...)`, and handling expected top-level `KeyboardInterrupt`.
- `run_configured_binance_producer(...)`: config loading, immutable topic override, concrete Kafka client construction, `KafkaPublisher` construction, SIGTERM lifecycle, invoking the Binance runtime, and finalizing the concrete Kafka client.
- `run_binance_trade_publisher(...)`: Binance receiver-session lifecycle.
- Publish loop: permanent sequential repetition.
- Per-event operation: one receive and one publish.
- `KafkaPublisher`: topic selection, send, per-message delivery-result observation, and current per-message flush behavior.

Current Kafka publication boundary:

- `ConfluentKafkaProducerClient.send()` calls `Producer.produce()`; a successful return means local librdkafka queue acceptance, not delivery confirmation.
- On the default `KafkaPublisher.publish_message(..., flush=True)` path, one local delivery-result state and callback belong to that publication only.
- The callback is processed during the existing synchronous `client.flush()` call. It records the result and does not raise into librdkafka.
- After flush, `KafkaPublisher` returns only for an observed callback with `error is None`; a callback error or missing callback result raises `KafkaDeliveryError`.
- A Kafka delivery failure propagates through the publisher and remains outside the Binance WebSocket reconnect boundary.
- `flush=False` remains an unconfirmed enqueue-style compatibility path and is not used by production.

Current Kafka finalization contract:

- Generic Kafka adapter code does not own timeout policy.
- The executable application assembly owns the final Kafka flush because it creates the concrete client.
- Final application flush uses `FINAL_KAFKA_FLUSH_TIMEOUT_SECONDS = 5.0`.
- Zero remaining messages means the final application queue was empty after finalization; it is not a per-message callback or exactly-once guarantee.
- Nonzero remaining messages raise `KafkaFinalizationError`.
- If runtime succeeds and finalization succeeds, the application returns normally.
- If runtime succeeds and messages remain queued, `KafkaFinalizationError` propagates.
- If runtime fails and finalization succeeds, the original runtime exception propagates.
- If runtime fails and messages remain queued, `KafkaFinalizationError` is outward and the runtime failure remains in normal Python exception context.
- If `flush` itself raises, that exception propagates through normal `finally` semantics.

Current operational semantics:

- Each active Binance session keeps one WebSocket connection open; the outer producer runtime replaces a session after a classified transport failure.
- Reconnect restores the live session only. It does not replay or backfill trades missed while disconnected.
- A WebSocket opening is not the recovery boundary: recovery is declared only after the recovered session publishes its first trade successfully to Kafka. Attempt numbers and timing are incident-local process state, not durable metrics.
- The monotonic clock measures disconnected duration only; the existing injected async sleep controls backoff scheduling. Lifecycle markers contain no trade payloads or Kafka values.
- Processing is sequential and preserves event order within the application path.
- Kafka publishing is synchronous.
- Per-message flush remains enabled. `KafkaPublisher.publish_message(..., flush=True)` calls `client.flush()` with no explicit timeout after every message, then inspects the per-message callback result; the flush return value remains ignored as a queue policy.
- This synchronous no-timeout flush limits batching and throughput; polling, batching, backpressure, and flush redesign remain future work.
- Only the application-level final flush currently uses the 5.0-second timeout.
- Exceptions propagate naturally.
- On `SIGINT`, Python `asyncio.run(...)` cancels the main task, cancellation unwinds the Binance/WebSocket contexts, application assembly performs a final Kafka flush, `asyncio.run(...)` surfaces `KeyboardInterrupt`, and `main()` treats that top-level interruption as expected operator shutdown.
- On `SIGTERM`, an asyncio loop callback records the request and cancels the main task without calling WebSocket or Kafka code directly. The WebSocket context exits before the bounded final Kafka flush; successful handled SIGTERM returns normally.
- Runtime INFO markers expose producer shutdown request, final flush start/result/success/failure, and completed shutdown.
- Total process shutdown is not guaranteed within five seconds because Binance/WebSocket cleanup occurs before final Kafka finalization.
- The implemented shutdown path is not a complete production shutdown framework.

Manual checks completed:

- Local Kafka service starts and shuts down cleanly.
- Local `market.trades.raw` topic creation and describe checks have passed.
- Synthetic one-event producer smoke-check has published to local Kafka.
- Bounded local console consume-check has read the synthetic event from Kafka.
- Manual live one-shot Binance smoke-check has connected to Binance, received one real combined-stream trade message, parsed it into `TradeEvent`, and closed normally.
- Manual bounded Binance-to-Kafka smoke-check has run the executable producer, published fresh real Binance `TradeEvent` records, consumed them with a fresh latest-offset consumer group, and verified clean producer shutdown.
- Controlled local two-session reconnect smoke-check has published trades at Kafka offsets `0` and `1`, observed the second connection after `5.005s`, logged recovery after successful publication, handled SIGTERM with final flush `remaining=0`, exited `0`, and opened no third session.
- Reconnect observability smoke-check has emitted `BINANCE_RECONNECT_ATTEMPT attempt=1 delay_seconds=5.0 failure_type=ConnectionClosedOK` and `BINANCE_RECONNECT_RECOVERED attempt=1 recovery_after_seconds=5.024`; the external close-to-session-2 delay was `5.004438s`.
- Controlled local-Kafka delivery-result smoke-check has returned successfully from the default publisher path after callback success and read back the exact published key/value with one new record.
- Dedicated Binance -> Kafka -> Spark -> Iceberg smoke-checks have verified Bronze writes, S3A checkpoint progress, checkpoint recovery, and clean application-level Spark SIGINT/SIGTERM shutdown.

## Planned Target Architecture

Planned but not implemented:

- Replay, backfill, and gap recovery for trades missed during a WebSocket outage.
- WebSocket close-timeout tuning or instrumentation.
- Shutdown-stage timing.
- Per-message flush timeout, per-message flush removal, and throughput optimization.
- Second-interrupt escalation behavior and bounded escalation policy.
- Broader delivery acknowledgement policy beyond the per-message callback result.
- Producer container execution.
- Delivery or undelivered-message logging and metrics.
- Persistent reconnect counters, aggregate shutdown summaries, periodic health reporting, metrics export, dashboards, and alerts.
- ClickHouse aggregate loading.
- Dashboard or analytical SQL layer.
- Production-like observability, consumer lag monitoring, and reliability features.

Do not treat planned ClickHouse, dashboard, Debezium, observability, or reliability features as implemented. The current Spark/Kafka parser, Iceberg sink, S3A checkpoint, and tested graceful shutdown slices are implemented, but they do not provide universal exactly-once or crash-safety guarantees.
