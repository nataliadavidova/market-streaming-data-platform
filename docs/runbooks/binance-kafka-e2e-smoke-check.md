# Binance-to-Kafka End-to-End Smoke Check

This runbook verifies the executable producer path from a real Binance WebSocket message through Kafka to a bounded consumer.

## Purpose

Confirm that the executable producer can load the configured Binance stream, receive a fresh live trade, serialize it as the internal `TradeEvent` contract, publish it to `market.trades.raw`, and let a Kafka consumer read that fresh message.

This is different from:

- [Kafka smoke check](kafka-smoke-check.md), which uses a synthetic local `TradeEvent`.
- [Binance one-shot smoke check](binance-one-shot-smoke-check.md), which receives and parses one live Binance event without Kafka.

This is a manual operational check. It depends on Docker, local Kafka, network access, and Binance availability. It is not run in CI.

## Prerequisites

- The project Python environment is active.
- Project dependencies are installed.
- Docker and Docker Compose are available.
- The working directory is the repository root.
- Local Kafka ports required by `docker-compose.yml` are free.

Install dependencies if needed:

```bash
python -m pip install -e ".[dev]"
```

Current values:

- Executable: `python -m jobs.producer.binance_producer`
- Default config: `config/market_symbols.yaml`
- Kafka topic: `market.trades.raw`
- Host bootstrap: `localhost:9092`
- Docker-network bootstrap: `kafka:29092`
- Bootstrap environment variable: `KAFKA_BOOTSTRAP_SERVERS`

Configured symbols:

- `BTCUSDT`
- `ETHUSDT`
- `SOLUSDT`

## Kafka Setup

Start Kafka:

```bash
make kafka-up
```

Create the topic if needed:

```bash
make kafka-create-topic
```

The current topic creation workflow uses `--if-not-exists`, so it is intended to be safe when `market.trades.raw` already exists. Do not delete the topic for this check.

Verify the topic:

```bash
make kafka-describe-topic
```

The local topic currently uses one partition and one replica.

## Fresh-Message Consumer

Start the consumer before starting the producer. Use a unique consumer group for every smoke run, start at the latest offset, consume at most one message, and use a bounded timeout.

Reading from the earliest offset can produce a false positive by returning an old synthetic event or an earlier live event.

Choose a unique group:

```bash
GROUP_ID="binance-entrypoint-smoke-$(date +%s)"
```

Start the bounded consumer:

```bash
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic market.trades.raw \
  --group "$GROUP_ID" \
  --consumer-property auto.offset.reset=latest \
  --max-messages 1 \
  --timeout-ms 30000 \
  --property print.key=true \
  --property "key.separator=	"
```

The `key.separator` value above is a literal tab inside the quoted string. In shells that support ANSI-C quoting, this equivalent form is also acceptable:

```bash
--property key.separator=$'\t'
```

## Producer Start

From the host environment, run the executable producer:

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
python -m jobs.producer.binance_producer
```

The producer is a permanent process. It does not exit after one message. After the bounded consumer receives one fresh message, stop the producer externally.

An external `SIGINT` may produce `CancelledError`, `KeyboardInterrupt`, and a non-zero process status. That distinguishes a successful data-path smoke-check from graceful shutdown behavior, which is not implemented yet.

Do not require GNU `timeout` for this check on macOS. Use a terminal, shell job control, or a small local orchestration command to bound the producer externally.

## Validation

Expect one key and one JSON value.

Key contract:

```text
exchange:symbol
```

Value contract:

- `exchange`
- `symbol`
- `trade_id`
- `price`
- `quantity`
- `event_time_ms`
- `ingested_at_ms`

Validate that:

- `exchange` is `binance`.
- `symbol` is one of the configured symbols.
- `trade_id` is present.
- `price` and `quantity` are present and parseable.
- `price` and `quantity` are serialized as strings to preserve decimal representation.
- `event_time_ms` and `ingested_at_ms` are integer milliseconds.
- The JSON shape matches `TradeEvent.to_json_message()`.

The observed `ingested_at_ms - event_time_ms` difference is only an observation. Do not treat it as a stable latency assertion because Binance and local system clocks may differ.

## Successful Example

One successful bounded smoke-check used group `binance-entrypoint-smoke-1784202344` and consumed:

```text
binance:BTCUSDT	{"exchange":"binance","symbol":"BTCUSDT","trade_id":"6510458765","price":"64262.09000000","quantity":"0.00085000","event_time_ms":1784202348710,"ingested_at_ms":1784202348660}
```

The consumer exited with status `0` after one message. The producer was externally stopped with `SIGINT`; that subprocess run exited with status `-2` and showed `CancelledError` followed by `KeyboardInterrupt`.

The live symbol, trade ID, price, quantity, timestamps, symbol sequence, and timestamp difference are variable. This is operational evidence, not an automated test expectation.

## Cleanup

Stop the producer and confirm the bounded consumer has exited.

Shut Kafka down:

```bash
make kafka-down
```

Confirm no project containers are running:

```bash
docker compose ps
```

Confirm no producer or console-consumer process remains using local process inspection appropriate for your environment.

Confirm the repository is unchanged:

```bash
git status --short
```

## Troubleshooting Boundaries

- Environment or dependency import failures.
- `config/market_symbols.yaml` loading or validation failures.
- Kafka startup, listener, or topic setup failures.
- Consumer subscription, unique group, or offset-policy mistakes.
- Binance DNS, TLS, WebSocket handshake, receive, or availability failures.
- Binance combined-message parsing failures.
- Kafka serialization, send, or flush failures.
- External timeout or process orchestration failures.
- Cleanup failures.

## Current Limitations

- No graceful shutdown or final flush policy.
- No retry or reconnect behavior.
- No delivery callback acknowledgement handling.
- Per-message flush remains enabled.
- No high-throughput validation.
- No containerized producer execution.
