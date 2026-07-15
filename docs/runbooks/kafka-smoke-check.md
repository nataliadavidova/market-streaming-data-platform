# Kafka Smoke Check

This runbook verifies the local Kafka producer slice with one synthetic `TradeEvent`.

## Purpose

Confirm that local Kafka can start, the `market.trades.raw` topic can be created, the existing synthetic producer can publish one event, and the bounded console consumer can read that event back.

This is a manual local integration check. It is not run in CI. It does not exercise the live Binance WebSocket path and does not prove continuous Binance-to-Kafka streaming.

## Prerequisites

- Docker is running.
- The project environment is active.
- Project dependencies are installed.
- The working tree is clean enough that manual smoke output is easy to reason about.

Install dependencies if needed:

```bash
python -m pip install -e ".[dev]"
```

## Procedure

Start Kafka:

```bash
make kafka-up
```

Create the topic if needed:

```bash
make kafka-create-topic
```

Optionally verify the topic:

```bash
make kafka-describe-topic
```

Publish one synthetic `TradeEvent`:

```bash
make kafka-smoke-publish-one
```

Consume one message from the beginning of `market.trades.raw`:

```bash
make kafka-consume-one
```

Shut Kafka down:

```bash
make kafka-down
```

## Expected Success Criteria

The consume output should include a JSON message with:

```text
"trade_id":"smoke-test-1"
```

It should also include:

```text
Processed a total of 1 messages
```

`make kafka-down` should shut the local Kafka service down cleanly.

## Troubleshooting Boundaries

- If `make kafka-up` fails, check Docker availability and Docker socket permissions.
- If topic creation or consume commands fail, check that the Kafka container is running and ready.
- If publish fails, check the local Kafka bootstrap listener at `localhost:9092`.
- This runbook uses a synthetic event only. It does not validate live Binance connectivity, continuous receive behavior, or live Binance-to-Kafka publication.
