# Kafka Smoke Check

This runbook verifies the local Kafka producer slice with one synthetic `TradeEvent`.

## Purpose

Confirm that local Kafka can start, the `market.trades.raw` topic can be created, the existing synthetic producer can publish one event, and the bounded console consumer can read that event back.

This is a manual local integration check. It is not run in CI. It does not exercise the live Binance WebSocket path and does not prove continuous Binance-to-Kafka streaming.

For the next validation level after this synthetic check, use the [Binance-to-Kafka end-to-end smoke check](binance-kafka-e2e-smoke-check.md).

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

## Persistence boundary

Kafka broker state is stored in the named Compose volume `kafka_data`, mounted at `/var/lib/kafka/data` with `KAFKA_LOG_DIRS=/var/lib/kafka/data`. The normal lifecycle is intentionally:

```bash
make kafka-up
make kafka-down
make kafka-up
```

Do not use `docker compose down -v` for this workflow. A successful persistence check should retain the topic, partition offsets, and message after the second `make kafka-up`. The previous configuration wrote to container-local `/tmp/kafka-logs`; that state was lost when its container was removed and is not recoverable.

The controlled persistence proof used the isolated topic `market.kafka.persistence.probe.v2`: TopicId `F-WKZJefSoClnDoU6F9JRw`, partition `0`, end offset `1`, and message `persistence-probe-v2` survived two ordinary lifecycle cycles. This is local single-broker persistence evidence, not replication or disaster-recovery evidence.

The Spark quality-v1 checkpoint remains preserved and must not be reused with a newly created Kafka timeline. Any future Kafka reset or cutover must use a new versioned checkpoint/query epoch such as quality-v2.

## Delivery Callback Smoke

This is a separate producer-level check for the default delivery-result path. It does not replace the synthetic procedure above.

Use a fresh dedicated topic:

```text
market.trades.delivery-result-smoke
```

The controlled run used the real `KafkaMessage`, `KafkaPublisher`, and `ConfluentKafkaProducerClient` classes. Before publication, partition `0` had end offset `0`.

The harness called:

```python
publisher.publish_message(message)
```

with the default `flush=True` behavior and returned normally without `KafkaDeliveryError`. The message was:

```text
key: binance:BTCUSDT
value: {"exchange":"binance","symbol":"BTCUSDT","trade_id":"delivery-result-smoke-20260723-1","price":"1.00000000","quantity":"0.00100000","event_time_ms":1782777600000,"ingested_at_ms":1782777600500}
```

The record was read back exactly from Kafka. The final end offset was `1`, so the smoke added exactly one record at offset `0`.

Under the current publisher contract, the normal return demonstrates that librdkafka invoked the per-message delivery callback with no delivery error. It does not establish exactly-once delivery, no-loss behavior, or duplicate prevention.

Stop Kafka after collecting the evidence:

```bash
make kafka-down
```

## Troubleshooting Boundaries

- If `make kafka-up` fails, check Docker availability and Docker socket permissions.
- If topic creation or consume commands fail, check that the Kafka container is running and ready.
- If publish fails, check the local Kafka bootstrap listener at `localhost:9092`.
- This runbook uses a synthetic event only. It does not validate live Binance connectivity, continuous receive behavior, or live Binance-to-Kafka publication.
- The default `flush=True` publisher path now observes per-message delivery callbacks. The detailed real-Kafka callback smoke is recorded separately above.
- A callback result is distinct from local queue acceptance and final-flush `remaining=0`; none of these establishes end-to-end exactly-once delivery.
- Per-message `flush()` remains synchronous with no explicit timeout, its return value is not a queue policy, and `flush=False` is an unconfirmed compatibility path not used by production.
