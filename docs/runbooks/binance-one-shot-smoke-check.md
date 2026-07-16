# Binance One-Shot Smoke Check

This runbook verifies the one-shot Binance WebSocket receive-and-parse path with one live trade event.

## Purpose

Confirm that the project can load the real producer config, build the Binance combined trade-stream URL, open a real WebSocket connection, receive exactly one text message, capture `ingested_at_ms` immediately after `recv()` returns, parse the Binance combined-stream message, return one `TradeEvent`, and close the one-shot connection.

This is a manual external integration check. It depends on network and Binance availability and is not run in CI.

For the next validation level after this receive-only check, use the [Binance-to-Kafka end-to-end smoke check](binance-kafka-e2e-smoke-check.md).

## Prerequisites

- The project environment is active.
- Project dependencies are installed.
- Network access to Binance WebSocket endpoints is available.
- `websockets>=15,<16` is installed.

Activate the project environment:

```bash
conda activate market-streaming
```

Install dependencies if needed:

```bash
python -m pip install -e ".[dev]"
```

The configuration source is `config/market_symbols.yaml`.

Configured symbols:

- `BTCUSDT`
- `ETHUSDT`
- `SOLUSDT`

## Procedure

Run the bounded one-shot check:

```bash
python - <<'PY'
import asyncio

from jobs.producer.binance import receive_one_binance_trade_event
from jobs.producer.config import load_producer_config


async def main() -> None:
    config = load_producer_config("config/market_symbols.yaml")
    event = await asyncio.wait_for(
        receive_one_binance_trade_event(config),
        timeout=20,
    )
    print(event.to_json_message())


asyncio.run(main())
PY
```

The timeout is intentionally outside project code so this manual command cannot wait indefinitely.

## Expected Success Criteria

The command should print one serialized `TradeEvent` JSON object.

Expected fields include:

- `exchange`
- `symbol`
- `trade_id`
- `price`
- `quantity`
- `event_time_ms`
- `ingested_at_ms`

Live symbol, trade ID, price, quantity, timestamps, and observed timing vary. Do not use fixed market values as expected output.

`ingested_at_ms` is the local Unix epoch millisecond timestamp captured immediately after WebSocket `recv()` returns. Do not assert that `ingested_at_ms >= event_time_ms`; Binance and local system clocks may differ.

The one-shot helper receives exactly one event and then closes the connection.

## Cleanup Behavior

No local service cleanup is required. The WebSocket connection context closes after one received message.

## Troubleshooting Boundaries

- If import fails, check that dependencies are installed in the active environment.
- If configuration loading fails, check `config/market_symbols.yaml`.
- If connection or receive fails, check network access, DNS/TLS behavior, and Binance availability.
- If parsing fails, inspect whether Binance changed the combined-stream envelope or trade payload shape.
- The executable Binance-to-Kafka path is covered separately by the end-to-end smoke runbook.
- Broader shutdown hardening, retry/reconnect, delivery acknowledgement handling, and throughput optimization are not implemented yet.
