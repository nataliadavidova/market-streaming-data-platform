# Silver Serving MVP Design

## 1. Milestone goal

Add the smallest analytical serving slice after the completed local Bronze MVP:

```text
canonical Bronze Iceberg
→ bounded Spark Silver transformation
→ Silver Iceberg
→ bounded ClickHouse load
→ one Superset dashboard
```

The design is for the existing real Binance symbols `BTCUSDT`, `ETHUSDT`, and `SOLUSDT`. It does not change the live Bronze stream, Kafka, checkpoints, or the canonical Bronze contract.

Repository discovery found no existing Silver job or table, ClickHouse service/table/client, JDBC path, Superset configuration, dashboard, or Iceberg batch-read job. The current repository provides the Bronze Iceberg table, Iceberg-enabled Spark configuration, read-only inspector, Makefile service workflows, and deterministic Bronze tests; all serving components described here are new.

## 2. Dashboard questions and metric contracts

The first dashboard answers only:

1. What is the latest observed price for each symbol?
2. How many trades occurred over time?
3. What notional volume occurred over time?
4. How do activity and volume compare across the three symbols?
5. What ingestion latency was observed?

Metrics use valid Silver rows only:

| Metric | Definition |
| --- | --- |
| `trade_count` | `count(*)` of Silver rows |
| `notional` | `price * quantity` using exact decimal arithmetic |
| `notional_volume` | `sum(notional)` |
| `latest_price` | `price` from the greatest `event_time` in the selected symbol/time context; ties use the greatest Kafka partition and offset |
| `latency_ms` | `ingested_at_ms - event_time_ms` |
| `median_latency_ms` | median of `latency_ms` for the selected context; an average may be shown if the dashboard engine lacks a suitable median function |

The Bronze timestamp fields are Unix epoch milliseconds stored as `BIGINT`. Silver keeps the source millisecond values available during transformation and exposes `event_time` and `ingested_at` as millisecond-precision timestamps. `latency_ms` remains a signed `BIGINT`; negative values are retained for investigation rather than silently clipped.

Dashboard grains are raw trade rows in Silver and minute-level grouping in ClickHouse queries or Superset datasets. No permanent Gold aggregate table is introduced.

## 3. Proposed architecture

```text
Binance → Kafka → Spark quality-v2 → canonical Bronze Iceberg
                                      ↓ bounded batch read
                              Silver Spark transformation
                                      ↓ bounded load
                              silver_trades Iceberg
                                      ↓ repeatable load
                           market_analytics.silver_trades
                                      ↓ SQL datasets
                                   Superset dashboard
```

Bronze remains the complete audit and quality layer. Silver is the clean analytical layer: valid records only, derived notional and latency, and traceable Kafka coordinates. ClickHouse is a serving copy for dashboard queries, not the source of truth.

## 4. Minimal Silver contract

Create one Iceberg table:

```text
market_catalog.market.silver_trades
```

Proposed columns and types:

| Column | Type | Rule |
| --- | --- | --- |
| `exchange` | `STRING` | copied from Bronze |
| `symbol` | `STRING` | copied without normalization changes |
| `trade_id` | `STRING` | copied without changing business identity |
| `price` | `DECIMAL(38,18)` | copied exactly |
| `quantity` | `DECIMAL(38,18)` | copied exactly |
| `notional` | `DECIMAL(38,18)` | exact decimal `price * quantity`; overflow must fail visibly rather than use floating point |
| `event_time` | `TIMESTAMP` | `event_time_ms` converted from epoch milliseconds |
| `ingested_at` | `TIMESTAMP` | `ingested_at_ms` converted from epoch milliseconds |
| `latency_ms` | `BIGINT` | `ingested_at_ms - event_time_ms` |
| `kafka_topic` | `STRING` | copied for traceability |
| `kafka_partition` | `INT` | copied for traceability |
| `kafka_offset` | `BIGINT` | copied for traceability |

The bounded transformation reads canonical Bronze and applies exactly:

```text
is_valid = true
AND is_valid IS NOT NULL
```

Thus `false` rows and historical `NULL`/`NULL` rows are excluded. `raw_json`, `validation_errors`, and `kafka_key` are not carried initially because Silver is analytical; Kafka coordinates retain the link back to Bronze. No deduplication, grouping, identity rewrite, or silent correction is performed.

Spark decimal multiplication must remain decimal throughout. The implementation should make the resulting `notional` type explicit and test overflow behavior before any runtime load; it must never route through `DOUBLE`.

## 5. Serving alternatives

### A. Bounded Spark transformation and load — recommended

```text
Bronze Iceberg → bounded Spark → Silver Iceberg → bounded ClickHouse load
```

This keeps Silver as an inspectable source of truth, fits the current Spark/Iceberg repository, is repeatable on the small local dataset, and makes failures easy to rerun. Dashboard freshness is load-driven rather than continuous.

### B. Continuous Spark streaming

This offers fresher dashboards but introduces another long-running query, checkpoint, sink lifecycle, and restart contract before the Silver schema is proven. It is too much operational surface for the first serving slice.

### C. ClickHouse consuming Kafka directly

This bypasses Silver, duplicates parsing/quality semantics, and makes Kafka-to-serving correctness the primary contract. It would weaken the demonstrated Bronze→Silver architecture and complicate replay boundaries.

### D. ClickHouse reading Iceberg directly

This avoids a load job but couples dashboard availability to Iceberg object/catalog support and does not establish a clean serving-table contract. It is useful to investigate later, not the MVP default.

## 6. ClickHouse serving contract

Use one table:

```text
market_analytics.silver_trades
```

Proposed columns mirror Silver, with `Decimal(38,18)` for `price`, `quantity`, and `notional`, `DateTime64(3)` for `event_time` and `ingested_at`, `Int64` for `latency_ms` and `kafka_offset`, `Int32` for `kafka_partition`, and `String` for text fields. Keep Kafka coordinates in the serving table.

Use `MergeTree` with:

```text
ORDER BY (symbol, event_time, kafka_partition, kafka_offset)
```

At the current local scale, daily partitioning adds lifecycle complexity without a demonstrated benefit; do not partition the first table merely by convention.

For the first demo dataset, use a deterministic full rebuild: load Silver into a fresh or explicitly cleared demo table, then expose it to Superset. This avoids inventing a watermark store or a deduplication framework. The load command must make its rebuild boundary explicit and must never claim incremental or exactly-once behavior. A later incremental design can use the maximum processed Silver/Iceberg snapshot or Kafka coordinate only after measured requirements exist.

`ReplacingMergeTree` is not recommended now: it would introduce deduplication semantics that are explicitly out of scope and could obscure duplicate-source behavior.

## 7. Superset dashboard contract

Recommend Superset because it is the conventional lightweight SQL dashboard layer and no visualization layer exists in the repository. The first dashboard should fit one screen.

Filters:

- `symbol` (BTCUSDT, ETHUSDT, SOLUSDT)
- event-time range

KPI cards:

- trade count
- notional volume
- latest price
- median (or average) latency

Charts:

- price over time by symbol
- trade count per minute
- notional volume per minute by symbol
- latency over time or a latency distribution

Datasets should query `market_analytics.silver_trades` directly. No authentication, role model, alerting, scheduled reports, or production BI governance is part of this local milestone.

## 8. Recommended implementation sequence

1. Add deterministic Silver schema and a bounded Spark batch reader from canonical Bronze.
2. Test filtering, decimal notional, millisecond timestamp conversion, latency, and coordinate preservation with local Spark fixtures.
3. Add isolated local Iceberg Silver-table creation and repeatable full-rebuild behavior.
4. Add ClickHouse Compose/service configuration only when the Silver contract tests pass.
5. Add a bounded loader from Silver to `market_analytics.silver_trades` with explicit rebuild semantics.
6. Add read-only ClickHouse query checks for the three symbols and metric definitions.
7. Add Superset dataset/dashboard configuration and a concise local runbook.
8. Run one short real-data refresh using the existing Bronze rows, then document the supported refresh procedure.

The first implementation branch should stop after deterministic Silver tests and one bounded local Silver→ClickHouse validation if the infrastructure is available. Dashboard wiring follows the serving-table contract rather than driving it.

## 9. Acceptance criteria

The implemented milestone is complete when it proves:

1. `market_catalog.market.silver_trades` exists with the agreed schema.
2. Only valid Bronze rows are transformed; invalid and historical unevaluated rows are absent.
3. Decimal price, quantity, and notional calculations remain exact.
4. Epoch-millisecond conversion and `latency_ms` are correct.
5. Kafka topic/partition/offset coordinates remain traceable.
6. Silver data loads repeatedly into ClickHouse using the documented full-rebuild boundary.
7. ClickHouse queries return the expected BTCUSDT, ETHUSDT, and SOLUSDT dimensions.
8. Superset displays the four KPI cards and four compact charts with symbol/time filters.
9. A short subsequent real Binance run becomes visible after the supported refresh/load procedure.
10. The process is documented and reproducible locally.

## 10. Non-goals

- Gold tables or permanent aggregate tables.
- Continuous ClickHouse streaming sinks.
- Universal exactly-once, replay, or general deduplication frameworks.
- Historical backfill and repair workflows.
- Monitoring, alerting, or data-quality observability platforms.
- Cloud deployment, Kubernetes, Terraform, or multi-broker durability.
- Large-scale performance tuning.
- Production Superset authentication, roles, governance, or alerting.

## 11. Open questions requiring implementation evidence

- Does the selected Spark/Iceberg runtime preserve `DECIMAL(38,18)` for `price * quantity`, or must `notional` use a narrower documented precision after overflow tests?
- Which ClickHouse image/version and local Compose resource limits provide a stable, reproducible load without adding unnecessary services?
- Should the bounded loader replace the demo table or truncate its rows, and which operation is safest for the chosen ClickHouse engine?
- What exact Superset provisioning format is least maintenance-heavy for this repository: SQL dataset definitions, a small export, or manual setup instructions?
- What refresh duration and freshness target would justify a later incremental or continuous serving design?
