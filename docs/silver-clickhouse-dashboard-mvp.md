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

The deterministic Silver job and unit tests are implemented. ClickHouse service/table/client, JDBC path, Superset configuration, dashboard, and a bounded ClickHouse loader remain future work.

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
| `latest_price` | `price` from the maximum ordered identity `(event_time, kafka_partition, kafka_offset)` in the selected symbol/time context; ClickHouse may implement this with an equivalent deterministic `argMax` expression |
| `latency_ms` | `ingested_at_ms - event_time_ms` |
| `median_latency_ms` | median of `latency_ms` for the selected context; an average may be shown if the dashboard engine lacks a suitable median function |

The Bronze timestamp fields are Unix epoch milliseconds stored as `BIGINT`. `event_time` is the Binance event timestamp represented in UTC, and `ingested_at` is the producer ingestion timestamp represented in UTC. Spark must use an explicit UTC session/time-zone configuration and preserve millisecond precision. `latency_ms` is exactly `ingested_at_ms - event_time_ms`: source-to-ingestion latency, not end-to-end Kafka/Spark/Iceberg/ClickHouse processing latency. It remains signed; negative values are retained for clock or source anomalies and are never silently clamped.

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

Spark decimal multiplication must remain decimal throughout. The implementation must explicitly cast the result to the agreed `DECIMAL(38,18)` target, test both scale/precision and overflow behavior, and fail visibly if the value cannot be represented. It must never route through `DOUBLE` or silently reduce scale.

The current preserved Bronze evidence is 188 rows: 184 valid, 3 invalid, and 1 historical row with `is_valid IS NULL`. The first Silver build is expected to produce 184 rows, but this is a runtime expectation to verify, not a value hard-coded into production logic.

### Deterministic Silver materialization

Every bounded Silver run is a full rebuild, not an append:

```text
canonical Bronze
→ select is_valid = true
→ construct the complete Silver dataset
→ replace previous Silver state
```

The implementation uses Iceberg V2 `DataFrameWriterV2`:

```python
silver_df.writeTo(silver_table).using("iceberg").createOrReplace()
```

Runtime validation established replacement/overwrite behavior rather than append. Repeated builds over unchanged Bronze produced 184 rows and matching complete SHA-256 row-multiset fingerprints, with no accumulated duplicate append. This documents the behavior demonstrated by the current Spark/Iceberg stack; it does not claim universal atomic replacement beyond that evidence. A deterministic row serialization plus SHA-256 and occurrence counts is suitable; process-random Python `hash()` is not.

### Transport identity and Kafka source epochs

`(kafka_topic, kafka_partition, kafka_offset)` is unique only within one Kafka topic/source epoch. Recreated Kafka timelines can reuse a topic name and reset offsets. The preserved Bronze table contains valid rows from both the earlier quality-v1 timeline and the current persistent quality-v2 timeline, so reused coordinates are expected historical evidence rather than proof of duplicate trades.

Silver preserves these coordinates for audit and local traceability, but the current contract does not treat them as a globally unique primary key. Quality-v1 and quality-v2 rows may reuse offsets; known collisions at offset 0 (BTCUSDT/SOLUSDT) and offset 3 (BTCUSDT/ETHUSDT) remain as distinct rows. Rows must not be dropped solely because coordinates collide. A globally unique transport identity would require an additional `source_epoch` or topic-generation identifier; adding that field is deferred to replay/deduplication reliability work. No historical epoch is inferred from symbols, values, timestamps, snapshot order, or other heuristics.

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

Silver remains the authoritative analytical source of truth:

```text
market_catalog.market.silver_trades
```

ClickHouse is a reproducible serving copy only. The serving table is:

```text
market_analytics.silver_trades
```

The existing `market_data` value in `.env.example` is stale scaffolding and will be corrected in a later implementation or documentation slice. It is not the approved serving database name.

### Loading boundaries

Spark is the data plane:

- resolve and record one exact Iceberg `snapshot_id` for `market_catalog.market.silver_trades` at the beginning of the rebuild;
- read the complete Silver Iceberg snapshot identified by that recorded `snapshot_id`;
- write the complete DataFrame to ClickHouse staging through the official ClickHouse JDBC driver;
- read staging back through JDBC when needed for validation.

The Python `clickhouse-connect` client is the control plane:

- create the Atomic database and both tables;
- inspect ClickHouse schemas;
- truncate staging at the start of a rebuild;
- run validation queries;
- perform the atomic table exchange.

The complete Silver DataFrame must not be collected to the Python driver for insertion.

All source-side operations for one rebuild use the same recorded Silver
snapshot: full DataFrame read, schema validation, `NULL` validation, row
count, symbol set, per-symbol row counts, and the complete row-multiset
fingerprint. The staging table is compared with that recorded snapshot, not
with whatever snapshot is current under the Silver table name later in the
run. This prevents a concurrent Silver rebuild from producing a false
mismatch or a serving copy assembled against inconsistent source states.

For the MVP, concurrent Silver and ClickHouse rebuilds may additionally be
prohibited operationally, but that restriction does not replace the
`snapshot_id` contract. The exact Spark time-travel syntax for reading the
recorded snapshot remains deferred to the implementation slice.

### Exact ClickHouse schema

The serving and staging tables preserve all 12 Silver columns in the same order. The mapping is:

| Silver type | ClickHouse type |
| --- | --- |
| `STRING` | `String` |
| `DECIMAL(38,18)` | `Decimal(38,18)` |
| `TIMESTAMP` | `DateTime64(3, 'UTC')` |
| `BIGINT` | `Int64` |
| `INT` | `Int32` |

All target columns are non-nullable. The loader must validate that every required Silver column contains zero `NULL` values before loading or exchanging tables.

### Database and table design

The database and tables are:

```text
CREATE DATABASE market_analytics ENGINE = Atomic

market_analytics.silver_trades
market_analytics.silver_trades_staging
```

Both tables must have identical DDL. Both use `MergeTree` with monthly event-time partitioning:

```text
PARTITION BY toYYYYMM(event_time)
```

The ordering key is, in order:

```text
ORDER BY (
    exchange,
    symbol,
    event_time,
    trade_id,
    kafka_topic,
    kafka_partition,
    kafka_offset
)
```

The ordering key is not a uniqueness constraint. Ordinary `MergeTree` preserves duplicate rows. `ReplacingMergeTree` is rejected because the serving copy must preserve the complete Silver row multiset and must not perform implicit deduplication.

### Full-rebuild lifecycle

Each rebuild uses this exact sequence:

1. Resolve and record the source `snapshot_id` for `market_catalog.market.silver_trades`.
2. Ensure the `market_analytics` Atomic database exists.
3. Ensure `market_analytics.silver_trades` and `market_analytics.silver_trades_staging` exist with identical schemas.
4. Truncate staging while leaving the current serving target untouched.
5. Read the complete Silver snapshot identified by the recorded `snapshot_id`.
6. Write the complete snapshot to staging through Spark JDBC.
7. Validate staging against the pre-exchange contract using that same recorded snapshot.
8. Atomically exchange the tables:

   ```sql
   EXCHANGE TABLES
       market_analytics.silver_trades
   AND market_analytics.silver_trades_staging
   ```

9. After the exchange, the serving table contains the new snapshot and staging contains the previous serving snapshot.
10. The previous serving snapshot is available for rollback only until the next rebuild attempt begins.
11. At the start of the next rebuild attempt, staging is truncated.

Failures before `EXCHANGE` leave the serving target unchanged. If that attempt has begun, staging may be empty or partially loaded and the older rollback snapshot is no longer retained. Truncating and inserting directly into the serving table is rejected, as is a non-atomic multi-step rename. Retaining more than one historical serving snapshot or providing durable multi-version rollback is outside the MVP.

### Pre-exchange validation

Before exchange, require all of the following:

- exact expected schema and column order;
- zero `NULL` values in all 12 columns;
- Silver row count equal to staging row count;
- matching symbol sets;
- matching per-symbol row counts;
- a matching complete SHA-256 row-multiset fingerprint over all 12 columns after canonical normalization.

Both complete fingerprints use the same Spark canonical-normalization implementation:

- fingerprint A is computed from the recorded Silver snapshot;
- fingerprint B is computed from the staging table read back through JDBC.

The complete cross-system fingerprint comparison does not use a separate
ClickHouse-side fingerprint algorithm. `clickhouse-connect` may run bounded
operational validation queries such as schema inspection, row count, `NULL`
counts, symbol-set checks, and per-symbol counts, but Spark owns the shared
canonicalization used for the complete comparison. This prevents Decimal,
timestamp, string, and row-serialization rules from diverging between
engines. Canonical normalization must use the declared column order and
types, deterministic timestamp and decimal representations, explicit
separators, and a deterministic ordering of serialized rows before hashing.
The fingerprint must preserve duplicate multiplicity. Count comparison alone
is insufficient because different row contents can have the same count.

### Repeatability acceptance criteria

For unchanged Silver input:

- two complete ClickHouse rebuilds produce the same row count;
- two complete rebuilds produce the same full row-multiset fingerprint;
- the second rebuild does not accumulate duplicate rows;
- failed pre-exchange validation does not alter the serving table.

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

Datasets should query `market_analytics.silver_trades` directly. Dashboard freshness is refresh-based in this milestone, not continuous real-time serving. No authentication, role model, alerting, scheduled reports, or production BI governance is part of this local milestone.

## 8. Recommended implementation sequence

1. Add the approved ClickHouse database, identical target/staging DDL, and control-plane schema checks.
2. Add the bounded Spark JDBC data-plane load from the complete Silver snapshot.
3. Add pre-exchange row-count, symbol-set, per-symbol-count, null, schema, and row-multiset fingerprint validation.
4. Add the Atomic `EXCHANGE TABLES` full-rebuild lifecycle and failure-boundary checks.
5. Add read-only ClickHouse query checks for the three symbols and metric definitions.
6. Add Superset dataset/dashboard configuration and a concise local runbook.
7. Run one short real-data refresh using the existing Silver rows, then document the supported refresh procedure.

The first implementation branch should stop after deterministic Silver tests and one bounded local Silver→ClickHouse validation if the infrastructure is available. Dashboard wiring follows the serving-table contract rather than driving it.

## 9. Acceptance criteria

The implemented milestone is complete when it proves:

1. `market_catalog.market.silver_trades` exists with the agreed schema.
2. Only valid Bronze rows are transformed; invalid and historical unevaluated rows are absent; the current preserved dataset produces 184 rows.
3. Decimal price, quantity, and notional calculations remain exact.
4. Epoch-millisecond conversion and `latency_ms` are correct.
5. Kafka topic/partition/offset coordinates remain traceable.
6. Two consecutive Silver builds over unchanged Bronze produce the same complete multiset of Silver rows, including occurrence counts for exact duplicate rows, without append accumulation.
7. Silver data loads repeatedly into ClickHouse using the documented staging/full-rebuild boundary without duplication.
8. ClickHouse queries return the expected BTCUSDT, ETHUSDT, and SOLUSDT dimensions.
9. Superset displays the four KPI cards and four compact charts with symbol/time filters.
10. A short subsequent real Binance run becomes visible after the supported refresh/load procedure.
11. The process is documented and reproducible locally.

## 10. Non-goals

- Gold tables or permanent aggregate tables.
- Continuous ClickHouse streaming sinks.
- Universal exactly-once, replay, or general deduplication frameworks.
- Historical backfill and repair workflows.
- Monitoring, alerting, or data-quality observability platforms.
- Cloud deployment, Kubernetes, Terraform, or multi-broker durability.
- Large-scale performance tuning.
- Production Superset authentication, roles, governance, or alerting.

## 11. Explicitly deferred decisions

The following decisions remain outside this design slice:

- exact pinned ClickHouse image and version;
- Compose service and healthcheck;
- port and credential wiring;
- persistent volume configuration;
- JDBC and Python dependency versions;
- implementation module names;
- Makefile targets;
- dashboard implementation;
- incremental ClickHouse loading;
- incremental Silver loading;
- replay-aware `source_epoch` and global deduplication.

Superset provisioning format and refresh-duration targets also remain future dashboard and operational decisions. The approved serving boundary is bounded and refresh-based, not continuous.
