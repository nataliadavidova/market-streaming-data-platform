# Silver-to-ClickHouse Serving Contract

## 1. Milestone goal

This document records the implemented Silver-to-ClickHouse serving contract:

```text
canonical Bronze Iceberg
→ bounded Spark Silver transformation
→ Silver Iceberg
→ bounded ClickHouse load
→ Metabase through read-only `bi_reader`
```

The design is for the existing real Binance symbols `BTCUSDT`, `ETHUSDT`, and `SOLUSDT`. It does not change the live Bronze stream, Kafka, checkpoints, or the canonical Bronze contract.

The deterministic Silver job, ClickHouse service/table/client, bounded JDBC loader, validation gates, atomic publication, and read-only BI access are implemented. The operational command and final acceptance evidence are maintained in the [ClickHouse serving refresh runbook](runbooks/clickhouse-serving-refresh.md).

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

Dashboard grains are raw trade rows in Silver and minute-level grouping in ClickHouse queries or BI datasets. No permanent Gold aggregate table is introduced.

## 3. Implemented architecture

```text
Binance → Kafka → Spark quality-v2 → canonical Bronze Iceberg
                                      ↓ bounded batch read
                              Silver Spark transformation
                                      ↓ bounded load
                              silver_trades Iceberg
                                      ↓ repeatable load
                           market_analytics.silver_trades
                                      ↓ SQL datasets
                                   Metabase via bi_reader
```

Bronze remains the complete audit and quality layer. Silver is the clean analytical layer: valid records only, derived notional and latency, and traceable Kafka coordinates. ClickHouse is a reproducible serving copy for BI queries, not the source of truth.

## 4. Minimal Silver contract

Create one Iceberg table:

```text
market_catalog.market.silver_trades
```

Columns and types:

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

An earlier preserved Bronze baseline contained 188 rows: 184 valid, 3 invalid, and 1 historical row with `is_valid IS NULL`. Its initial Silver result is historical validation evidence, not a current dataset size or production constant.

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

Runtime validation established replacement/overwrite behavior rather than append. Repeated builds over unchanged Bronze produced matching complete SHA-256 row-multiset fingerprints, with no accumulated duplicate append. This documents the behavior demonstrated by the current Spark/Iceberg stack; it does not claim universal atomic replacement beyond that evidence. A deterministic row serialization plus SHA-256 and occurrence counts is suitable; process-random Python `hash()` is not.

### Transport identity and Kafka source epochs

`(kafka_topic, kafka_partition, kafka_offset)` is unique only within one Kafka topic/source epoch. Recreated Kafka timelines can reuse a topic name and reset offsets. The preserved Bronze table contains valid rows from both the earlier quality-v1 timeline and the current persistent quality-v2 timeline, so reused coordinates are expected historical evidence rather than proof of duplicate trades.

Silver preserves these coordinates for audit and local traceability, but the current contract does not treat them as a globally unique primary key. Quality-v1 and quality-v2 rows may reuse offsets; known collisions at offset 0 (BTCUSDT/SOLUSDT) and offset 3 (BTCUSDT/ETHUSDT) remain as distinct rows. Rows must not be dropped solely because coordinates collide. A globally unique transport identity would require an additional `source_epoch` or topic-generation identifier; adding that field is deferred to replay/deduplication reliability work. No historical epoch is inferred from symbols, values, timestamps, snapshot order, or other heuristics.

## 5. Serving alternatives

### A. Bounded Spark transformation and load — recommended

```text
Bronze Iceberg → bounded Spark → Silver Iceberg → bounded ClickHouse load
```

This keeps Silver as an inspectable source of truth, fits the current Spark/Iceberg repository, is repeatable on the local dataset, and makes failures bounded to one publication attempt. Dashboard freshness is load-driven rather than continuous.

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

For the MVP, concurrent Silver and ClickHouse rebuilds are prohibited operationally, but that restriction does not replace the `snapshot_id` contract. The implementation reads the recorded snapshot through the existing bounded Silver source path.

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

9. After the atomic exchange, the serving table contains the new snapshot and staging contains the previously published target as a consequence of the swap.
10. The current workflow does not implement automatic rollback.
11. At the start of the next rebuild attempt, staging is truncated.

Failures before `EXCHANGE` leave the serving target unchanged. If that attempt has begun, staging may be empty or partially loaded. The current workflow does not implement automatic rollback orchestration. Truncating and inserting directly into the serving table is rejected, as is a non-atomic multi-step rename. Retaining more than one historical serving snapshot or providing durable multi-version rollback is outside the MVP.

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

## 7. ClickHouse infrastructure contract

### Image and architecture

Use the official pinned image:

```text
clickhouse/clickhouse-server:26.3.17.56
```

`26.3.17.56` is the approved ClickHouse release version. The full
version tag is substantially more stable and reproducible than moving
aliases such as `latest`, `lts`, `26`, or `26.3`, which are rejected.
Docker tags are registry references and are not technically immutable;
the version tag identifies the selected software release, not a
byte-identical artifact identity. The unsuffixed standard image is
preferred for local development; Alpine and distroless variants are
outside the MVP.

For byte-identical image reproducibility, the Compose image reference uses
both the readable version tag and the verified multi-platform manifest-index
digest:

```text
clickhouse/clickhouse-server:26.3.17.56@sha256:<manifest-index-digest>
```

The digest is the immutable artifact identity. It identifies the
multi-platform manifest index;
an amd64-only or arm64-only child-image digest must not be used. A
platform-specific child digest would break the native cross-architecture
contract. Any later change to the ClickHouse version or pinned digest
requires an explicit reviewed repository change.

The resolved manifest must support both native architectures:

- `linux/amd64`
- `linux/arm64`

Compose must select the native image architecture from the
multi-platform manifest. Do not configure `platform: linux/amd64`; native
Apple Silicon development must not require x86 emulation.

The verified Compose configuration uses the exact `26.3.17.56` release,
the multi-platform manifest-index digest, and native platform selection
without `platform: linux/amd64`.

### Compose service identity and network

The service name is:

```text
clickhouse
```

No explicit `container_name` is required. Other Compose services and
containerized jobs address it through Docker DNS as `clickhouse`. The
service joins the existing project Compose network; it must not create an
isolated ClickHouse-only network.

### Ports and connection boundaries

The container exposes:

- HTTP: `8123`
- native: `9000`

The default host mappings are:

```text
${CLICKHOUSE_HTTP_PORT:-18123}:8123
${CLICKHOUSE_NATIVE_PORT:-19000}:9000
```

From the host, clients use:

```text
HTTP:   localhost:${CLICKHOUSE_HTTP_PORT}
native: localhost:${CLICKHOUSE_NATIVE_PORT}
```

From another Compose service, clients use:

```text
HTTP:   clickhouse:8123
native: clickhouse:9000
```

Host port `19000` avoids collision with MinIO host port `9000`. Host
networking is not used.

### User and credentials

The local MVP uses one technical user:

```text
market_loader
```

The configured `CLICKHOUSE_USER` and `CLICKHOUSE_PASSWORD` values are
environment-backed and are shared by the `clickhouse-connect` control
plane and Spark JDBC data plane. For the local MVP, `CLICKHOUSE_USER`
resolves to `market_loader`. Credentials must not be hardcoded in
`docker-compose.yml`. `CLICKHOUSE_SKIP_USER_SETUP=1` is not enabled.

The committed `.env.example` contains safe development examples, while real
credentials remain outside Git. The existing `bi_reader` account provides
read-only access to the published serving target for BI queries.

### Database ownership

Application configuration uses:

```text
CLICKHOUSE_DATABASE=market_analytics
```

The implementation does not rely on `CLICKHOUSE_DB` container bootstrap to
establish the database contract. The Python control plane explicitly runs:

```sql
CREATE DATABASE IF NOT EXISTS market_analytics
ENGINE = Atomic
```

Explicit creation keeps the Atomic engine visible and testable rather than
depending on implicit image initialization.

### Persistent storage

Use one named Compose volume:

```text
clickhouse-data:/var/lib/clickhouse
```

Ordinary container recreation preserves serving data. The volume may be
explicitly removed during a destructive reset. ClickHouse remains a
reproducible serving copy even though the local volume is persistent, and
Silver Iceberg remains the source of truth.

Do not add a persistent volume for `/var/log/clickhouse-server`, and do
not use a host bind mount for ClickHouse data in the MVP.

### Healthcheck and file descriptors

The service healthcheck must run a real authenticated query using the
bundled `clickhouse-client`:

```sql
SELECT 1
```

The healthcheck must use `CLICKHOUSE_USER` and `CLICKHOUSE_PASSWORD` and
prove query readiness, not merely process existence or an open TCP port.
Its timing contract is:

```text
interval:      5s
timeout:       3s
retries:       20
start_period: 10s
```

The Compose healthcheck uses the environment-backed credentials and has been
validated with the local service configuration.

The service configures the official image's recommended file-descriptor
limit:

```text
nofile:
  soft: 262144
  hard: 262144
```

No additional Linux capabilities are required for the MVP.

### Lifecycle

Infrastructure lifecycle remains explicit. The Makefile provides bounded
operations for starting ClickHouse, waiting for healthy status, inspecting
status, and stopping it without deleting data. The MVP does not configure an
automatic restart policy.

### Infrastructure acceptance criteria

The verified infrastructure contract includes:

- the exact `26.3.17.56` tag resolves successfully;
- its manifest includes `linux/amd64` and `linux/arm64`;
- the Compose image is pinned to the verified multi-platform
  manifest-index digest;
- `docker compose config` validates;
- the service starts on Apple Silicon without forced amd64 emulation;
- the healthcheck reaches healthy status;
- authenticated HTTP and native queries both return `SELECT 1` successfully;
- the reported server version equals `26.3.17.56`;
- the named volume preserves data across container recreation;
- an explicit destructive reset removes the ClickHouse volume;
- the control plane creates `market_analytics` with `ENGINE = Atomic`;
- no ClickHouse service starts implicitly from application code.

## 8. Metabase and BI boundary

Metabase is the current BI layer. It reads the published ClickHouse serving
target through the read-only `bi_reader` account. A read-only `SELECT` through
`bi_reader` is the operational serving check; the final acceptance refresh
also succeeded against the published target.

The current Metabase dashboard includes the following saved questions and
visualizations. The final E2E acceptance verified that Metabase refreshed
successfully against the newly published ClickHouse target; it did not
separately validate every visualization as an independent acceptance criterion.

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

Datasets query `market_analytics.silver_trades` directly. Dashboard freshness
is refresh-based, not continuous real-time serving. Dashboard layout,
authentication, role management, alerting, scheduled reports, and production
BI governance are outside this local contract.

## 9. Implemented serving sequence

1. Bind one exact Silver snapshot.
2. Load the snapshot into ClickHouse staging.
3. Validate source/staging counts and the duplicate-sensitive full-row multiset fingerprint.
4. Execute one atomic `EXCHANGE TABLES` only after validation succeeds.
5. Validate the published target through ClickHouse and `bi_reader`, then refresh Metabase.

The [serving refresh runbook](runbooks/clickhouse-serving-refresh.md) owns the
operational command, runtime settings, and failure handling.

## 10. Acceptance criteria

The implemented contract requires:

1. `market_catalog.market.silver_trades` exists with the agreed schema.
2. Only valid Bronze rows are transformed; invalid and historical unevaluated rows are absent.
3. Decimal price, quantity, and notional calculations remain exact.
4. Epoch-millisecond conversion and `latency_ms` are correct.
5. Kafka topic/partition/offset coordinates remain traceable.
6. Two consecutive Silver builds over unchanged Bronze produce the same complete multiset of Silver rows, including occurrence counts for exact duplicate rows, without append accumulation.
7. Silver data loads repeatedly into ClickHouse using the documented staging/full-rebuild boundary without duplication.
8. ClickHouse queries return the expected BTCUSDT, ETHUSDT, and SOLUSDT dimensions.
9. The published target is readable through `bi_reader`, and the final E2E acceptance verifies a successful Metabase refresh against it; individual visualizations are not separate acceptance criteria.
10. The process is documented and reproducible locally.

## 11. Non-goals

- Gold tables or permanent aggregate tables.
- Continuous ClickHouse streaming sinks.
- Universal exactly-once, replay, or general deduplication frameworks.
- Historical backfill and repair workflows.
- Monitoring, alerting, or data-quality observability platforms.
- Cloud deployment, Kubernetes, Terraform, or multi-broker durability.
- Large-scale performance tuning.
- Production Metabase authentication, roles, governance, or alerting.

## 12. Explicitly deferred decisions

The following decisions remain outside this local contract:

- TLS;
- Keeper;
- replication and clustering;
- resource quotas;
- persistent ClickHouse log volume;
- backup and multi-version rollback;
- production secret management;
- cloud deployment;
- incremental ClickHouse loading;
- incremental Silver loading;
- replay-aware `source_epoch` and global deduplication.

Additional Metabase provisioning, refresh scheduling, and production BI operations remain future decisions. The implemented serving boundary is bounded and refresh-based, not continuous.
