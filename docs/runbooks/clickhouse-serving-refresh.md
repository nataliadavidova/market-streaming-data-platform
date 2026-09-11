# ClickHouse Serving Refresh

This runbook describes the bounded full-snapshot refresh from the authoritative
Silver Iceberg table to the ClickHouse serving copy.

## Prerequisites

- Local Kafka, MinIO, Iceberg REST, and ClickHouse services are healthy.
- The producer, Bronze streaming job, and another serving rebuild are stopped.
- `market_catalog.market.silver_trades` contains the intended deterministic
  Silver snapshot.
- The ClickHouse database is `market_analytics`, with target
  `silver_trades` and staging `silver_trades_staging` tables.
- The ClickHouse schema and MergeTree/Atomic engine contracts pass before the
  load begins.

## Command

From the repository root, run the high-level workflow once:

```bash
python -m jobs.serving.clickhouse_rebuild rebuild
```

The workflow binds one Silver `snapshot_id` at the start and uses that bound
snapshot for the complete source read and validation.

## Serving runtime contract

The data-plane dependency is:

```text
com.clickhouse:clickhouse-jdbc:0.9.9
```

The JDBC URL uses the supported UTC client-timezone properties:

```text
use_server_time_zone=false
use_time_zone=UTC
```

The serving Spark session also sets, before session creation:

```text
spark.task.maxFailures=1
spark.stage.maxConsecutiveAttempts=1
spark.speculation=false
```

The serving JVM and Spark SQL session use the repository UTC contract. These
settings are scoped to the bounded ClickHouse serving session.

## Execution and validation order

The control plane and Spark data plane perform this sequence:

1. Validate the ClickHouse database, target, and staging contracts.
2. Bind the current Silver snapshot once.
3. Truncate staging through the workflow.
4. Load the complete bound Silver snapshot into staging through Spark JDBC.
5. Validate source and staging row counts and the duplicate-sensitive full-row
   multiset fingerprint. The fingerprint preserves legitimate duplicate row
   multiplicity.
6. Execute exactly one atomic `EXCHANGE TABLES` only after all validation
   passes.
7. Validate the new target and the swapped staging table, then run a read-only
   `bi_reader` query. Refresh Metabase only after successful publication.

The active target remains unchanged if loading or validation fails. After a
successful exchange, staging contains the previous active target as a direct
effect of the swap; this is not an automatic rollback mechanism.

## Failure handling

Do not manually publish, issue `EXCHANGE TABLES`, or blindly retry after a
failed or ambiguous rebuild. Inspect the workflow output and ClickHouse query
history first. A later whole-load retry must begin through the normal workflow,
including its staging-truncate boundary.

## Compatibility and reliability note

ClickHouse JDBC 0.8.6 reproduced duplicate batch submission during the larger
serving load. Count and fingerprint validation blocked that invalid staging
copy, and the active target stayed unchanged. The serving-only Spark fail-fast
settings were added as a defensive measure; they were not the root-cause fix.

JDBC 0.9.9 was then adopted after the legacy `session_timezone=UTC` URL
property was rejected by that driver. The supported contract is
`use_server_time_zone=false&use_time_zone=UTC`. Two subsequent publications
with JDBC 0.9.9 completed with exact row counts, `exact_copy=true`, and one
atomic exchange each. This is controlled local runtime evidence, not a general
exactly-once guarantee.

## Verified acceptance

The final controlled local acceptance run demonstrated the complete path

`Binance WebSocket -> Kafka -> Spark Structured Streaming -> Bronze Iceberg -> Silver Iceberg -> ClickHouse -> bi_reader -> Metabase`.

Observed acceptance evidence:

- Bronze snapshot `1442655979947713871`: total `11291`, valid `11287`,
  invalid `3`, unevaluated `1`.
- Silver snapshot `602819691880655230`: `11287` rows, with `BTCUSDT 5774`,
  `ETHUSDT 4290`, and `SOLUSDT 1223`.
- ClickHouse source/staging validation reached `11287` rows and returned
  `exact_copy=true`.
- Exactly one real atomic `EXCHANGE` occurred for the publication.
- The final ClickHouse target contained `11287` rows and staging contained the
  previous `10007`-row target after the exchange.
- `bi_reader` saw `11287` rows.
- Metabase refreshed successfully to the published state.

These values are acceptance evidence for this run, not architectural constants.
