# Canonical Bronze quality migration and live cutover

## Purpose

This runbook covers the explicit Iceberg schema migration and the subsequent quality-v2 streaming cutover for:

```text
market_catalog.market.bronze_trades
```

The migration adds the two nullable quality fields. The live job then classifies raw Kafka rows and persists the exact 15-column contract through a new checkpoint and query identity.

## Preconditions

Before running the migration:

- Stop the Binance producer.
- Stop the Spark streaming job.
- Confirm no other writer is active for the canonical table.
- Ensure MinIO and Iceberg REST are available.
- Confirm the canonical table exists and inspect its current schema and metadata.
- Understand that this command mutates canonical table metadata in the development environment.

Keep the old writer stopped throughout migration and cutover. Never run the quality job against the legacy checkpoint.

## Cutover command sequence

Run from the repository root:

```bash
make kafka-up
make kafka-create-topic
make iceberg-up
make iceberg-migrate-bronze-quality
make iceberg-inspect
```

The migration target does not start or stop infrastructure automatically. It does not start Kafka, the producer, or a streaming query. Keep the storage services running for the live verification below.

## Live cutover and restart

After `make iceberg-inspect` confirms the exact 15-column schema:

1. Start `make iceberg-trade-stream`.
2. Confirm the query is active before publishing records.
3. Publish controlled records and verify their quality labels and Kafka coordinates in canonical Bronze.
4. Stop the Spark job.
5. Restart `make iceberg-trade-stream` with the same defaults.
6. Publish new controlled records and verify that only the new Kafka identities are appended.
7. Stop Spark.
8. Run `make iceberg-down` and `make kafka-down`.

The live job uses:

```text
table: market_catalog.market.bronze_trades
checkpoint: s3a://market-lake/checkpoints/market/bronze-trades-quality-v2
query name: market-iceberg-bronze-trades-quality-v2
first-start startingOffsets: latest
```

`startingOffsets=latest` applies when the new checkpoint has no progress. Restarts resume from the saved quality-v2 checkpoint. The legacy and quality-v1 checkpoints remain preserved and are not reused.

## State behavior

The command inspects the exact canonical schema before attempting DDL:

```text
LEGACY_13_COLUMN
→ one ALTER
→ MIGRATED
```

```text
QUALITY_15_COLUMN
→ no ALTER
→ ALREADY_MIGRATED
```

```text
INCOMPATIBLE
→ stop safely
→ no ALTER
```

Partial migrations, reordered or extra columns, missing columns, and type mismatches are incompatible. The final schema is validated after an ALTER; an unexpected result is a failure, not success.

## Exact schema change

The migration appends only:

```sql
ALTER TABLE market_catalog.market.bronze_trades
ADD COLUMNS (
    is_valid BOOLEAN,
    validation_errors ARRAY<STRING>
)
```

The existing 13 columns, their order, and their types remain unchanged. No defaults or `NOT NULL` constraints are added.

## Historical rows

The migration does not backfill or rewrite existing data. Historical rows therefore have:

```text
is_valid = NULL
validation_errors = NULL
```

This means “not evaluated under the quality contract”; it does not mean invalid.

## Safety boundary

The migration command:

- targets only `market_catalog.market.bronze_trades`;
- performs schema inspection and additive `ALTER TABLE` only;
- does not start streaming;
- does not access Kafka or the producer;
- does not use, read, delete, or migrate a checkpoint;
- does not backfill or rewrite rows/files;
- does not drop, recreate, overwrite, or truncate the table;
- does not roll back automatically.

The quality-v2 streaming job:

- requires the exact `QUALITY_15_COLUMN` state before constructing the Kafka source;
- does not run `ALTER TABLE`;
- classifies every Kafka input row without filtering;
- uses the existing append sink;
- rejects both the legacy and quality-v1 checkpoints;
- does not delete, copy, reset, or migrate any checkpoint.

Do not run an older 13-column application version against the migrated table. Compatibility with the legacy writer is not established.

## Verification

Interpret the CLI output as follows:

- `MIGRATED`: the exact legacy schema was found, one ALTER executed, and exact 15-column validation passed.
- `ALREADY_MIGRATED`: the exact 15-column schema was already present and no ALTER executed.
- `INCOMPATIBLE`: the schema is not one of the supported states; no ALTER is attempted.

Use `make iceberg-inspect` to verify the 15-column schema, row count, snapshots/history, data files, and location. For cutover verification, use narrowly scoped read-only queries for `is_valid`, `validation_errors`, `kafka_topic`, `kafka_partition`, and `kafka_offset`. This is table-state inspection, not continuous monitoring.

## Failure handling

If ALTER succeeds but post-migration validation fails:

1. Stop.
2. Do not drop or recreate the table.
3. Preserve the actual schema and error evidence.
4. Do not start streaming.
5. Investigate the current table state before taking any further action.

The migration preserves underlying Spark/catalog causes through exception chaining. A Spark cleanup failure is retained as secondary context when an earlier migration failure exists.

## Controlled runtime evidence

The completed local smoke used Spark 4.1.2, Iceberg 1.11.0, Iceberg REST, and MinIO.

Before migration:

```text
table: market_catalog.market.bronze_trades
location: s3://market-lake/warehouse/market/bronze_trades
schema: 13 columns
row count: 1
snapshots/history: 1 / 1
latest snapshot: 8232280423536300118
data files: 1
```

The first run returned `MIGRATED` and executed one ALTER. Afterwards:

```text
schema: 15 columns
row count: 1
snapshots/history: 1 / 1
latest snapshot: unchanged
data-file count/path: unchanged
location: unchanged
historical quality fields: NULL / NULL
```

The second migration run returned `ALREADY_MIGRATED` and executed no ALTER.

The subsequent controlled live smoke started from the one historical row:

```text
first start:
Kafka offsets: 0, 1, 2
quality outcomes: 1 valid, 1 MALFORMED_JSON, 1 INVALID_PRICE
row count: 1 -> 4
snapshots/history: 1/1 -> 3/3
data files: 1 -> 2
```

The earlier controlled quality-v1 job was stopped and restarted with the same quality-v1 checkpoint:

```text
restart:
Kafka offsets: 3, 4
quality outcomes: 1 valid, 1 INVALID_QUANTITY
additional rows: 2
final row count: 6
snapshots/history: 4/4
data files: 3
schema: 15 columns
```

All observed identities used topic `market.trades.raw`, partition 0, offsets `0..4`. The first-run offsets were not appended again, and raw JSON plus Kafka audit fields remained available. This is controlled restart evidence, not a universal exactly-once, replay, deduplication, or crash-safety guarantee.

Terminal interruption caused the `make` wrapper to return exit code 130. Spark separately logged successful query/context cleanup and process cleanup with exit code 0, and final `docker compose ps` was empty. Code 130 is not a successful application exit, but it was not a data-processing failure in this controlled run.

The old checkpoints remain untouched and must not be reused for the quality-v2 query. No historical backfill, checkpoint deletion, replay control, deduplication, monitoring, Silver transformation, rejected-record table, or production deployment is included.

## Real Binance quality-v2 smoke

The completed local MVP smoke used the documented combined Binance stream for `BTCUSDT`, `ETHUSDT`, and `SOLUSDT`, the production producer, persistent Kafka, Spark quality-v2, Iceberg REST, and MinIO. It appended 182 real rows to canonical Bronze:

```text
BTCUSDT: 162
ETHUSDT: 13
SOLUSDT: 7
```

All observed rows had positive trade values, populated event/ingestion and Kafka audit fields, `is_valid = true`, empty `validation_errors`, unique topic/partition/offset coordinates, and normalized internal `TradeEvent` JSON in `raw_json`. The table ended at 15 columns, 188 rows, 30 snapshots/history entries, and 28 data files.

Kafka `market.trades.raw` retained its TopicId and end offset `182`, and records remained readable after one ordinary `make kafka-down` -> `make kafka-up` cycle using the named broker volume. Producer and Spark were stopped cleanly; a terminal wrapper interruption may report exit 130 even when Spark cleanup exits 0. This is controlled local evidence, not a universal exactly-once, replay, disaster-recovery, or multi-broker durability guarantee.

The next milestone is minimal Silver -> ClickHouse -> mini-dashboard.
