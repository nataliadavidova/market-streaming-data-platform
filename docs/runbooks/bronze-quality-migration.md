# Canonical Bronze quality migration

## Purpose

`make iceberg-migrate-bronze-quality` performs one explicit, idempotent Iceberg schema migration for the canonical development table:

```text
market_catalog.market.bronze_trades
```

It adds the two nullable quality fields required by the persisted Bronze contract. It does not connect the classifier to streaming and does not write rows.

## Preconditions

Before running the migration:

- Stop the Binance producer.
- Stop the Spark streaming job.
- Confirm no other writer is active for the canonical table.
- Ensure MinIO and Iceberg REST are available.
- Confirm the canonical table exists and inspect its current schema and metadata.
- Understand that this command mutates canonical table metadata in the development environment.

Do not use this command as a substitute for a live-path cutover. The current streaming job still produces the legacy 13-column DataFrame.

## Supported command sequence

Run from the repository root:

```bash
make iceberg-up
make iceberg-migrate-bronze-quality
make iceberg-inspect
make iceberg-down
```

The migration target does not start or stop infrastructure automatically. It does not start Kafka, the producer, or a streaming query.

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

This command:

- targets only `market_catalog.market.bronze_trades`;
- performs schema inspection and additive `ALTER TABLE` only;
- does not start streaming;
- does not access Kafka or the producer;
- does not use, read, delete, or migrate a checkpoint;
- does not backfill or rewrite rows/files;
- does not drop, recreate, overwrite, or truncate the table;
- does not roll back automatically.

**After the canonical table has 15 columns, do not start the current legacy 13-column streaming job.** The next integration slice must first connect `classify_raw_trade_kafka_messages(...)`, validate the 15-column table before stream start, and configure a new versioned checkpoint.

## Verification

Interpret the CLI output as follows:

- `MIGRATED`: the exact legacy schema was found, one ALTER executed, and exact 15-column validation passed.
- `ALREADY_MIGRATED`: the exact 15-column schema was already present and no ALTER executed.
- `INCOMPATIBLE`: the schema is not one of the supported states; no ALTER is attempted.

Use `make iceberg-inspect` to verify the 15-column schema, unchanged row count, snapshots/history, data files, and location. This is table-state inspection, not continuous monitoring.

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

The second run returned `ALREADY_MIGRATED` and executed no ALTER. These are controlled development-environment observations, not guarantees about future table state, exactly-once behavior, historical classification, or streaming restart compatibility.

## Next step

The next slice is canonical live quality integration:

```text
replace the legacy parser with classify_raw_trade_kafka_messages(...)
→ validate the 15-column table before stream start
→ introduce a new versioned checkpoint and query name
→ use explicit startingOffsets=latest for first start
→ validate controlled initial start and restart
```

The old checkpoint remains untouched and must not be reused for the migrated query.
