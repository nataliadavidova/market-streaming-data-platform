# Iceberg Inspection Runbook

This runbook describes the repeatable, read-only workflow for inspecting the existing Bronze Iceberg table.

## Purpose

The inspector helps answer whether the table exists and what it currently contains: table identity and location, schema, row count, Iceberg snapshots and history, physical data files, and partition metadata. It reads table state; it does not create or modify the table.

## Prerequisites

- Docker Desktop is accessible.
- The project environment and dependencies are installed.
- The repository root is the current directory.
- The existing table is available as `market_catalog.market.bronze_trades`.
- MinIO and the Iceberg REST catalog are available.

The inspection itself does not require Binance, Kafka, the producer, or a Spark streaming query. The table must already exist; the inspector never creates it.

## Standard workflow

Start the local storage services explicitly, run the inspection, and stop the services afterward:

```bash
make iceberg-up
make iceberg-inspect
make iceberg-down
```

`iceberg-inspect` does not start or stop infrastructure automatically. It uses the existing Spark/Iceberg package coordinates and configuration defaults from the streaming job. It defaults to the canonical Bronze table, or uses `ICEBERG_BRONZE_TABLE` when that environment value is supplied through the existing configuration contract.

## Direct module usage

The inspector exposes the same CLI arguments as a Python module:

```bash
python -m jobs.streaming.iceberg_inspection \
  --table market_catalog.market.bronze_trades
```

This form is useful when the required Spark and Iceberg runtime packages are already available to the environment. The normal `make iceberg-inspect` target uses `spark-submit` with the project's Spark SQL, Hadoop AWS, Iceberg Spark runtime, and Iceberg AWS bundle packages, so it is the preferred local command when those packages are not preinstalled.

The optional `--max-rows` argument bounds displayed metadata rows and defaults to `100`:

```bash
python -m jobs.streaming.iceberg_inspection \
  --table market_catalog.market.bronze_trades \
  --max-rows 25
```

## Output sections

The command prints these sections in order:

1. **Table identity and existence** — describes the requested table and confirms that the catalog can resolve it. The output includes the table location.
2. **Schema** — shows the table columns, types, and nullability information exposed by Spark.
3. **Row count** — runs a read-only `COUNT(*)`. This is acceptable for the current small local table, but can be expensive on a large table.
4. **Snapshots** — shows committed Iceberg table versions, including snapshot identifiers, timestamps, operations, and available summaries. A snapshot is one committed table version; it is not necessarily one trade or one Spark micro-batch.
5. **History** — shows when snapshots became current and their parent relationships.
6. **Data files** — shows Iceberg file metadata such as paths, formats, record counts, sizes, and available partition values. The inspector does not read Parquet contents.
7. **Partition information** — shows the current partition metadata representation and its aggregate or partition-level statistics.

Iceberg metadata describes table state; it is distinct from table data in Parquet files. Spark checkpoints are also separate: they retain streaming progress and Kafka-offset state for the streaming query and are outside this inspector.

## Unpartitioned Bronze behavior

The controlled runtime used Spark 4.1.2 and Iceberg 1.11.0. For `market_catalog.market.bronze_trades`, the partitions metadata relation returned one row without a `partition` column. The inspector therefore prints:

```text
unpartitioned table (aggregate table statistics)
```

The aggregate row retained fields including:

- `record_count`
- `file_count`
- `total_data_file_size_in_bytes`
- `last_updated_at`
- `last_updated_snapshot_id`

This is the representation observed for the tested runtime, not a universal metadata schema for every Iceberg release. Partitioned tables with a `partition` column are reported as partitioned; an empty metadata result is reported clearly without failing.

## Read-only boundary

The inspector executes only `DESCRIBE` and `SELECT` statements, including bounded metadata queries. It never executes:

```text
CREATE
ALTER
DROP
INSERT
UPDATE
DELETE
MERGE
CALL
```

It also does not:

- call `ensure_bronze_trade_table(...)`;
- use a DataFrame writer;
- start a streaming query;
- inspect or mutate a Spark checkpoint;
- compact data files;
- expire snapshots;
- remove orphan files;
- change table properties or schema.

The command owns the Spark session it creates and stops it in both success and failure paths. An inspection error remains primary if Spark cleanup also fails, with the cleanup failure retained as diagnostic context.

## Controlled runtime evidence

The completed smoke used the real inspector against Spark 4.1.2, Iceberg 1.11.0, the Iceberg REST catalog, and MinIO:

```text
table: market_catalog.market.bronze_trades
location: s3://market-lake/warehouse/market/bronze_trades
schema columns: 13
row count: 1
snapshots: 1
history entries: 1
data files: 1
partition metadata rows: 1 aggregate row
latest snapshot ID: 8232280423536300118
```

Two consecutive inspections were run without a writer between them. Row count, snapshot count, history count, data-file count, latest snapshot ID, data-file path, and partition metadata count remained unchanged.

This is controlled runtime evidence that the inspector did not create a new Iceberg commit during the smoke. It is not a universal immutability, exactly-once, deduplication, or crash-safety guarantee.

## Troubleshooting

- **Table missing:** confirm the catalog URI, table identifier, and that the Bronze writer has already created the table. Do not create it from the inspector.
- **Iceberg REST unavailable:** check that `iceberg-rest` is healthy and that the configured catalog URI is reachable.
- **MinIO unavailable:** check that MinIO is healthy and that the configured endpoint and credentials match the local environment.
- **Docker socket unavailable:** restore Docker Desktop access or use an environment with an accessible Docker daemon; do not treat this as a table result.
- **Unsupported metadata relation:** retain the Spark/Iceberg error. Metadata relation schemas and support can vary by Iceberg version.
- **Spark cleanup failure:** the command reports the cleanup failure when inspection succeeded, or adds it as context when inspection already failed.
- **Slow row count:** `COUNT(*)` scans the current table and may be expensive as the table grows.

Errors preserve the underlying Spark or catalog cause where possible rather than converting every failure into “table missing.”

## Limitations and next work

This workflow does not provide:

- Spark checkpoint inspection or checkpoint repair;
- Bronze data-quality validation or quarantine;
- a Silver table;
- deduplication, replay, or backfill;
- monitoring, metrics, or alerting;
- Iceberg maintenance, compaction, snapshot expiration, or orphan cleanup;
- schema-evolution policy;
- an exactly-once guarantee.

The next storage slice is a narrow Bronze data-quality contract with explicit validity rules and deterministic handling. Broader Silver modeling, storage monitoring, producer monitoring, Kafka batching/backpressure redesign, Iceberg maintenance, schema evolution, deduplication, and replay remain separate future work.

## Cleanup

After inspection, stop only the project storage services:

```bash
make iceberg-down
docker compose ps
```

Do not remove persisted MinIO/Iceberg volumes merely to complete an inspection.
