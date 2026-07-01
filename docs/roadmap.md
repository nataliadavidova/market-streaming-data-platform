# Roadmap

This project is currently in the Version 1 bootstrap phase. Later versions are planned but not implemented yet.

## Version 1: Market Streaming MVP

Build the first end-to-end market data flow:

`Market API/WebSocket -> Kafka -> Spark Structured Streaming -> Iceberg on S3-compatible storage -> ClickHouse -> dashboard + basic DQ checks`

Current progress: bootstrap structure, market symbols config, config loader, and one unit test.

## Version 2: CDC + Greenplum MVP

Add a PostgreSQL operational source, Debezium CDC, Kafka, and Greenplum DWH flow.

## Version 3: dbt / Marts / Docs / Basic Lineage

Add dbt models, tests, documentation, marts, and basic lineage.

## Version 4: Production-Like Reliability

Add reliability features such as Schema Registry, DLQ or quarantine handling, monitoring, alerts, checkpointing, watermarking, idempotency, and CI/CD.

## Version 5: ML / MLOps

Add feature tables, model training, experiment tracking, and prediction outputs.

## Version 6: Cloud / Infra / Terraform / Optional Kubernetes

Add Terraform-managed cloud infrastructure, deployment strategy, and optional Kubernetes support.
