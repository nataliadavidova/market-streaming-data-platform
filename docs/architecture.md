# Architecture

This project is currently in the bootstrap phase.

## Version 1 Target Flow

`Market API/WebSocket -> Kafka -> Spark Structured Streaming -> Iceberg on S3-compatible storage -> ClickHouse -> dashboard + basic DQ checks`

Kafka, Spark, Iceberg, ClickHouse, and the dashboard are planned but not implemented yet.

## Current Implemented Slice

`config/market_symbols.yaml -> jobs.producer.config.load_config -> unit test`

The current code loads the market symbols YAML config and verifies it with one unit test.
