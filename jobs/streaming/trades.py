"""Parse raw trade Kafka messages into typed Spark columns."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
)


TRADE_DECIMAL_TYPE = DecimalType(38, 18)

RAW_TRADE_EVENT_SCHEMA = StructType(
    [
        StructField("exchange", StringType(), nullable=True),
        StructField("symbol", StringType(), nullable=True),
        StructField("trade_id", StringType(), nullable=True),
        StructField("price", StringType(), nullable=True),
        StructField("quantity", StringType(), nullable=True),
        StructField("event_time_ms", LongType(), nullable=True),
        StructField("ingested_at_ms", LongType(), nullable=True),
    ]
)


def parse_raw_trade_kafka_messages(kafka_df: DataFrame) -> DataFrame:
    parsed_df = (
        kafka_df.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_json"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn(
            "event",
            F.from_json(F.col("raw_json"), RAW_TRADE_EVENT_SCHEMA),
        )
    )

    return parsed_df.select(
        F.col("event.exchange").alias("exchange"),
        F.col("event.symbol").alias("symbol"),
        F.col("event.trade_id").alias("trade_id"),
        F.col("event.price").cast(TRADE_DECIMAL_TYPE).alias("price"),
        F.col("event.quantity").cast(TRADE_DECIMAL_TYPE).alias("quantity"),
        F.col("event.event_time_ms").alias("event_time_ms"),
        F.col("event.ingested_at_ms").alias("ingested_at_ms"),
        "kafka_key",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "raw_json",
    )
