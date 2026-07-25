"""Classify raw Kafka trade records without filtering or persisting them."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType

from jobs.streaming.trades import RAW_TRADE_EVENT_SCHEMA, TRADE_DECIMAL_TYPE


QUALITY_COLUMNS = ("is_valid", "validation_errors")


def _missing_text(column: object) -> object:
    return column.isNull() | (F.length(F.trim(column)) == 0)


def _validation_errors(
    *,
    raw_json: object,
    json_probe: object,
    event: object,
    price_text: object,
    price: object,
    quantity_text: object,
    quantity: object,
    kafka_topic: object,
    kafka_partition: object,
    kafka_offset: object,
) -> object:
    parsed = json_probe.isNotNull() & event.isNotNull()
    raw_errors = [
        F.when(raw_json.isNull(), F.lit("NULL_RAW_JSON")),
        F.when(raw_json.isNotNull() & json_probe.isNull(), F.lit("MALFORMED_JSON")),
    ]
    field_errors = [
        F.when(parsed & _missing_text(event["exchange"]), F.lit("MISSING_EXCHANGE")),
        F.when(parsed & _missing_text(event["symbol"]), F.lit("MISSING_SYMBOL")),
        F.when(parsed & _missing_text(event["trade_id"]), F.lit("MISSING_TRADE_ID")),
        F.when(parsed & price_text.isNull(), F.lit("MISSING_PRICE")),
        F.when(
            parsed & price_text.isNotNull() & price.isNull(),
            F.lit("INVALID_PRICE"),
        ),
        F.when(parsed & price.isNotNull() & (price <= 0), F.lit("NON_POSITIVE_PRICE")),
        F.when(parsed & quantity_text.isNull(), F.lit("MISSING_QUANTITY")),
        F.when(
            parsed & quantity_text.isNotNull() & quantity.isNull(),
            F.lit("INVALID_QUANTITY"),
        ),
        F.when(
            parsed & quantity.isNotNull() & (quantity <= 0),
            F.lit("NON_POSITIVE_QUANTITY"),
        ),
        F.when(parsed & event["event_time_ms"].isNull(), F.lit("MISSING_EVENT_TIME")),
        F.when(
            parsed
            & event["event_time_ms"].isNotNull()
            & (event["event_time_ms"] <= 0),
            F.lit("NON_POSITIVE_EVENT_TIME"),
        ),
        F.when(
            parsed & event["ingested_at_ms"].isNull(),
            F.lit("MISSING_INGESTED_AT"),
        ),
        F.when(
            parsed
            & event["ingested_at_ms"].isNotNull()
            & (event["ingested_at_ms"] <= 0),
            F.lit("NON_POSITIVE_INGESTED_AT"),
        ),
        F.when(parsed & _missing_text(kafka_topic), F.lit("MISSING_KAFKA_TOPIC")),
        F.when(
            parsed & kafka_partition.isNull(),
            F.lit("MISSING_KAFKA_PARTITION"),
        ),
        F.when(parsed & kafka_offset.isNull(), F.lit("MISSING_KAFKA_OFFSET")),
    ]
    return F.filter(
        F.array(*(raw_errors + field_errors)),
        lambda error: error.isNotNull(),
    )


def classify_raw_trade_kafka_messages(kafka_df: DataFrame) -> DataFrame:
    """Add deterministic quality labels while preserving every raw Kafka row."""
    raw = kafka_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_json"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    ).withColumn(
        "json_probe",
        F.from_json(
            F.col("raw_json"),
            MapType(StringType(), StringType(), valueContainsNull=True),
        ),
    ).withColumn("event", F.from_json(F.col("raw_json"), RAW_TRADE_EVENT_SCHEMA))

    price = F.col("event.price").try_cast(TRADE_DECIMAL_TYPE)
    quantity = F.col("event.quantity").try_cast(TRADE_DECIMAL_TYPE)
    errors = _validation_errors(
        raw_json=F.col("raw_json"),
        json_probe=F.col("json_probe"),
        event=F.col("event"),
        price_text=F.col("event.price"),
        price=price,
        quantity_text=F.col("event.quantity"),
        quantity=quantity,
        kafka_topic=F.col("kafka_topic"),
        kafka_partition=F.col("kafka_partition"),
        kafka_offset=F.col("kafka_offset"),
    )

    return raw.select(
        F.col("event.exchange").alias("exchange"),
        F.col("event.symbol").alias("symbol"),
        F.col("event.trade_id").alias("trade_id"),
        price.alias("price"),
        quantity.alias("quantity"),
        F.col("event.event_time_ms").alias("event_time_ms"),
        F.col("event.ingested_at_ms").alias("ingested_at_ms"),
        "kafka_key",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "raw_json",
        (F.size(errors) == 0).alias("is_valid"),
        errors.alias("validation_errors"),
    )
