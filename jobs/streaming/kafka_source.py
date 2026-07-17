"""Build Spark Kafka streaming sources for raw market trade messages."""

from pyspark.sql import DataFrame, SparkSession


def read_raw_trade_kafka_stream(
    spark: SparkSession,
    *,
    bootstrap_servers: str,
    topic: str,
) -> DataFrame:
    """Build a streaming DataFrame for raw trade messages from Kafka."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .load()
    )
