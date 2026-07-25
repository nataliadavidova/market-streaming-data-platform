"""Configure Spark builders for S3A Structured Streaming checkpoints."""

from typing import Protocol, Self


class SparkBuilderLike(Protocol):
    def config(self, key: str, value: str) -> Self:
        ...


S3A_FILE_SYSTEM = "org.apache.hadoop.fs.s3a.S3AFileSystem"
S3A_SIMPLE_CREDENTIALS_PROVIDER = (
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
)
LEGACY_BRONZE_CHECKPOINT_LOCATION = (
    "s3a://market-lake/checkpoints/market/bronze-trades"
)
QUALITY_V1_BRONZE_CHECKPOINT_LOCATION = (
    "s3a://market-lake/checkpoints/market/bronze-trades-quality-v1"
)
QUALITY_V2_BRONZE_CHECKPOINT_LOCATION = (
    "s3a://market-lake/checkpoints/market/bronze-trades-quality-v2"
)
QUALITY_BRONZE_CHECKPOINT_LOCATION = QUALITY_V2_BRONZE_CHECKPOINT_LOCATION


def validate_quality_checkpoint_location(checkpoint_location: str) -> str:
    """Reject reuse of checkpoints belonging to an earlier Bronze epoch."""
    if checkpoint_location in {
        LEGACY_BRONZE_CHECKPOINT_LOCATION,
        QUALITY_V1_BRONZE_CHECKPOINT_LOCATION,
    }:
        raise ValueError(
            "the Bronze quality stream cannot use a legacy or quality-v1 checkpoint"
        )
    return checkpoint_location


def configure_s3a_checkpoint_storage(
    builder: SparkBuilderLike,
    *,
    endpoint: str,
    region: str,
    access_key: str,
    secret_key: str,
    path_style_access: bool = True,
    ssl_enabled: bool = False,
) -> SparkBuilderLike:
    """Apply Hadoop S3A settings for Spark checkpoint storage."""
    return (
        builder.config("spark.hadoop.fs.s3a.impl", S3A_FILE_SYSTEM)
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", str(path_style_access).lower())
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(ssl_enabled).lower())
        .config("spark.hadoop.fs.s3a.endpoint.region", region)
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            S3A_SIMPLE_CREDENTIALS_PROVIDER,
        )
    )
