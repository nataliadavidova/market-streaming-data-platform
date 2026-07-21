"""Configure Spark builders for S3A Structured Streaming checkpoints."""

from typing import Protocol, Self


class SparkBuilderLike(Protocol):
    def config(self, key: str, value: str) -> Self:
        ...


S3A_FILE_SYSTEM = "org.apache.hadoop.fs.s3a.S3AFileSystem"
S3A_SIMPLE_CREDENTIALS_PROVIDER = (
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
)


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
