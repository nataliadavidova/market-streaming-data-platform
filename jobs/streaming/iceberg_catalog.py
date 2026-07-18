"""Configure Spark builders for the local Iceberg REST catalog."""

from typing import Protocol, Self


class SparkBuilder(Protocol):
    def config(self, key: str, value: str) -> Self:
        ...


ICEBERG_SPARK_EXTENSIONS = (
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
)
ICEBERG_SPARK_CATALOG = "org.apache.iceberg.spark.SparkCatalog"
ICEBERG_REST_CATALOG_TYPE = "rest"
S3_FILE_IO = "org.apache.iceberg.aws.s3.S3FileIO"


def configure_iceberg_rest_catalog(
    builder: SparkBuilder,
    *,
    catalog_name: str,
    catalog_uri: str,
    warehouse: str,
    s3_endpoint: str,
    s3_region: str,
    s3_access_key: str,
    s3_secret_key: str,
    s3_path_style_access: bool = True,
) -> SparkBuilder:
    """Apply Iceberg REST catalog settings to a Spark builder."""
    catalog_prefix = f"spark.sql.catalog.{catalog_name}"
    path_style_access = str(s3_path_style_access).lower()

    return (
        builder.config("spark.sql.extensions", ICEBERG_SPARK_EXTENSIONS)
        .config(catalog_prefix, ICEBERG_SPARK_CATALOG)
        .config(f"{catalog_prefix}.type", ICEBERG_REST_CATALOG_TYPE)
        .config(f"{catalog_prefix}.uri", catalog_uri)
        .config(f"{catalog_prefix}.warehouse", warehouse)
        .config(f"{catalog_prefix}.io-impl", S3_FILE_IO)
        .config(f"{catalog_prefix}.s3.endpoint", s3_endpoint)
        .config(f"{catalog_prefix}.s3.path-style-access", path_style_access)
        .config(f"{catalog_prefix}.s3.access-key-id", s3_access_key)
        .config(f"{catalog_prefix}.s3.secret-access-key", s3_secret_key)
        .config(f"{catalog_prefix}.client.region", s3_region)
    )
