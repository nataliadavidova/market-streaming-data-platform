"""Unit tests for Spark Iceberg REST catalog builder configuration."""

from jobs.streaming.iceberg_catalog import configure_iceberg_rest_catalog


class RecordingSparkBuilder:
    def __init__(self) -> None:
        self.configs: list[tuple[str, str]] = []

    def config(self, key: str, value: str) -> "RecordingSparkBuilder":
        self.configs.append((key, value))
        return self


def test_configure_iceberg_rest_catalog_sets_full_configuration() -> None:
    builder = RecordingSparkBuilder()

    returned_builder = configure_iceberg_rest_catalog(
        builder,
        catalog_name="market_catalog",
        catalog_uri="http://localhost:8181",
        warehouse="s3://market-lake/warehouse",
        s3_endpoint="http://localhost:9000",
        s3_region="us-east-1",
        s3_access_key="minioadmin",
        s3_secret_key="minioadmin",
    )

    assert returned_builder is builder
    assert builder.configs == [
        (
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        ),
        ("spark.sql.catalog.market_catalog", "org.apache.iceberg.spark.SparkCatalog"),
        ("spark.sql.catalog.market_catalog.type", "rest"),
        ("spark.sql.catalog.market_catalog.uri", "http://localhost:8181"),
        (
            "spark.sql.catalog.market_catalog.warehouse",
            "s3://market-lake/warehouse",
        ),
        (
            "spark.sql.catalog.market_catalog.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        ),
        (
            "spark.sql.catalog.market_catalog.s3.endpoint",
            "http://localhost:9000",
        ),
        ("spark.sql.catalog.market_catalog.s3.path-style-access", "true"),
        ("spark.sql.catalog.market_catalog.s3.access-key-id", "minioadmin"),
        ("spark.sql.catalog.market_catalog.s3.secret-access-key", "minioadmin"),
        ("spark.sql.catalog.market_catalog.client.region", "us-east-1"),
    ]


def test_configure_iceberg_rest_catalog_interpolates_catalog_name() -> None:
    builder = RecordingSparkBuilder()

    configure_iceberg_rest_catalog(
        builder,
        catalog_name="analytics_catalog",
        catalog_uri="http://localhost:8181",
        warehouse="s3://market-lake/warehouse",
        s3_endpoint="http://localhost:9000",
        s3_region="us-east-1",
        s3_access_key="minioadmin",
        s3_secret_key="minioadmin",
    )

    catalog_scoped_keys = [
        key for key, _value in builder.configs if key.startswith("spark.sql.catalog.")
    ]

    assert catalog_scoped_keys == [
        "spark.sql.catalog.analytics_catalog",
        "spark.sql.catalog.analytics_catalog.type",
        "spark.sql.catalog.analytics_catalog.uri",
        "spark.sql.catalog.analytics_catalog.warehouse",
        "spark.sql.catalog.analytics_catalog.io-impl",
        "spark.sql.catalog.analytics_catalog.s3.endpoint",
        "spark.sql.catalog.analytics_catalog.s3.path-style-access",
        "spark.sql.catalog.analytics_catalog.s3.access-key-id",
        "spark.sql.catalog.analytics_catalog.s3.secret-access-key",
        "spark.sql.catalog.analytics_catalog.client.region",
    ]


def test_configure_iceberg_rest_catalog_can_disable_path_style_access() -> None:
    builder = RecordingSparkBuilder()

    configure_iceberg_rest_catalog(
        builder,
        catalog_name="market_catalog",
        catalog_uri="http://localhost:8181",
        warehouse="s3://market-lake/warehouse",
        s3_endpoint="http://localhost:9000",
        s3_region="us-east-1",
        s3_access_key="minioadmin",
        s3_secret_key="minioadmin",
        s3_path_style_access=False,
    )

    assert (
        "spark.sql.catalog.market_catalog.s3.path-style-access",
        "false",
    ) in builder.configs


def test_configure_iceberg_rest_catalog_returns_same_builder() -> None:
    builder = RecordingSparkBuilder()

    returned_builder = configure_iceberg_rest_catalog(
        builder,
        catalog_name="market_catalog",
        catalog_uri="http://localhost:8181",
        warehouse="s3://market-lake/warehouse",
        s3_endpoint="http://localhost:9000",
        s3_region="us-east-1",
        s3_access_key="minioadmin",
        s3_secret_key="minioadmin",
    )

    assert returned_builder is builder


def test_configure_iceberg_rest_catalog_does_not_create_session() -> None:
    builder = RecordingSparkBuilder()

    configure_iceberg_rest_catalog(
        builder,
        catalog_name="market_catalog",
        catalog_uri="http://localhost:8181",
        warehouse="s3://market-lake/warehouse",
        s3_endpoint="http://localhost:9000",
        s3_region="us-east-1",
        s3_access_key="minioadmin",
        s3_secret_key="minioadmin",
    )

    assert len(builder.configs) == 11
