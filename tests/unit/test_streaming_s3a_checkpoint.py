"""Unit tests for S3A checkpoint storage builder configuration."""

import pytest

from jobs.streaming.s3a_checkpoint import configure_s3a_checkpoint_storage


EXPECTED_DEFAULT_CONFIGS = [
    ("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"),
    ("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000"),
    ("spark.hadoop.fs.s3a.access.key", "minioadmin"),
    ("spark.hadoop.fs.s3a.secret.key", "minioadmin"),
    ("spark.hadoop.fs.s3a.path.style.access", "true"),
    ("spark.hadoop.fs.s3a.connection.ssl.enabled", "false"),
    ("spark.hadoop.fs.s3a.endpoint.region", "us-east-1"),
    (
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    ),
]


class RecordingSparkBuilder:
    def __init__(self, *, fail_on_call: int | None = None) -> None:
        self.fail_on_call = fail_on_call
        self.configs: list[tuple[str, str]] = []

    def config(self, key: str, value: str) -> "RecordingSparkBuilder":
        if self.fail_on_call is not None and len(self.configs) + 1 == self.fail_on_call:
            raise RuntimeError("config failed")
        self.configs.append((key, value))
        return self


def test_configure_s3a_checkpoint_storage_sets_default_minio_configuration() -> None:
    builder = RecordingSparkBuilder()

    result = configure_s3a_checkpoint_storage(
        builder,
        endpoint="http://localhost:9000",
        region="us-east-1",
        access_key="minioadmin",
        secret_key="minioadmin",
    )

    assert result is builder
    assert builder.configs == EXPECTED_DEFAULT_CONFIGS


def test_configure_s3a_checkpoint_storage_serializes_custom_booleans() -> None:
    builder = RecordingSparkBuilder()

    configure_s3a_checkpoint_storage(
        builder,
        endpoint="http://localhost:9000",
        region="us-east-1",
        access_key="minioadmin",
        secret_key="minioadmin",
        path_style_access=False,
        ssl_enabled=True,
    )

    assert builder.configs[4] == ("spark.hadoop.fs.s3a.path.style.access", "false")
    assert builder.configs[5] == ("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")


def test_configure_s3a_checkpoint_storage_uses_custom_values() -> None:
    builder = RecordingSparkBuilder()

    configure_s3a_checkpoint_storage(
        builder,
        endpoint="https://s3.example.test",
        region="eu-central-1",
        access_key="custom-access",
        secret_key="custom-secret",
    )

    assert builder.configs == [
        ("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"),
        ("spark.hadoop.fs.s3a.endpoint", "https://s3.example.test"),
        ("spark.hadoop.fs.s3a.access.key", "custom-access"),
        ("spark.hadoop.fs.s3a.secret.key", "custom-secret"),
        ("spark.hadoop.fs.s3a.path.style.access", "true"),
        ("spark.hadoop.fs.s3a.connection.ssl.enabled", "false"),
        ("spark.hadoop.fs.s3a.endpoint.region", "eu-central-1"),
        (
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        ),
    ]


def test_configure_s3a_checkpoint_storage_propagates_config_errors() -> None:
    builder = RecordingSparkBuilder(fail_on_call=4)

    with pytest.raises(RuntimeError, match="config failed"):
        configure_s3a_checkpoint_storage(
            builder,
            endpoint="http://localhost:9000",
            region="us-east-1",
            access_key="minioadmin",
            secret_key="minioadmin",
        )

    assert builder.configs == EXPECTED_DEFAULT_CONFIGS[:3]
