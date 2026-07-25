"""Protect the Docker Compose contract for persistent Kafka broker state."""

from pathlib import Path

import yaml


REPOSITORY_ROOT = Path(__file__).parents[2]
COMPOSE_PATH = REPOSITORY_ROOT / "docker-compose.yml"
MAKEFILE_PATH = REPOSITORY_ROOT / "Makefile"


def _compose_config() -> dict:
    return yaml.safe_load(COMPOSE_PATH.read_text())


def test_kafka_log_directory_is_backed_by_a_named_volume() -> None:
    config = _compose_config()
    kafka = config["services"]["kafka"]
    volume_mounts = kafka["volumes"]

    assert "kafka_data:/var/lib/kafka/data" in volume_mounts
    assert kafka["environment"]["KAFKA_LOG_DIRS"] == "/var/lib/kafka/data"
    assert "kafka_data" in config["volumes"]


def test_kafka_persistence_does_not_depend_on_anonymous_data_mount() -> None:
    config = _compose_config()
    volume_mounts = config["services"]["kafka"]["volumes"]

    assert all(not mount.startswith("/var/lib/kafka/data") for mount in volume_mounts)
    assert "kafka_data:/var/lib/kafka/data" in volume_mounts


def test_kafka_down_keeps_normal_compose_down_lifecycle() -> None:
    makefile = MAKEFILE_PATH.read_text()

    assert "kafka-down:\n\tdocker compose down" in makefile
    assert "kafka-down:\n\tdocker compose down -v" not in makefile


def test_kafka_persistence_change_does_not_touch_streaming_storage_contracts() -> None:
    config = _compose_config()
    makefile = MAKEFILE_PATH.read_text()

    assert set(config["services"]) == {"kafka", "minio", "minio-init", "iceberg-rest"}
    assert set(config["volumes"]) == {
        "kafka_data",
        "iceberg_catalog_data",
        "minio_data",
    }
    assert "iceberg-trade-stream:" in makefile
    assert "iceberg-inspect:" in makefile
