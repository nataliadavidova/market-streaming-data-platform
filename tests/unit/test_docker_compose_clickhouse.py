"""Protect the rendered Docker Compose ClickHouse service contract."""

import subprocess
from pathlib import Path

import yaml


REPOSITORY_ROOT = Path(__file__).parents[2]
ENV_PATH = REPOSITORY_ROOT / ".env.example"
CLICKHOUSE_IMAGE = (
    "clickhouse/clickhouse-server:26.3.17.56@"
    "sha256:422be85ae7344058369cdd366ac0efea9daa8428b55c9cf50258e83a7d12fcb3"
)


def _rendered_compose_config() -> dict:
    result = subprocess.run(
        [
            "docker",
            "compose",
            "--env-file",
            str(ENV_PATH),
            "config",
        ],
        cwd=REPOSITORY_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return yaml.safe_load(result.stdout)


def _example_environment() -> dict[str, str]:
    values: dict[str, str] = {}
    for line in ENV_PATH.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        name, value = stripped.split("=", maxsplit=1)
        values[name] = value
    return values


def test_rendered_clickhouse_service_contract() -> None:
    config = _rendered_compose_config()
    clickhouse = config["services"]["clickhouse"]

    assert clickhouse["image"] == CLICKHOUSE_IMAGE
    assert "platform" not in clickhouse
    assert "container_name" not in clickhouse
    assert "restart" not in clickhouse
    assert "privileged" not in clickhouse
    assert "network_mode" not in clickhouse
    assert "command" not in clickhouse
    assert "entrypoint" not in clickhouse

    assert {
        (str(port["published"]), int(port["target"]))
        for port in clickhouse["ports"]
    } == {("18123", 8123), ("19000", 9000)}

    environment = clickhouse["environment"]
    assert "CLICKHOUSE_USER" in environment
    assert "CLICKHOUSE_PASSWORD" in environment
    assert "CLICKHOUSE_DB" not in environment
    assert "CLICKHOUSE_SKIP_USER_SETUP" not in environment

    volume_mounts = clickhouse["volumes"]
    assert {
        (mount["source"], mount["target"])
        for mount in volume_mounts
    } == {("clickhouse-data", "/var/lib/clickhouse")}
    assert "clickhouse-data" in config["volumes"]

    healthcheck = clickhouse["healthcheck"]
    health_command = healthcheck["test"]
    assert health_command[0] == "CMD-SHELL"
    assert "clickhouse-client" in health_command[1]
    assert "SELECT 1" in health_command[1]
    assert '"$${CLICKHOUSE_USER}"' in health_command[1]
    assert '"$${CLICKHOUSE_PASSWORD}"' in health_command[1]
    assert healthcheck["interval"] == "5s"
    assert healthcheck["timeout"] == "3s"
    assert healthcheck["retries"] == 20
    assert healthcheck["start_period"] == "10s"

    assert clickhouse["ulimits"]["nofile"] == {
        "soft": 262144,
        "hard": 262144,
    }


def test_clickhouse_example_environment_contract() -> None:
    environment = _example_environment()

    assert environment["CLICKHOUSE_HOST"] == "localhost"
    assert environment["CLICKHOUSE_HTTP_PORT"] == "18123"
    assert environment["CLICKHOUSE_NATIVE_PORT"] == "19000"
    assert environment["CLICKHOUSE_DATABASE"] == "market_analytics"
    assert environment["CLICKHOUSE_USER"] == "market_loader"
    assert "CLICKHOUSE_DB" not in environment
