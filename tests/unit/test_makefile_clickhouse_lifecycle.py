"""Protect the bounded, non-destructive ClickHouse Makefile lifecycle."""

import os
from pathlib import Path
import subprocess


MAKEFILE_PATH = Path(__file__).parents[2] / "Makefile"


def _makefile() -> str:
    return MAKEFILE_PATH.read_text()


def _target_body(makefile: str, target: str) -> str:
    start = makefile.index(f"{target}:")
    remainder = makefile[start + len(target) + 1 :]
    end = remainder.find("\n\n")
    return remainder if end == -1 else remainder[:end]


def test_clickhouse_lifecycle_targets_are_public_and_phony() -> None:
    makefile = _makefile()

    for target in ("clickhouse-up", "clickhouse-wait", "clickhouse-status", "clickhouse-stop"):
        assert f"{target}:" in makefile
        assert target in makefile.split(".PHONY:", maxsplit=1)[1].split("\n", maxsplit=1)[0]


def test_clickhouse_up_status_and_stop_are_service_scoped() -> None:
    makefile = _makefile()

    assert "docker compose up -d clickhouse" in _target_body(makefile, "clickhouse-up")
    assert "docker compose ps clickhouse" in _target_body(makefile, "clickhouse-status")

    stop = _target_body(makefile, "clickhouse-stop")
    assert "docker compose stop clickhouse" in stop
    assert " down" not in stop
    assert " rm" not in stop
    assert "volume" not in stop


def test_clickhouse_wait_is_bounded_and_resolves_container_dynamically() -> None:
    wait = _target_body(_makefile(), "clickhouse-wait")

    assert "docker compose ps -q clickhouse" in wait
    assert "docker inspect --format '{{.State.Health.Status}}'" in wait
    assert "CLICKHOUSE_WAIT_TIMEOUT_SECONDS := 120" in _makefile()
    assert "CLICKHOUSE_WAIT_INTERVAL_SECONDS := 2" in _makefile()
    assert "healthy)" in wait
    assert "unhealthy)" in wait
    assert "ClickHouse container not found." in wait
    assert "Timed out waiting 120 seconds" in wait
    assert "docker compose ps clickhouse" in wait
    assert "docker compose logs --no-color --tail=100 clickhouse" in wait
    assert "--env-file .env.example" not in wait


def test_clickhouse_lifecycle_contains_no_destructive_volume_operation() -> None:
    makefile = _makefile()
    lifecycle = "\n".join(
        _target_body(makefile, target)
        for target in ("clickhouse-up", "clickhouse-wait", "clickhouse-status", "clickhouse-stop")
    )

    assert "down -v" not in lifecycle
    assert "volume rm" not in lifecycle
    assert "docker rm" not in lifecycle


def _run_wait_with_fake_docker(
    tmp_path: Path,
    *,
    container_id: str,
    health_status: str,
    timeout: int | None = None,
) -> subprocess.CompletedProcess[str]:
    fake_docker = tmp_path / "docker"
    fake_docker.write_text(
        """#!/bin/sh
if [ "$1" = "compose" ]; then
    case "$*" in
        *"ps -q clickhouse"*)
            printf '%s\\n' "$FAKE_CONTAINER_ID"
            ;;
    esac
    exit 0
fi
if [ "$1" = "inspect" ]; then
    printf '%s\\n' "$FAKE_HEALTH_STATUS"
    exit 0
fi
exit 0
"""
    )
    fake_docker.chmod(0o755)

    environment = os.environ.copy()
    environment.update(
        {
            "PATH": f"{tmp_path}:{environment['PATH']}",
            "FAKE_CONTAINER_ID": container_id,
            "FAKE_HEALTH_STATUS": health_status,
        }
    )
    command = ["make", "--no-print-directory"]
    if timeout is not None:
        command.extend(
            [
                f"CLICKHOUSE_WAIT_TIMEOUT_SECONDS={timeout}",
                "CLICKHOUSE_WAIT_INTERVAL_SECONDS=0",
            ]
        )
    command.append("clickhouse-wait")
    return subprocess.run(
        command,
        cwd=MAKEFILE_PATH.parent,
        env=environment,
        capture_output=True,
        text=True,
    )


def test_clickhouse_wait_handles_missing_container(tmp_path: Path) -> None:
    result = _run_wait_with_fake_docker(
        tmp_path,
        container_id="",
        health_status="",
    )

    assert result.returncode != 0
    assert "ClickHouse container not found." in result.stderr


def test_clickhouse_wait_handles_unhealthy_container(tmp_path: Path) -> None:
    result = _run_wait_with_fake_docker(
        tmp_path,
        container_id="fake-id",
        health_status="unhealthy",
    )

    assert result.returncode != 0
    assert "ClickHouse became unhealthy." in result.stderr
    assert "clickhouse" in result.stderr


def test_clickhouse_wait_handles_healthy_container(tmp_path: Path) -> None:
    result = _run_wait_with_fake_docker(
        tmp_path,
        container_id="fake-id",
        health_status="healthy",
    )

    assert result.returncode == 0
    assert "ClickHouse is healthy." in result.stdout


def test_clickhouse_wait_handles_timeout(tmp_path: Path) -> None:
    result = _run_wait_with_fake_docker(
        tmp_path,
        container_id="fake-id",
        health_status="starting",
        timeout=0,
    )

    assert result.returncode != 0
    assert "Timed out waiting 120 seconds" in result.stderr
    assert "clickhouse" in result.stderr
