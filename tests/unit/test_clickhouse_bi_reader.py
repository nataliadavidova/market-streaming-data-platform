"""Unit tests for fixed-scope ClickHouse BI reader provisioning."""

import pytest

from jobs.serving import clickhouse_bi_reader as bi_reader
from jobs.serving import clickhouse_schema


class RecordingClient:
    def __init__(self) -> None:
        self.commands: list[str] = []

    def command(self, query: str) -> object:
        self.commands.append(query)
        return None

    def query(self, _query: str) -> object:
        return []


def _config() -> clickhouse_schema.ClickHouseConfig:
    return clickhouse_schema.ClickHouseConfig(
        database="market_analytics",
        user="market_loader",
        password="market-password",
    )


def test_bi_password_is_required() -> None:
    with pytest.raises(bi_reader.ClickHouseBIReaderError, match="CLICKHOUSE_BI_PASSWORD"):
        bi_reader.run_provision(
            environ={
                "CLICKHOUSE_USER": "market_loader",
                "CLICKHOUSE_PASSWORD": "market-password",
            }
        )


def test_fixed_user_queries_converge_auth_and_exact_select_scope() -> None:
    client = RecordingClient()

    result = bi_reader.provision_bi_reader(
        _config(),
        bi_password="bi-secret",
        client_factory=lambda _config: client,
    )

    assert result == bi_reader.BIReaderProvisionResult(
        user="bi_reader",
        database="market_analytics",
        table="silver_trades",
        authentication="sha256_password",
        grant="SELECT",
        status="provisioned",
    )
    assert client.commands == [
        "CREATE USER IF NOT EXISTS bi_reader IDENTIFIED WITH sha256_password BY 'bi-secret'",
        "ALTER USER bi_reader IDENTIFIED WITH sha256_password BY 'bi-secret'",
        "GRANT SELECT ON market_analytics.silver_trades TO bi_reader",
    ]
    assert all("staging" not in command for command in client.commands)
    assert all(
        not any(
            mutation in command.upper()
            for mutation in ("INSERT", "ALTER", "CREATE", "DROP", "TRUNCATE", "EXCHANGE")
        )
        for command in client.commands[2:]
    )


def test_repeated_provisioning_replays_only_idempotent_convergence_commands() -> None:
    clients = [RecordingClient(), RecordingClient()]

    for client in clients:
        bi_reader.provision_bi_reader(
            _config(),
            bi_password="same-secret",
            client_factory=lambda _config, client=client: client,
        )

    assert clients[0].commands == clients[1].commands


def test_empty_direct_password_is_rejected_without_connecting() -> None:
    with pytest.raises(bi_reader.ClickHouseBIReaderError, match="CLICKHOUSE_BI_PASSWORD"):
        bi_reader.provision_bi_reader(
            _config(),
            bi_password="",
            client_factory=lambda _config: (_ for _ in ()).throw(
                AssertionError("connected")
            ),
        )


def test_cli_success_is_bounded_and_hides_password(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    result = bi_reader.BIReaderProvisionResult(
        user="bi_reader",
        database="market_analytics",
        table="silver_trades",
        authentication="sha256_password",
        grant="SELECT",
        status="provisioned",
    )
    monkeypatch.setattr(bi_reader, "run_provision", lambda **_: result)

    assert bi_reader.main(
        ["provision"],
        environ={"CLICKHOUSE_BI_PASSWORD": "bi-secret"},
    ) == 0
    output = capsys.readouterr()
    assert "user=bi_reader" in output.out
    assert "grant=SELECT" in output.out
    assert "bi-secret" not in output.out + output.err


def test_cli_missing_password_is_nonzero_and_does_not_print_secret(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert bi_reader.main(["provision"], environ={}) == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert "CLICKHOUSE_BI_PASSWORD" in output.err
