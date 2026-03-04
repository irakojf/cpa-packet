from __future__ import annotations

from pathlib import Path
from typing import Any

from click.testing import CliRunner

from cpapacket.cli.main import cli
from cpapacket.packet.health_check import DataHealthIssue, DataHealthReport


def _valid_env() -> dict[str, str]:
    return {
        "CPAPACKET_QBO_CLIENT_ID": "id",
        "CPAPACKET_QBO_CLIENT_SECRET": "secret",
        "CPAPACKET_QBO_REDIRECT_URI": "https://localhost/callback",
        "CPAPACKET_QBO_REALM_ID": "realm",
    }


def test_cli_check_requires_qbo_env() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["--year", "2025", "check"], env={})

    assert result.exit_code == 1
    assert "Missing required environment variable: CPAPACKET_QBO_CLIENT_ID" in result.output


def test_cli_check_writes_report_and_prints_warnings(monkeypatch: Any, tmp_path: Path) -> None:
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    monkeypatch.setattr("cpapacket.cli.check.SessionDataStore", lambda *args, **kwargs: object())
    monkeypatch.setattr("cpapacket.cli.check.DataProviders", lambda *args, **kwargs: object())
    monkeypatch.setattr("cpapacket.cli.check._build_qbo_client", lambda: object())

    report = DataHealthReport(
        year=2025,
        generated_at="2026-03-04T00:00:00Z",
        check_names=["sample"],
        issues=[
            DataHealthIssue(
                code="uncategorized_transactions",
                title="Uncategorized Transactions",
                message="Found uncategorized activity.",
            )
        ],
    )

    monkeypatch.setattr("cpapacket.cli.check.run_data_health_precheck", lambda **kwargs: report)
    monkeypatch.setattr(
        "cpapacket.cli.check.write_data_health_report",
        lambda **kwargs: tmp_path / "_meta" / "public" / "data_health_check.txt",
    )
    monkeypatch.setattr("cpapacket.cli.check.should_continue_after_report", lambda **kwargs: True)

    result = runner.invoke(cli, ["--year", "2025", "check"], env=_valid_env())

    assert result.exit_code == 0
    assert "Data health check complete." in result.output
    assert "Warnings found: 1" in result.output
    assert "WARNING [uncategorized_transactions] Found uncategorized activity." in result.output


def test_cli_check_aborts_when_user_declines_continue(monkeypatch: Any, tmp_path: Path) -> None:
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    monkeypatch.setattr("cpapacket.cli.check.SessionDataStore", lambda *args, **kwargs: object())
    monkeypatch.setattr("cpapacket.cli.check.DataProviders", lambda *args, **kwargs: object())
    monkeypatch.setattr("cpapacket.cli.check._build_qbo_client", lambda: object())

    report = DataHealthReport(
        year=2025,
        generated_at="2026-03-04T00:00:00Z",
        check_names=["sample"],
        issues=[
            DataHealthIssue(
                code="open_prior_year_items",
                title="Open Prior-Year Items",
                message="Found open prior-year entries.",
            )
        ],
    )

    monkeypatch.setattr("cpapacket.cli.check.run_data_health_precheck", lambda **kwargs: report)
    monkeypatch.setattr(
        "cpapacket.cli.check.write_data_health_report",
        lambda **kwargs: tmp_path / "_meta" / "public" / "data_health_check.txt",
    )
    monkeypatch.setattr("cpapacket.cli.check.should_continue_after_report", lambda **kwargs: False)

    result = runner.invoke(cli, ["--year", "2025", "check"], env=_valid_env())

    assert result.exit_code == 1
    assert "Aborted by user: Data quality issues detected. Continue anyway?" in result.output
