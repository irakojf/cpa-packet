from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

from click.testing import CliRunner

from cpapacket.cli import payroll_summary as payroll_cli
from cpapacket.cli.main import build_run_context, cli
from cpapacket.clients.auth import OAuthToken
from cpapacket.core.context import RunContext
from cpapacket.deliverables.base import DeliverableResult


def test_payroll_summary_command_requires_gusto_connection() -> None:
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--year", "2025", "payroll-summary"],
        env={},
    )

    assert result.exit_code == 1
    assert (
        "Gusto is not connected. Run `cpapacket auth gusto login` and try again." in result.output
    )


def test_payroll_summary_command_supports_local_overrides(monkeypatch, tmp_path: Path) -> None:
    class _FakeStore:
        def __init__(self, *, cache_dir: Path) -> None:
            self.cache_dir = cache_dir

    class _FakeProviders:
        def __init__(self, *, store: _FakeStore, qbo_client: object, gusto_client: object) -> None:
            self.store = store
            self.qbo_client = qbo_client
            self.gusto_client = gusto_client

    out_dir = tmp_path / "packet_out"

    class _CapturingDeliverable:
        def generate(
            self,
            ctx: object,
            _providers: object,
            prompts: dict[str, object],
        ) -> DeliverableResult:
            run_context = cast(RunContext, ctx)
            assert run_context.year == 2024
            assert run_context.year_source == "explicit"
            assert run_context.out_dir == out_dir.resolve()
            assert run_context.incremental is True
            assert run_context.force is True
            assert prompts == {}
            return DeliverableResult(
                deliverable_key="payroll_summary",
                success=True,
                artifacts=[str(out_dir / "04_Annual_Payroll_Summary" / "summary.csv")],
            )

    monkeypatch.setattr(payroll_cli, "SessionDataStore", _FakeStore)
    monkeypatch.setattr(payroll_cli, "DataProviders", _FakeProviders)
    monkeypatch.setattr(
        payroll_cli,
        "PayrollSummaryDeliverable",
        lambda: _CapturingDeliverable(),
    )
    monkeypatch.setattr(payroll_cli, "_build_gusto_client", lambda: object())

    class _StoreWithToken:
        def __init__(self, provider_name: str) -> None:
            assert provider_name == "gusto"

        def load_token(self) -> OAuthToken:
            return OAuthToken(
                access_token="access",
                refresh_token="refresh",
                expires_at=datetime.now(UTC) + timedelta(hours=1),
            )

    monkeypatch.setitem(build_run_context.__globals__, "OAuthTokenStore", _StoreWithToken)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--year",
            "2025",
            "--non-interactive",
            "payroll-summary",
            "--year",
            "2024",
            "--out",
            str(out_dir),
            "--incremental",
            "--force",
        ],
    )

    assert result.exit_code == 0
    assert "Payroll summary deliverable complete." in result.output
    assert str(out_dir / "04_Annual_Payroll_Summary" / "summary.csv") in result.output
