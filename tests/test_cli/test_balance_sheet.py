from __future__ import annotations

from pathlib import Path
from typing import cast

from click.testing import CliRunner

from cpapacket.cli import balance_sheet as balance_sheet_cli
from cpapacket.cli.main import cli
from cpapacket.core.context import RunContext
from cpapacket.deliverables.base import DeliverableResult


def test_balance_sheet_command_supports_local_overrides(monkeypatch, tmp_path: Path) -> None:
    class _FakeStore:
        def __init__(self, *, cache_dir: Path) -> None:
            self.cache_dir = cache_dir

    class _FakeProviders:
        def __init__(self, *, store: _FakeStore, qbo_client: object) -> None:
            self.store = store
            self.qbo_client = qbo_client

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
                deliverable_key="balance_sheet",
                success=True,
                artifacts=[str(out_dir / "Balance_Sheet_2024-12-31.csv")],
            )

    monkeypatch.setattr(balance_sheet_cli, "SessionDataStore", _FakeStore)
    monkeypatch.setattr(balance_sheet_cli, "DataProviders", _FakeProviders)
    monkeypatch.setattr(
        balance_sheet_cli,
        "BalanceSheetDeliverable",
        lambda: _CapturingDeliverable(),
    )
    monkeypatch.setattr(balance_sheet_cli, "_build_qbo_client", lambda: object())

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "balance-sheet",
            "--year",
            "2024",
            "--out",
            str(out_dir),
            "--incremental",
            "--force",
        ],
    )

    assert result.exit_code == 0
    assert "Balance sheet deliverable complete." in result.output
    assert str(out_dir / "Balance_Sheet_2024-12-31.csv") in result.output


def test_prior_balance_sheet_command_supports_local_overrides(monkeypatch, tmp_path: Path) -> None:
    class _FakeStore:
        def __init__(self, *, cache_dir: Path) -> None:
            self.cache_dir = cache_dir

    class _FakeProviders:
        def __init__(self, *, store: _FakeStore, qbo_client: object) -> None:
            self.store = store
            self.qbo_client = qbo_client

    out_dir = tmp_path / "packet_out"

    class _CapturingDeliverable:
        def generate(
            self,
            ctx: object,
            _providers: object,
            prompts: dict[str, object],
        ) -> DeliverableResult:
            run_context = cast(RunContext, ctx)
            assert run_context.year == 2023
            assert run_context.year_source == "explicit"
            assert run_context.out_dir == out_dir.resolve()
            assert run_context.incremental is False
            assert run_context.force is False
            assert prompts == {}
            return DeliverableResult(
                deliverable_key="prior_balance_sheet",
                success=True,
                artifacts=[str(out_dir / "Balance_Sheet_2022-12-31.csv")],
            )

    monkeypatch.setattr(balance_sheet_cli, "SessionDataStore", _FakeStore)
    monkeypatch.setattr(balance_sheet_cli, "DataProviders", _FakeProviders)
    monkeypatch.setattr(
        balance_sheet_cli,
        "PriorBalanceSheetDeliverable",
        lambda: _CapturingDeliverable(),
    )
    monkeypatch.setattr(balance_sheet_cli, "_build_qbo_client", lambda: object())

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "prior-balance-sheet",
            "--year",
            "2023",
            "--out",
            str(out_dir),
            "--no-incremental",
            "--no-force",
        ],
    )

    assert result.exit_code == 0
    assert "Prior balance sheet deliverable complete." in result.output
    assert str(out_dir / "Balance_Sheet_2022-12-31.csv") in result.output
