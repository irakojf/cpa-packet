from __future__ import annotations

from pathlib import Path
from typing import cast

from click.testing import CliRunner

from cpapacket.cli import general_ledger as general_ledger_cli
from cpapacket.cli.main import cli
from cpapacket.core.context import RunContext
from cpapacket.deliverables.base import DeliverableResult


def test_general_ledger_command_supports_local_overrides(monkeypatch, tmp_path: Path) -> None:
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
                deliverable_key="general_ledger",
                success=True,
                artifacts=[str(out_dir / "General_Ledger_2024.csv")],
            )

    monkeypatch.setattr(general_ledger_cli, "SessionDataStore", _FakeStore)
    monkeypatch.setattr(general_ledger_cli, "DataProviders", _FakeProviders)
    monkeypatch.setattr(
        general_ledger_cli,
        "GeneralLedgerDeliverable",
        lambda: _CapturingDeliverable(),
    )
    monkeypatch.setattr(general_ledger_cli, "_build_qbo_client", lambda: object())

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "general-ledger",
            "--year",
            "2024",
            "--out",
            str(out_dir),
            "--incremental",
            "--force",
        ],
    )

    assert result.exit_code == 0
    assert "General ledger deliverable complete." in result.output
    assert str(out_dir / "General_Ledger_2024.csv") in result.output
