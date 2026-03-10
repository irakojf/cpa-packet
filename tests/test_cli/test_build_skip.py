from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from click.testing import CliRunner

from cpapacket.cli.main import cli
from cpapacket.deliverables.base import DeliverableResult
from cpapacket.packet.validator import ValidationResult


def _valid_qbo_env() -> dict[str, str]:
    return {
        "CPAPACKET_QBO_CLIENT_ID": "id",
        "CPAPACKET_QBO_CLIENT_SECRET": "secret",
        "CPAPACKET_QBO_REDIRECT_URI": "https://localhost/callback",
        "CPAPACKET_QBO_REALM_ID": "realm",
    }


class _TestDeliverable:
    def __init__(self, key: str, folder: str) -> None:
        self.key = key
        self.folder = folder
        self.required = True
        self.dependencies: list[str] = []
        self.requires_gusto = False

    def gather_prompts(self, _ctx: Any) -> dict[str, Any]:
        return {}

    def is_current(self, _ctx: Any) -> bool:
        return False

    def generate(
        self,
        ctx: Any,
        _store: Any,
        _prompts: dict[str, Any],
    ) -> DeliverableResult:
        artifact = Path(ctx.out_dir) / self.folder / f"{self.key}.txt"
        artifact.parent.mkdir(parents=True, exist_ok=True)
        artifact.write_text(f"{self.key}\n", encoding="utf-8")
        return DeliverableResult(
            deliverable_key=self.key,
            success=True,
            artifacts=[str(artifact)],
            warnings=[],
        )


def test_build_skip_marks_deliverable_skipped_and_generates_remaining(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)
    captured_validation_kwargs: dict[str, Any] = {}

    monkeypatch.setattr("cpapacket.cli.build._ensure_qbo_authenticated", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build._build_qbo_client", lambda: object())
    monkeypatch.setattr("cpapacket.cli.build._build_optional_gusto_client", lambda: None)
    monkeypatch.setattr(
        "cpapacket.cli.build.get_ordered_registry",
        lambda: [
            _TestDeliverable("pnl", "01_Year-End_Profit_and_Loss"),
            _TestDeliverable("payroll_summary", "04_Annual_Payroll_Summary"),
        ],
    )

    def _validate(**kwargs: Any) -> ValidationResult:
        captured_validation_kwargs.update(kwargs)
        return ValidationResult(records=())

    monkeypatch.setattr("cpapacket.cli.build.validate_packet_deliverables", _validate)

    result = runner.invoke(
        cli,
        ["--year", "2025", "build", "--skip", "payroll_summary"],
        env=_valid_qbo_env(),
    )

    assert result.exit_code == 0
    assert "payroll_summary" in captured_validation_kwargs["skipped_keys"]

    pnl_artifact = tmp_path / "01_Year-End_Profit_and_Loss" / "pnl.txt"
    payroll_artifact = tmp_path / "04_Annual_Payroll_Summary" / "payroll_summary.txt"
    assert pnl_artifact.exists()
    assert not payroll_artifact.exists()

    manifest_path = tmp_path / "_meta" / "public" / "packet_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    statuses_by_key = {entry["key"]: entry["status"] for entry in manifest["deliverables"]}
    assert statuses_by_key["pnl"] == "success"
    assert statuses_by_key["payroll_summary"] == "skipped"

    summary_path = tmp_path / "00_PACKET_SUMMARY.md"
    summary_text = summary_path.read_text(encoding="utf-8")
    assert "pnl: success" in summary_text
    assert "payroll_summary: skipped (user_skip)" in summary_text

    zip_path = tmp_path.parent / f"{tmp_path.name}.zip"
    assert zip_path.exists()
