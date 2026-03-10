from __future__ import annotations

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


class _WritingDeliverable:
    required = True
    dependencies: list[str] = []
    requires_gusto = False

    def __init__(self, key: str, folder: str) -> None:
        self.key = key
        self.folder = folder

    def gather_prompts(self, _ctx: Any) -> dict[str, Any]:
        return {"enabled": True}

    def is_current(self, _ctx: Any) -> bool:
        return False

    def generate(
        self,
        ctx: Any,
        _store: Any,
        _prompts: dict[str, Any],
    ) -> DeliverableResult:
        artifact_rel = f"{self.folder}/{self.key}.csv"
        artifact_path = ctx.out_dir / artifact_rel
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text("col_a,col_b\nx,1\n", encoding="utf-8")
        return DeliverableResult(
            deliverable_key=self.key,
            success=True,
            artifacts=[artifact_rel],
            warnings=[],
        )


def test_cli_build_valid_args_exit_zero_and_writes_archive_and_artifacts(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    deliverables = [
        _WritingDeliverable("deliverable_one", "01_deliverable_one"),
        _WritingDeliverable("deliverable_two", "02_deliverable_two"),
    ]
    captured_manifest: dict[str, Any] = {}

    monkeypatch.setattr("cpapacket.cli.build._ensure_qbo_authenticated", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build._build_qbo_client", lambda: object())
    monkeypatch.setattr("cpapacket.cli.build._build_optional_gusto_client", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build.SessionDataStore", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build.DataProviders", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build.get_ordered_registry", lambda: deliverables)
    monkeypatch.setattr(
        "cpapacket.cli.build.validate_packet_deliverables",
        lambda **kwargs: ValidationResult(records=()),
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_validation_report",
        lambda **kwargs: tmp_path / "_meta" / "public" / "validation_report.txt",
    )

    def _capture_manifest(**kwargs: Any) -> Path:
        captured_manifest["deliverables"] = kwargs["deliverables"]
        return tmp_path / "_meta" / "public" / "packet_manifest.json"

    monkeypatch.setattr("cpapacket.cli.build.write_packet_manifest", _capture_manifest)
    monkeypatch.setattr(
        "cpapacket.cli.build.write_packet_summary",
        lambda **kwargs: tmp_path / "00_PACKET_SUMMARY.md",
    )

    def _create_zip(**kwargs: Any) -> Path:
        archive_path = tmp_path / "Packet.zip"
        archive_path.write_text("zip-bytes", encoding="utf-8")
        return archive_path

    monkeypatch.setattr("cpapacket.cli.build.create_packet_zip", _create_zip)

    result = runner.invoke(
        cli,
        ["--year", "2025", "--non-interactive", "build"],
        env=_valid_qbo_env(),
    )

    assert result.exit_code == 0
    assert (tmp_path / "Packet.zip").exists()
    assert (tmp_path / "01_deliverable_one" / "deliverable_one.csv").exists()
    assert (tmp_path / "02_deliverable_two" / "deliverable_two.csv").exists()

    manifest_statuses = {entry.key: entry.status for entry in captured_manifest["deliverables"]}
    assert manifest_statuses == {
        "deliverable_one": "success",
        "deliverable_two": "success",
    }
