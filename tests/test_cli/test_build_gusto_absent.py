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


class _NonGustoDeliverable:
    key = "non_gusto"
    folder = "99_non_gusto"
    required = True
    dependencies: list[str] = []
    requires_gusto = False

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
        artifact = Path(ctx.out_dir) / self.folder / "non_gusto.txt"
        artifact.parent.mkdir(parents=True, exist_ok=True)
        artifact.write_text("ok\n", encoding="utf-8")
        return DeliverableResult(
            deliverable_key=self.key,
            success=True,
            artifacts=[str(artifact)],
            warnings=[],
        )


class _GustoOnlyDeliverable:
    key = "gusto_only"
    folder = "98_gusto_only"
    required = True
    dependencies: list[str] = []
    requires_gusto = True

    def gather_prompts(self, _ctx: Any) -> dict[str, Any]:
        raise AssertionError("gather_prompts should not run for skipped gusto deliverables")

    def is_current(self, _ctx: Any) -> bool:
        return False

    def generate(
        self,
        _ctx: Any,
        _store: Any,
        _prompts: dict[str, Any],
    ) -> DeliverableResult:
        raise AssertionError("generate should not run for skipped gusto deliverables")


def test_build_gusto_absent_skips_gusto_deliverables_and_still_packages(
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
        lambda: [_NonGustoDeliverable(), _GustoOnlyDeliverable()],
    )

    def _validate(**kwargs: Any) -> ValidationResult:
        captured_validation_kwargs.update(kwargs)
        return ValidationResult(records=())

    monkeypatch.setattr("cpapacket.cli.build.validate_packet_deliverables", _validate)

    result = runner.invoke(cli, ["--year", "2025", "build"], env=_valid_qbo_env())

    assert result.exit_code == 0
    assert (
        "WARNING: Gusto not authenticated; payroll deliverables will be skipped."
        in result.output
    )
    assert captured_validation_kwargs["gusto_available"] is False
    assert "gusto_only" in captured_validation_kwargs["skipped_keys"]

    artifact = tmp_path / "99_non_gusto" / "non_gusto.txt"
    assert artifact.exists()

    manifest_path = tmp_path / "_meta" / "public" / "packet_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    statuses_by_key = {entry["key"]: entry["status"] for entry in manifest["deliverables"]}
    assert statuses_by_key["non_gusto"] == "success"
    assert statuses_by_key["gusto_only"] == "skipped"

    summary_path = tmp_path / "00_PACKET_SUMMARY.md"
    summary_text = summary_path.read_text(encoding="utf-8")
    assert "non_gusto: success" in summary_text
    assert "gusto_only: skipped (gusto_unavailable)" in summary_text

    zip_path = tmp_path.parent / f"{tmp_path.name}.zip"
    assert zip_path.exists()
