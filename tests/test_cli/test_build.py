from __future__ import annotations

from pathlib import Path
from typing import Any

import click
from click.testing import CliRunner

from cpapacket.cli.main import cli
from cpapacket.deliverables.base import DeliverableResult
from cpapacket.packet.manifest import DeliverableManifestEntry
from cpapacket.packet.validator import DeliverableValidationRecord, ValidationResult


def _valid_qbo_env() -> dict[str, str]:
    return {
        "CPAPACKET_QBO_CLIENT_ID": "id",
        "CPAPACKET_QBO_CLIENT_SECRET": "secret",
        "CPAPACKET_QBO_REDIRECT_URI": "https://localhost/callback",
        "CPAPACKET_QBO_REALM_ID": "realm",
    }


class _FakeDeliverable:
    key = "fake"
    folder = "99_fake"
    required = True
    dependencies: list[str] = []
    requires_gusto = False

    def gather_prompts(self, _ctx: Any) -> dict[str, Any]:
        return {"mode": "default"}

    def is_current(self, _ctx: Any) -> bool:
        return False

    def generate(
        self,
        _ctx: Any,
        _store: Any,
        _prompts: dict[str, Any],
    ) -> DeliverableResult:
        return DeliverableResult(
            deliverable_key="fake",
            success=True,
            artifacts=["99_fake/fake.csv"],
            warnings=[],
        )


class _OrderedDeliverable:
    def __init__(self, key: str, calls: list[tuple[str, str]]) -> None:
        self.key = key
        self.folder = f"{key}_folder"
        self.required = True
        self.dependencies = []
        self.requires_gusto = False
        self._calls = calls

    def gather_prompts(self, _ctx: Any) -> dict[str, Any]:
        self._calls.append(("gather", self.key))
        return {"deliverable": self.key}

    def is_current(self, _ctx: Any) -> bool:
        return False

    def generate(
        self,
        _ctx: Any,
        _store: Any,
        prompts: dict[str, Any],
    ) -> DeliverableResult:
        self._calls.append(("generate", self.key))
        assert prompts["deliverable"] == self.key
        return DeliverableResult(
            deliverable_key=self.key,
            success=True,
            artifacts=[f"{self.folder}/{self.key}.csv"],
            warnings=[],
        )


class _MissingArtifactDeliverable:
    key = "missing"
    folder = "98_missing"
    required = True
    dependencies: list[str] = []
    requires_gusto = False

    def gather_prompts(self, _ctx: Any) -> dict[str, Any]:
        return {}

    def is_current(self, _ctx: Any) -> bool:
        return False

    def generate(
        self,
        _ctx: Any,
        _store: Any,
        _prompts: dict[str, Any],
    ) -> DeliverableResult:
        return DeliverableResult(
            deliverable_key="missing",
            success=True,
            artifacts=["98_missing/missing.csv"],
            warnings=[],
        )


class _ImmediateFuture:
    def __init__(self, value: tuple[DeliverableResult, int]) -> None:
        self._value = value

    def result(self) -> tuple[DeliverableResult, int]:
        return self._value


class _RecordingProgress:
    def __init__(self, capture: dict[str, Any], *columns: Any, **kwargs: Any) -> None:
        self._capture = capture
        self._capture["columns"] = columns
        self._capture["kwargs"] = kwargs

    def __enter__(self) -> _RecordingProgress:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        del exc_type, exc, tb
        return None

    def add_task(self, description: str, *, total: int) -> int:
        self._capture.setdefault("added", []).append((description, total))
        return 1

    def update(self, task_id: int, **kwargs: Any) -> None:
        self._capture.setdefault("updates", []).append((task_id, kwargs))

    def advance(self, task_id: int, advance: int = 1) -> None:
        self._capture.setdefault("advances", []).append((task_id, advance))


class _GustoDeliverable:
    key = "gusto_deliverable"
    folder = "payroll_folder"
    required = True
    dependencies: list[str] = []
    requires_gusto = True

    def gather_prompts(self, _ctx: Any) -> dict[str, Any]:
        raise AssertionError("gather_prompts should not run when gusto is unavailable")

    def is_current(self, _ctx: Any) -> bool:
        return False

    def generate(
        self,
        _ctx: Any,
        _store: Any,
        _prompts: dict[str, Any],
    ) -> DeliverableResult:
        raise AssertionError("generate should not run when gusto is unavailable")


class _FailingDeliverable:
    key = "failing"
    folder = "fail_folder"
    required = True
    dependencies: list[str] = []
    requires_gusto = False

    def gather_prompts(self, _ctx: Any) -> dict[str, Any]:
        return {}

    def is_current(self, _ctx: Any) -> bool:
        return False

    def generate(
        self,
        _ctx: Any,
        _store: Any,
        _prompts: dict[str, Any],
    ) -> DeliverableResult:
        return DeliverableResult(
            deliverable_key="failing",
            success=False,
            artifacts=[],
            warnings=[],
            error="boom",
        )


class _CurrentDeliverable:
    key = "current"
    folder = "current_folder"
    required = True
    dependencies: list[str] = []
    requires_gusto = False

    def gather_prompts(self, _ctx: Any) -> dict[str, Any]:
        raise AssertionError("gather_prompts should not run when incremental skip is active")

    def is_current(self, _ctx: Any) -> bool:
        return True

    def generate(
        self,
        _ctx: Any,
        _store: Any,
        _prompts: dict[str, Any],
    ) -> DeliverableResult:
        raise AssertionError("generate should not run when incremental skip is active")


class _ForceCurrentDeliverable(_CurrentDeliverable):
    def __init__(self, calls: list[str]) -> None:
        self._calls = calls

    def gather_prompts(self, _ctx: Any) -> dict[str, Any]:
        self._calls.append("gather")
        return {}

    def generate(
        self,
        _ctx: Any,
        _store: Any,
        _prompts: dict[str, Any],
    ) -> DeliverableResult:
        self._calls.append("generate")
        return DeliverableResult(
            deliverable_key="current",
            success=True,
            artifacts=["current_folder/current.csv"],
            warnings=[],
        )


class _IncrementalProbeDeliverable:
    def __init__(self, key: str, *, current: bool, calls: list[tuple[str, str]]) -> None:
        self.key = key
        self.folder = f"{key}_folder"
        self.required = True
        self.dependencies: list[str] = []
        self.requires_gusto = False
        self._current = current
        self._calls = calls

    def gather_prompts(self, _ctx: Any) -> dict[str, Any]:
        self._calls.append(("gather", self.key))
        return {"deliverable": self.key}

    def is_current(self, _ctx: Any) -> bool:
        return self._current

    def generate(
        self,
        _ctx: Any,
        _store: Any,
        prompts: dict[str, Any],
    ) -> DeliverableResult:
        self._calls.append(("generate", self.key))
        assert prompts["deliverable"] == self.key
        return DeliverableResult(
            deliverable_key=self.key,
            success=True,
            artifacts=[f"{self.folder}/{self.key}.csv"],
            warnings=[],
        )


def test_render_packet_tree_text_snapshots_rich_and_plain(tmp_path: Path) -> None:
    from cpapacket.cli import build as build_cli

    present_artifact = tmp_path / "99_fake" / "fake.csv"
    present_artifact.parent.mkdir(parents=True, exist_ok=True)
    present_artifact.write_text("ok\n", encoding="utf-8")

    manifest_entries = (
        DeliverableManifestEntry(
            key="fake",
            required=True,
            status="success",
            artifacts=["99_fake/fake.csv"],
            timing_ms=1,
            warnings=[],
        ),
        DeliverableManifestEntry(
            key="missing",
            required=True,
            status="error",
            artifacts=["98_missing/missing.csv"],
            timing_ms=1,
            warnings=[],
        ),
    )

    rich_text = build_cli._render_packet_tree_text(
        packet_root=tmp_path,
        manifest_entries=manifest_entries,
        plain=False,
    )
    plain_text = build_cli._render_packet_tree_text(
        packet_root=tmp_path,
        manifest_entries=manifest_entries,
        plain=True,
    )

    assert plain_text == "\n".join(
        [
            "Packet directory tree:",
            "- 98_missing/missing.csv (n/a) [Missing]",
            "- 99_fake/fake.csv (3 B) [Present]",
        ]
    )
    assert "Packet directory:" in rich_text
    assert "98_missing/" in rich_text
    assert "missing.csv" in rich_text
    assert "99_fake/" in rich_text
    assert "fake.csv" in rich_text


def test_render_validation_summary_and_status_panel_snapshots() -> None:
    from cpapacket.cli import build as build_cli

    validation = ValidationResult(
        records=(
            DeliverableValidationRecord(
                key="fake",
                required=True,
                status="missing",
                expected_patterns=("^99_fake/.+$",),
                found_files=(),
                missing_patterns=("^99_fake/.+$",),
            ),
        )
    )
    manifest_entries = (
        DeliverableManifestEntry(
            key="fake",
            required=True,
            status="error",
            artifacts=["99_fake/fake.csv"],
            timing_ms=1,
            warnings=[],
        ),
    )

    rich_summary = build_cli._render_validation_summary_text(
        validation=validation,
        manifest_entries=manifest_entries,
        plain=False,
    )
    plain_summary = build_cli._render_validation_summary_text(
        validation=validation,
        manifest_entries=manifest_entries,
        plain=True,
    )
    plain_panel = build_cli._render_status_panel_text(
        title="Warning",
        message="Needs review",
        plain=True,
        level="warning",
    )
    rich_panel = build_cli._render_status_panel_text(
        title="Warning",
        message="Needs review",
        plain=False,
        level="warning",
    )

    assert plain_summary == "\n".join(
        [
            "Validation summary:",
            "deliverable | status | files | warnings",
            "fake | missing | 0 | 1",
        ]
    )
    assert "Validation Summary" in rich_summary
    assert "fake" in rich_summary
    assert "Missing" in rich_summary
    assert plain_panel == "Warning: Needs review"
    assert "Warning" in rich_panel
    assert "Needs review" in rich_panel


def test_cli_build_requires_qbo_authentication() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["--year", "2025", "build"], env={})

    assert result.exit_code == 1
    assert "QBO authentication is required." in result.output
    assert "cpapacket auth qbo login" in result.output


def test_cli_build_runs_core_orchestration(monkeypatch: Any, tmp_path: Path) -> None:
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    monkeypatch.setattr("cpapacket.cli.build.SessionDataStore", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build.DataProviders", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build._ensure_qbo_authenticated", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build._build_qbo_client", lambda: object())
    monkeypatch.setattr("cpapacket.cli.build._build_optional_gusto_client", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build.get_ordered_registry", lambda: [_FakeDeliverable()])
    monkeypatch.setattr(
        "cpapacket.cli.build.validate_packet_deliverables",
        lambda **kwargs: ValidationResult(records=()),
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_validation_report",
        lambda **kwargs: tmp_path / "_meta" / "public" / "validation_report.txt",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_packet_manifest",
        lambda **kwargs: tmp_path / "_meta" / "public" / "packet_manifest.json",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_packet_summary",
        lambda **kwargs: tmp_path / "00_PACKET_SUMMARY.md",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.create_packet_zip",
        lambda **kwargs: tmp_path / "Packet.zip",
    )

    result = runner.invoke(cli, ["--year", "2025", "build"], env=_valid_qbo_env())

    assert result.exit_code == 0
    assert "Build complete." in result.output
    assert "validation_report.txt" in result.output
    assert "packet_manifest.json" in result.output
    assert "00_PACKET_SUMMARY.md" in result.output
    assert "Packet.zip" in result.output
    assert "deliverable(s) completed successfully." in result.output


def test_cli_build_outputs_packet_directory_tree_with_sizes_and_statuses(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)
    present_artifact = tmp_path / "99_fake" / "fake.csv"
    present_artifact.parent.mkdir(parents=True, exist_ok=True)
    present_artifact.write_text("ok\n", encoding="utf-8")

    monkeypatch.setattr("cpapacket.cli.build.SessionDataStore", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build.DataProviders", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build._ensure_qbo_authenticated", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build._build_qbo_client", lambda: object())
    monkeypatch.setattr("cpapacket.cli.build._build_optional_gusto_client", lambda: None)
    monkeypatch.setattr(
        "cpapacket.cli.build.get_ordered_registry",
        lambda: [_FakeDeliverable(), _MissingArtifactDeliverable()],
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.validate_packet_deliverables",
        lambda **kwargs: ValidationResult(records=()),
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_validation_report",
        lambda **kwargs: tmp_path / "_meta" / "public" / "validation_report.txt",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_packet_manifest",
        lambda **kwargs: tmp_path / "_meta" / "public" / "packet_manifest.json",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_packet_summary",
        lambda **kwargs: tmp_path / "00_PACKET_SUMMARY.md",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.create_packet_zip",
        lambda **kwargs: tmp_path / "Packet.zip",
    )

    result = runner.invoke(cli, ["--year", "2025", "build"], env=_valid_qbo_env())

    assert result.exit_code == 0
    assert "Packet directory:" in result.output
    assert "99_fake/" in result.output
    assert "fake.csv" in result.output
    assert "(3 B)" in result.output
    assert "Present" in result.output
    assert "98_missing/" in result.output
    assert "missing.csv" in result.output
    assert "(n/a)" in result.output
    assert "Missing" in result.output


def test_cli_build_uses_registry_order_and_prompt_generate_flow(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    calls: list[tuple[str, str]] = []
    first = _OrderedDeliverable("first", calls)
    second = _OrderedDeliverable("second", calls)

    monkeypatch.setattr("cpapacket.cli.build.SessionDataStore", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build.DataProviders", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build._ensure_qbo_authenticated", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build._build_qbo_client", lambda: object())
    monkeypatch.setattr("cpapacket.cli.build._build_optional_gusto_client", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build.get_ordered_registry", lambda: [first, second])
    monkeypatch.setattr(
        "cpapacket.cli.build.validate_packet_deliverables",
        lambda **kwargs: ValidationResult(records=()),
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_validation_report",
        lambda **kwargs: tmp_path / "_meta" / "public" / "validation_report.txt",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_packet_manifest",
        lambda **kwargs: tmp_path / "_meta" / "public" / "packet_manifest.json",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_packet_summary",
        lambda **kwargs: tmp_path / "00_PACKET_SUMMARY.md",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.create_packet_zip",
        lambda **kwargs: tmp_path / "Packet.zip",
    )

    result = runner.invoke(cli, ["--year", "2025", "build"], env=_valid_qbo_env())

    assert result.exit_code == 0
    assert calls == [
        ("gather", "first"),
        ("gather", "second"),
        ("generate", "first"),
        ("generate", "second"),
    ]


def test_cli_build_reports_progress_for_each_deliverable(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)
    calls: list[tuple[str, str]] = []
    captured: dict[str, Any] = {}
    first = _OrderedDeliverable("first", calls)
    second = _OrderedDeliverable("second", calls)

    monkeypatch.setattr("cpapacket.cli.build.SessionDataStore", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build.DataProviders", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build._ensure_qbo_authenticated", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build._build_qbo_client", lambda: object())
    monkeypatch.setattr("cpapacket.cli.build._build_optional_gusto_client", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build.get_ordered_registry", lambda: [first, second])
    monkeypatch.setattr(
        "cpapacket.cli.build.validate_packet_deliverables",
        lambda **kwargs: ValidationResult(records=()),
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_validation_report",
        lambda **kwargs: tmp_path / "_meta" / "public" / "validation_report.txt",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_packet_manifest",
        lambda **kwargs: tmp_path / "_meta" / "public" / "packet_manifest.json",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_packet_summary",
        lambda **kwargs: tmp_path / "00_PACKET_SUMMARY.md",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.create_packet_zip",
        lambda **kwargs: tmp_path / "Packet.zip",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.Progress",
        lambda *columns, **kwargs: _RecordingProgress(captured, *columns, **kwargs),
    )

    result = runner.invoke(cli, ["--year", "2025", "build"], env=_valid_qbo_env())

    assert result.exit_code == 0
    assert captured["added"] == [("Generating deliverables...", 2)]
    descriptions = [
        update_kwargs["description"]
        for _, update_kwargs in captured["updates"]
        if "description" in update_kwargs
    ]
    assert "Generating first..." in descriptions
    assert "Generating second..." in descriptions
    assert sum(advance for _, advance in captured["advances"]) == 2


def test_cli_build_plain_mode_disables_rich_progress(monkeypatch: Any, tmp_path: Path) -> None:
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)
    calls: list[tuple[str, str]] = []
    first = _OrderedDeliverable("first", calls)
    second = _OrderedDeliverable("second", calls)

    monkeypatch.setattr("cpapacket.cli.build.SessionDataStore", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build.DataProviders", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build._ensure_qbo_authenticated", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build._build_qbo_client", lambda: object())
    monkeypatch.setattr("cpapacket.cli.build._build_optional_gusto_client", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build.get_ordered_registry", lambda: [first, second])
    monkeypatch.setattr(
        "cpapacket.cli.build.validate_packet_deliverables",
        lambda **kwargs: ValidationResult(records=()),
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_validation_report",
        lambda **kwargs: tmp_path / "_meta" / "public" / "validation_report.txt",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_packet_manifest",
        lambda **kwargs: tmp_path / "_meta" / "public" / "packet_manifest.json",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_packet_summary",
        lambda **kwargs: tmp_path / "00_PACKET_SUMMARY.md",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.create_packet_zip",
        lambda **kwargs: tmp_path / "Packet.zip",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.Progress",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("Progress should not be used in plain mode")
        ),
    )

    result = runner.invoke(cli, ["--year", "2025", "--plain", "build"], env=_valid_qbo_env())

    assert result.exit_code == 0
    assert calls == [
        ("gather", "first"),
        ("gather", "second"),
        ("generate", "first"),
        ("generate", "second"),
    ]
    assert "Packet directory tree:" in result.output
    assert "Validation summary: no records." in result.output
    assert "Success: 2 deliverable(s) completed successfully." in result.output


def test_cli_build_returns_exit_code_2_when_validation_requires_review(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    monkeypatch.setattr("cpapacket.cli.build.SessionDataStore", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build.DataProviders", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build._ensure_qbo_authenticated", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build._build_qbo_client", lambda: object())
    monkeypatch.setattr("cpapacket.cli.build._build_optional_gusto_client", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build.get_ordered_registry", lambda: [_FakeDeliverable()])
    monkeypatch.setattr(
        "cpapacket.cli.build.validate_packet_deliverables",
        lambda **kwargs: ValidationResult(
            records=(
                DeliverableValidationRecord(
                    key="fake",
                    required=True,
                    status="missing",
                    expected_patterns=("^99_fake/.+$",),
                    found_files=(),
                    missing_patterns=("^99_fake/.+$",),
                ),
            )
        ),
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_validation_report",
        lambda **kwargs: tmp_path / "_meta" / "public" / "validation_report.txt",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_packet_manifest",
        lambda **kwargs: tmp_path / "_meta" / "public" / "packet_manifest.json",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_packet_summary",
        lambda **kwargs: tmp_path / "00_PACKET_SUMMARY.md",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.create_packet_zip",
        lambda **kwargs: tmp_path / "Packet.zip",
    )

    result = runner.invoke(cli, ["--year", "2025", "build"], env=_valid_qbo_env())

    assert result.exit_code == 2
    assert "Build complete." in result.output
    assert "Validation Summary" in result.output
    assert "fake" in result.output
    assert "Missing" in result.output
    assert "completed with warnings or missing artifacts" in result.output


def test_cli_build_preflight_qbo_auth_guidance(monkeypatch: Any, tmp_path: Path) -> None:
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    monkeypatch.setattr(
        "cpapacket.cli.build._ensure_qbo_authenticated",
        lambda: (_ for _ in ()).throw(
            click.ClickException(
                "QBO authentication is required. Run `cpapacket auth qbo login` and retry."
            )
        ),
    )

    result = runner.invoke(cli, ["--year", "2025", "build"], env=_valid_qbo_env())

    assert result.exit_code == 1
    assert "QBO authentication is required" in result.output
    assert "cpapacket auth qbo login" in result.output


def test_cli_build_warns_when_gusto_unavailable(monkeypatch: Any, tmp_path: Path) -> None:
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    monkeypatch.setattr("cpapacket.cli.build.SessionDataStore", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build.DataProviders", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build._ensure_qbo_authenticated", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build._build_qbo_client", lambda: object())
    monkeypatch.setattr("cpapacket.cli.build._build_optional_gusto_client", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build.get_ordered_registry", lambda: [_FakeDeliverable()])
    monkeypatch.setattr(
        "cpapacket.cli.build.validate_packet_deliverables",
        lambda **kwargs: ValidationResult(records=()),
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_validation_report",
        lambda **kwargs: tmp_path / "_meta" / "public" / "validation_report.txt",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_packet_manifest",
        lambda **kwargs: tmp_path / "_meta" / "public" / "packet_manifest.json",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_packet_summary",
        lambda **kwargs: tmp_path / "00_PACKET_SUMMARY.md",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.create_packet_zip",
        lambda **kwargs: tmp_path / "Packet.zip",
    )

    result = runner.invoke(cli, ["--year", "2025", "build"], env=_valid_qbo_env())

    assert result.exit_code == 0
    assert (
        "WARNING: Gusto not authenticated; payroll deliverables will be skipped." in result.output
    )


def test_cli_build_auto_skips_requires_gusto_deliverables(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)
    captured: dict[str, Any] = {}

    monkeypatch.setattr("cpapacket.cli.build.SessionDataStore", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build.DataProviders", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build._ensure_qbo_authenticated", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build._build_qbo_client", lambda: object())
    monkeypatch.setattr("cpapacket.cli.build._build_optional_gusto_client", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build.get_ordered_registry", lambda: [_GustoDeliverable()])

    def _validate(**kwargs: Any) -> ValidationResult:
        captured.update(kwargs)
        return ValidationResult(records=())

    monkeypatch.setattr("cpapacket.cli.build.validate_packet_deliverables", _validate)
    monkeypatch.setattr(
        "cpapacket.cli.build.write_validation_report",
        lambda **kwargs: tmp_path / "_meta" / "public" / "validation_report.txt",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_packet_manifest",
        lambda **kwargs: tmp_path / "_meta" / "public" / "packet_manifest.json",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_packet_summary",
        lambda **kwargs: tmp_path / "00_PACKET_SUMMARY.md",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.create_packet_zip",
        lambda **kwargs: tmp_path / "Packet.zip",
    )

    result = runner.invoke(cli, ["--year", "2025", "build"], env=_valid_qbo_env())

    assert result.exit_code == 0
    assert captured["gusto_available"] is False
    assert "gusto_deliverable" in captured["skipped_keys"]


def test_cli_build_writes_manifest_with_timing_fields(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)
    captured_manifest: dict[str, Any] = {}

    monkeypatch.setattr("cpapacket.cli.build.SessionDataStore", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build.DataProviders", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build._ensure_qbo_authenticated", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build._build_qbo_client", lambda: object())
    monkeypatch.setattr("cpapacket.cli.build._build_optional_gusto_client", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build.get_ordered_registry", lambda: [_FakeDeliverable()])
    monkeypatch.setattr(
        "cpapacket.cli.build.validate_packet_deliverables",
        lambda **kwargs: ValidationResult(records=()),
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_validation_report",
        lambda **kwargs: tmp_path / "_meta" / "public" / "validation_report.txt",
    )

    def _capture_manifest(**kwargs: Any) -> Path:
        captured_manifest.update(kwargs)
        return tmp_path / "_meta" / "public" / "packet_manifest.json"

    monkeypatch.setattr("cpapacket.cli.build.write_packet_manifest", _capture_manifest)
    monkeypatch.setattr(
        "cpapacket.cli.build.write_packet_summary",
        lambda **kwargs: tmp_path / "00_PACKET_SUMMARY.md",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.create_packet_zip",
        lambda **kwargs: tmp_path / "Packet.zip",
    )

    result = runner.invoke(cli, ["--year", "2025", "build"], env=_valid_qbo_env())

    assert result.exit_code == 0
    assert "started_at" in captured_manifest
    assert "finished_at" in captured_manifest
    deliverables = captured_manifest["deliverables"]
    assert len(deliverables) == 1
    assert deliverables[0].key == "fake"
    assert deliverables[0].timing_ms >= 0


def test_cli_build_returns_exit_code_1_on_hard_deliverable_failure(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    monkeypatch.setattr("cpapacket.cli.build.SessionDataStore", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build.DataProviders", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build._ensure_qbo_authenticated", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build._build_qbo_client", lambda: object())
    monkeypatch.setattr("cpapacket.cli.build._build_optional_gusto_client", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build.get_ordered_registry", lambda: [_FailingDeliverable()])

    result = runner.invoke(cli, ["--year", "2025", "build"], env=_valid_qbo_env())

    assert result.exit_code == 1
    assert "boom" in result.output
    assert "deliverable(s) failed." in result.output


def test_cli_build_continue_on_failure_runs_remaining_deliverables(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)
    calls: list[tuple[str, str]] = []

    monkeypatch.setattr("cpapacket.cli.build.SessionDataStore", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build.DataProviders", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build._ensure_qbo_authenticated", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build._build_qbo_client", lambda: object())
    monkeypatch.setattr("cpapacket.cli.build._build_optional_gusto_client", lambda: None)
    monkeypatch.setattr(
        "cpapacket.cli.build.get_ordered_registry",
        lambda: [_FailingDeliverable(), _OrderedDeliverable("after_fail", calls)],
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.validate_packet_deliverables",
        lambda **kwargs: ValidationResult(records=()),
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_validation_report",
        lambda **kwargs: tmp_path / "_meta" / "public" / "validation_report.txt",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_packet_manifest",
        lambda **kwargs: tmp_path / "_meta" / "public" / "packet_manifest.json",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_packet_summary",
        lambda **kwargs: tmp_path / "00_PACKET_SUMMARY.md",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.create_packet_zip",
        lambda **kwargs: tmp_path / "Packet.zip",
    )

    result = runner.invoke(cli, ["--year", "2025", "build"], env=_valid_qbo_env())

    assert result.exit_code == 1
    assert ("generate", "after_fail") in calls


def test_cli_build_partial_failure_still_writes_zip_and_validation_report(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)
    calls: list[tuple[str, str]] = []
    captured: dict[str, int] = {"zip_calls": 0, "validation_calls": 0}

    monkeypatch.setattr("cpapacket.cli.build.SessionDataStore", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build.DataProviders", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build._ensure_qbo_authenticated", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build._build_qbo_client", lambda: object())
    monkeypatch.setattr("cpapacket.cli.build._build_optional_gusto_client", lambda: None)
    monkeypatch.setattr(
        "cpapacket.cli.build.get_ordered_registry",
        lambda: [_FailingDeliverable(), _OrderedDeliverable("after_fail", calls)],
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.validate_packet_deliverables",
        lambda **kwargs: ValidationResult(records=()),
    )

    def _capture_validation(**kwargs: Any) -> Path:
        del kwargs
        captured["validation_calls"] += 1
        return tmp_path / "_meta" / "public" / "validation_report.txt"

    def _capture_zip(**kwargs: Any) -> Path:
        del kwargs
        captured["zip_calls"] += 1
        return tmp_path / "Packet.zip"

    monkeypatch.setattr("cpapacket.cli.build.write_validation_report", _capture_validation)
    monkeypatch.setattr(
        "cpapacket.cli.build.write_packet_manifest",
        lambda **kwargs: tmp_path / "_meta" / "public" / "packet_manifest.json",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_packet_summary",
        lambda **kwargs: tmp_path / "00_PACKET_SUMMARY.md",
    )
    monkeypatch.setattr("cpapacket.cli.build.create_packet_zip", _capture_zip)

    result = runner.invoke(cli, ["--year", "2025", "build"], env=_valid_qbo_env())

    assert result.exit_code == 1
    assert ("generate", "after_fail") in calls
    assert captured["validation_calls"] == 1
    assert captured["zip_calls"] == 1


def test_cli_build_exit_code_1_takes_precedence_over_validation_missing(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)
    zip_calls = 0

    monkeypatch.setattr("cpapacket.cli.build.SessionDataStore", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build.DataProviders", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build._ensure_qbo_authenticated", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build._build_qbo_client", lambda: object())
    monkeypatch.setattr("cpapacket.cli.build._build_optional_gusto_client", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build.get_ordered_registry", lambda: [_FailingDeliverable()])
    monkeypatch.setattr(
        "cpapacket.cli.build.validate_packet_deliverables",
        lambda **kwargs: ValidationResult(
            records=(
                DeliverableValidationRecord(
                    key="failing",
                    required=True,
                    status="missing",
                    expected_patterns=("^fail_folder/.+$",),
                    found_files=(),
                    missing_patterns=("^fail_folder/.+$",),
                ),
            )
        ),
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_validation_report",
        lambda **kwargs: tmp_path / "_meta" / "public" / "validation_report.txt",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_packet_manifest",
        lambda **kwargs: tmp_path / "_meta" / "public" / "packet_manifest.json",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_packet_summary",
        lambda **kwargs: tmp_path / "00_PACKET_SUMMARY.md",
    )

    def _capture_zip(**kwargs: Any) -> Path:
        del kwargs
        nonlocal zip_calls
        zip_calls += 1
        return tmp_path / "Packet.zip"

    monkeypatch.setattr("cpapacket.cli.build.create_packet_zip", _capture_zip)

    result = runner.invoke(cli, ["--year", "2025", "build"], env=_valid_qbo_env())

    assert result.exit_code == 1
    assert zip_calls == 1


def test_cli_build_skip_flag_skips_selected_deliverable(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)
    captured: dict[str, Any] = {}

    monkeypatch.setattr("cpapacket.cli.build.SessionDataStore", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build.DataProviders", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build._ensure_qbo_authenticated", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build._build_qbo_client", lambda: object())
    monkeypatch.setattr("cpapacket.cli.build._build_optional_gusto_client", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build.get_ordered_registry", lambda: [_FailingDeliverable()])

    def _capture_manifest(**kwargs: Any) -> Path:
        captured.update(kwargs)
        return tmp_path / "_meta" / "public" / "packet_manifest.json"

    monkeypatch.setattr("cpapacket.cli.build.write_packet_manifest", _capture_manifest)
    monkeypatch.setattr(
        "cpapacket.cli.build.validate_packet_deliverables",
        lambda **kwargs: ValidationResult(records=()),
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_validation_report",
        lambda **kwargs: tmp_path / "_meta" / "public" / "validation_report.txt",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_packet_summary",
        lambda **kwargs: tmp_path / "00_PACKET_SUMMARY.md",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.create_packet_zip",
        lambda **kwargs: tmp_path / "Packet.zip",
    )

    result = runner.invoke(
        cli,
        ["--year", "2025", "build", "--skip", "failing"],
        env=_valid_qbo_env(),
    )

    assert result.exit_code == 0
    assert captured["deliverables"][0].status == "skipped"


def test_cli_build_validate_only_bypasses_generation_flow(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    monkeypatch.setattr(
        "cpapacket.cli.build._ensure_qbo_authenticated",
        lambda: (_ for _ in ()).throw(AssertionError("auth should not run in validate-only")),
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.get_ordered_registry",
        lambda: (_ for _ in ()).throw(AssertionError("registry should not run in validate-only")),
    )
    monkeypatch.setattr(
        "cpapacket.cli.build._build_qbo_client",
        lambda: (_ for _ in ()).throw(AssertionError("qbo client should not run in validate-only")),
    )
    monkeypatch.setattr(
        "cpapacket.cli.build._build_optional_gusto_client",
        lambda: (_ for _ in ()).throw(
            AssertionError("gusto client should not run in validate-only")
        ),
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.validate_packet_deliverables",
        lambda **kwargs: ValidationResult(records=()),
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_validation_report",
        lambda **kwargs: tmp_path / "_meta" / "public" / "validation_report.txt",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.create_packet_zip",
        lambda **kwargs: tmp_path / "Packet.zip",
    )

    result = runner.invoke(
        cli,
        ["--year", "2025", "build", "--validate-only"],
        env=_valid_qbo_env(),
    )

    assert result.exit_code == 0
    assert "Validation-only complete." in result.output
    assert "validation_report.txt" in result.output
    assert "Packet.zip" in result.output


def test_cli_build_incremental_skips_current_deliverable(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)
    captured_manifest: dict[str, Any] = {}

    metadata_dir = tmp_path / "_meta"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    (metadata_dir / "current_metadata.json").write_text(
        '{"artifacts":["current_folder/current.csv"]}',
        encoding="utf-8",
    )
    artifact_path = tmp_path / "current_folder" / "current.csv"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text("header\n", encoding="utf-8")

    monkeypatch.setattr("cpapacket.cli.build.SessionDataStore", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build.DataProviders", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build._ensure_qbo_authenticated", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build._build_qbo_client", lambda: object())
    monkeypatch.setattr("cpapacket.cli.build._build_optional_gusto_client", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build.get_ordered_registry", lambda: [_CurrentDeliverable()])
    monkeypatch.setattr(
        "cpapacket.cli.build.validate_packet_deliverables",
        lambda **kwargs: ValidationResult(records=()),
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_validation_report",
        lambda **kwargs: tmp_path / "_meta" / "public" / "validation_report.txt",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_packet_summary",
        lambda **kwargs: tmp_path / "00_PACKET_SUMMARY.md",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.create_packet_zip",
        lambda **kwargs: tmp_path / "Packet.zip",
    )

    def _capture_manifest(**kwargs: Any) -> Path:
        captured_manifest.update(kwargs)
        return tmp_path / "_meta" / "public" / "packet_manifest.json"

    monkeypatch.setattr("cpapacket.cli.build.write_packet_manifest", _capture_manifest)

    result = runner.invoke(cli, ["--year", "2025", "--incremental", "build"], env=_valid_qbo_env())

    assert result.exit_code == 0
    deliverables = captured_manifest["deliverables"]
    assert len(deliverables) == 1
    assert deliverables[0].status == "skipped"
    assert deliverables[0].artifacts == ["current_folder/current.csv"]


def test_cli_build_force_overrides_incremental_skip(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)
    calls: list[str] = []

    monkeypatch.setattr("cpapacket.cli.build.SessionDataStore", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build.DataProviders", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build._ensure_qbo_authenticated", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build._build_qbo_client", lambda: object())
    monkeypatch.setattr("cpapacket.cli.build._build_optional_gusto_client", lambda: None)
    monkeypatch.setattr(
        "cpapacket.cli.build.get_ordered_registry",
        lambda: [_ForceCurrentDeliverable(calls)],
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.validate_packet_deliverables",
        lambda **kwargs: ValidationResult(records=()),
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_validation_report",
        lambda **kwargs: tmp_path / "_meta" / "public" / "validation_report.txt",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_packet_manifest",
        lambda **kwargs: tmp_path / "_meta" / "public" / "packet_manifest.json",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_packet_summary",
        lambda **kwargs: tmp_path / "00_PACKET_SUMMARY.md",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.create_packet_zip",
        lambda **kwargs: tmp_path / "Packet.zip",
    )

    result = runner.invoke(
        cli,
        ["--year", "2025", "--incremental", "--force", "build"],
        env=_valid_qbo_env(),
    )

    assert result.exit_code == 0
    assert calls == ["gather", "generate"]


def test_cli_build_incremental_regenerates_stale_deliverables(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)
    captured_manifest: dict[str, Any] = {}
    calls: list[tuple[str, str]] = []

    current = _IncrementalProbeDeliverable("current", current=True, calls=calls)
    stale = _IncrementalProbeDeliverable("stale", current=False, calls=calls)

    metadata_dir = tmp_path / "_meta"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    (metadata_dir / "current_metadata.json").write_text(
        '{"artifacts":["current_folder/current.csv"]}',
        encoding="utf-8",
    )
    artifact_path = tmp_path / "current_folder" / "current.csv"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text("header\n", encoding="utf-8")

    monkeypatch.setattr("cpapacket.cli.build.SessionDataStore", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build.DataProviders", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build._ensure_qbo_authenticated", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build._build_qbo_client", lambda: object())
    monkeypatch.setattr("cpapacket.cli.build._build_optional_gusto_client", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build.get_ordered_registry", lambda: [current, stale])
    monkeypatch.setattr(
        "cpapacket.cli.build.validate_packet_deliverables",
        lambda **kwargs: ValidationResult(records=()),
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_validation_report",
        lambda **kwargs: tmp_path / "_meta" / "public" / "validation_report.txt",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_packet_summary",
        lambda **kwargs: tmp_path / "00_PACKET_SUMMARY.md",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.create_packet_zip",
        lambda **kwargs: tmp_path / "Packet.zip",
    )

    def _capture_manifest(**kwargs: Any) -> Path:
        captured_manifest.update(kwargs)
        return tmp_path / "_meta" / "public" / "packet_manifest.json"

    monkeypatch.setattr("cpapacket.cli.build.write_packet_manifest", _capture_manifest)

    result = runner.invoke(cli, ["--year", "2025", "--incremental", "build"], env=_valid_qbo_env())

    assert result.exit_code == 0
    assert calls == [("gather", "stale"), ("generate", "stale")]
    entries = {entry.key: entry for entry in captured_manifest["deliverables"]}
    assert entries["current"].status == "skipped"
    assert entries["stale"].status == "success"


def test_cli_build_force_bypasses_disk_cache_dir(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)
    captured_cache_dirs: list[Path | None] = []

    def _capture_store(*args: Any, **kwargs: Any) -> object:
        del args
        captured_cache_dirs.append(kwargs.get("cache_dir"))
        return object()

    monkeypatch.setattr("cpapacket.cli.build.SessionDataStore", _capture_store)
    monkeypatch.setattr("cpapacket.cli.build.DataProviders", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build._ensure_qbo_authenticated", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build._build_qbo_client", lambda: object())
    monkeypatch.setattr("cpapacket.cli.build._build_optional_gusto_client", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build.get_ordered_registry", lambda: [_FakeDeliverable()])
    monkeypatch.setattr(
        "cpapacket.cli.build.validate_packet_deliverables",
        lambda **kwargs: ValidationResult(records=()),
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_validation_report",
        lambda **kwargs: tmp_path / "_meta" / "public" / "validation_report.txt",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_packet_manifest",
        lambda **kwargs: tmp_path / "_meta" / "public" / "packet_manifest.json",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_packet_summary",
        lambda **kwargs: tmp_path / "00_PACKET_SUMMARY.md",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.create_packet_zip",
        lambda **kwargs: tmp_path / "Packet.zip",
    )

    result = runner.invoke(cli, ["--year", "2025", "--force", "build"], env=_valid_qbo_env())

    assert result.exit_code == 0
    assert captured_cache_dirs == [None]


def test_cli_build_runs_concurrent_step_keys_via_thread_pool(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)
    calls: list[tuple[str, str]] = []
    submitted: list[str] = []
    pool_meta: dict[str, int] = {}

    class _FakeExecutor:
        def __init__(self, *, max_workers: int) -> None:
            pool_meta["max_workers"] = max_workers

        def __enter__(self) -> _FakeExecutor:
            return self

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            del exc_type, exc, tb

        def submit(self, fn: Any, **kwargs: Any) -> _ImmediateFuture:
            submitted.append(kwargs["deliverable"].key)
            return _ImmediateFuture(fn(**kwargs))

    monkeypatch.setattr("cpapacket.cli.build.ThreadPoolExecutor", _FakeExecutor)
    monkeypatch.setattr("cpapacket.cli.build.SessionDataStore", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build.DataProviders", lambda *a, **k: object())
    monkeypatch.setattr("cpapacket.cli.build._ensure_qbo_authenticated", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build._build_qbo_client", lambda: object())
    monkeypatch.setattr("cpapacket.cli.build._build_optional_gusto_client", lambda: None)
    monkeypatch.setattr(
        "cpapacket.cli.build.get_ordered_registry",
        lambda: [
            _OrderedDeliverable("pnl", calls),
            _OrderedDeliverable("general_ledger", calls),
            _OrderedDeliverable("contractor", calls),
        ],
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.validate_packet_deliverables",
        lambda **kwargs: ValidationResult(records=()),
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_validation_report",
        lambda **kwargs: tmp_path / "_meta" / "public" / "validation_report.txt",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_packet_manifest",
        lambda **kwargs: tmp_path / "_meta" / "public" / "packet_manifest.json",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.write_packet_summary",
        lambda **kwargs: tmp_path / "00_PACKET_SUMMARY.md",
    )
    monkeypatch.setattr(
        "cpapacket.cli.build.create_packet_zip",
        lambda **kwargs: tmp_path / "Packet.zip",
    )

    result = runner.invoke(cli, ["--year", "2025", "build"], env=_valid_qbo_env())

    assert result.exit_code == 0
    assert pool_meta["max_workers"] == 2
    assert submitted == ["pnl", "general_ledger"]
