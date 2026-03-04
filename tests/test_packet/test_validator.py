from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from cpapacket.core.metadata import DeliverableMetadata, write_deliverable_metadata
from cpapacket.deliverables.base import Deliverable, DeliverableResult
from cpapacket.packet.validator import (
    render_validation_report,
    validate_packet_deliverables,
    write_validation_report,
)


@dataclass(frozen=True)
class _FakeDeliverable:
    key: str
    folder: str
    required: bool = True
    dependencies: list[str] = field(default_factory=list)
    requires_gusto: bool = False

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
        return DeliverableResult(deliverable_key=self.key, success=True)


def _write_metadata(
    root: Path,
    *,
    key: str,
    artifacts: list[str],
) -> None:
    metadata = DeliverableMetadata(
        deliverable=key,
        generated_at=datetime(2026, 1, 15, tzinfo=UTC),
        inputs={"year": 2025},
        input_fingerprint="abc123",
        schema_versions={"csv": "1.0"},
        artifacts=artifacts,
    )
    path = root / "_meta" / f"{key}_metadata.json"
    write_deliverable_metadata(path, metadata)


def _registry(*deliverables: _FakeDeliverable) -> tuple[Deliverable, ...]:
    return cast(tuple[Deliverable, ...], deliverables)


def test_validate_packet_deliverables_present_when_metadata_artifacts_exist(
    tmp_path: Path,
) -> None:
    root = tmp_path / "packet"
    (root / "01_Year-End_Profit_and_Loss").mkdir(parents=True, exist_ok=True)
    csv_path = root / "01_Year-End_Profit_and_Loss" / "pnl.csv"
    csv_path.write_text("header\n", encoding="utf-8")
    _write_metadata(root, key="pnl", artifacts=[csv_path.relative_to(root).as_posix()])

    result = validate_packet_deliverables(
        packet_root=root,
        registry=_registry(_FakeDeliverable(key="pnl", folder="01_Year-End_Profit_and_Loss")),
    )

    assert len(result.records) == 1
    record = result.records[0]
    assert record.status == "present"
    assert record.found_files == ("01_Year-End_Profit_and_Loss/pnl.csv",)
    assert record.missing_patterns == ()
    assert result.counts_by_status()["present"] == 1


def test_validate_packet_deliverables_incomplete_when_some_artifacts_missing(
    tmp_path: Path,
) -> None:
    root = tmp_path / "packet"
    (root / "01_Year-End_Profit_and_Loss").mkdir(parents=True, exist_ok=True)
    present_csv = root / "01_Year-End_Profit_and_Loss" / "pnl.csv"
    present_csv.write_text("header\n", encoding="utf-8")
    _write_metadata(
        root,
        key="pnl",
        artifacts=[
            "01_Year-End_Profit_and_Loss/pnl.csv",
            "01_Year-End_Profit_and_Loss/pnl.pdf",
        ],
    )

    result = validate_packet_deliverables(
        packet_root=root,
        registry=_registry(_FakeDeliverable(key="pnl", folder="01_Year-End_Profit_and_Loss")),
    )

    record = result.records[0]
    assert record.status == "incomplete"
    assert any(pattern.endswith(r"pnl\.pdf$") for pattern in record.missing_patterns)


def test_validate_packet_deliverables_missing_when_no_metadata_or_files(
    tmp_path: Path,
) -> None:
    root = tmp_path / "packet"
    root.mkdir(parents=True, exist_ok=True)

    result = validate_packet_deliverables(
        packet_root=root,
        registry=_registry(_FakeDeliverable(key="pnl", folder="01_Year-End_Profit_and_Loss")),
    )

    record = result.records[0]
    assert record.status == "missing"


def test_validate_packet_deliverables_skips_for_flagged_or_gusto_unavailable(
    tmp_path: Path,
) -> None:
    root = tmp_path / "packet"
    root.mkdir(parents=True, exist_ok=True)

    result = validate_packet_deliverables(
        packet_root=root,
        registry=_registry(
            _FakeDeliverable(key="pnl", folder="01_Year-End_Profit_and_Loss"),
            _FakeDeliverable(
                key="payroll_summary",
                folder="04_Annual_Payroll_Summary",
                requires_gusto=True,
            ),
        ),
        skipped_keys={"pnl"},
        gusto_available=False,
    )

    by_key = {item.key: item for item in result.records}
    assert by_key["pnl"].status == "skipped"
    assert by_key["payroll_summary"].status == "skipped"


def test_validate_packet_deliverables_uses_folder_regex_without_metadata(
    tmp_path: Path,
) -> None:
    root = tmp_path / "packet"
    folder = root / "03_Full-Year_General_Ledger"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "general_ledger_2025.csv").write_text("header\n", encoding="utf-8")

    result = validate_packet_deliverables(
        packet_root=root,
        registry=_registry(
            _FakeDeliverable(key="general_ledger", folder="03_Full-Year_General_Ledger")
        ),
    )

    record = result.records[0]
    assert record.status == "incomplete"
    assert record.found_files == ("03_Full-Year_General_Ledger/general_ledger_2025.csv",)


def test_render_validation_report_includes_counts_and_review_flags(tmp_path: Path) -> None:
    root = tmp_path / "packet"
    root.mkdir(parents=True, exist_ok=True)
    result = validate_packet_deliverables(
        packet_root=root,
        registry=_registry(_FakeDeliverable(key="pnl", folder="01_Year-End_Profit_and_Loss")),
    )

    report = render_validation_report(result)

    assert "CPA Packet Validation Report" in report
    assert "Missing: 1" in report
    assert "Review Required: YES" in report
    assert "- pnl [MISSING]" in report


def test_write_validation_report_outputs_meta_public_file(tmp_path: Path) -> None:
    root = tmp_path / "packet"
    root.mkdir(parents=True, exist_ok=True)
    result = validate_packet_deliverables(
        packet_root=root,
        registry=_registry(_FakeDeliverable(key="pnl", folder="01_Year-End_Profit_and_Loss")),
    )

    report_path = write_validation_report(output_root=root, result=result)

    assert report_path == root / "_meta" / "public" / "validation_report.txt"
    assert report_path.exists()
    text = report_path.read_text(encoding="utf-8")
    assert "Deliverables" in text
    assert "pnl" in text
