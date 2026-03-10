from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from cpapacket.core.context import RunContext
from cpapacket.deliverables.tax_tracker import TaxTrackerDeliverable


def _ctx(out_dir: Path) -> RunContext:
    return RunContext(
        year=2025,
        year_source="explicit",
        out_dir=out_dir,
        method="accrual",
        non_interactive=True,
        on_conflict="abort",
    )


def _stub_pdf_writer(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_write_table_report(self, output_path, **kwargs):  # type: ignore[no-untyped-def]
        del self, kwargs
        path = Path(output_path)
        path.write_bytes(b"%PDF-1.4\n%stub\n")
        return path

    monkeypatch.setattr(
        "cpapacket.deliverables.tax_tracker.PdfWriter.write_table_report",
        fake_write_table_report,
    )


def test_tax_tracker_deliverable_writes_outputs_and_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_pdf_writer(monkeypatch)
    config_root = tmp_path / "config"
    config_root.mkdir(parents=True)

    tracker_source = config_root / "tax_tracker_2025.json"
    tracker_source.write_text(
        json.dumps(
            [
                {
                    "jurisdiction": "Federal",
                    "due_date": "2025-04-15",
                    "amount": "1200.00",
                    "status": "not_paid",
                    "paid_date": None,
                    "last_updated": "2025-01-10T09:30:00Z",
                },
                {
                    "jurisdiction": "NY",
                    "due_date": "2025-06-16",
                    "amount": "400.00",
                    "status": "paid",
                    "paid_date": "2025-06-01",
                    "last_updated": "2025-06-01T12:00:00Z",
                },
            ]
        ),
        encoding="utf-8",
    )

    deadlines_source = config_root / "tax_deadlines_2025.json"
    deadlines_source.write_text(
        json.dumps(
            [
                {
                    "jurisdiction": "Federal",
                    "name": "Q1 Estimated",
                    "due_date": "2025-04-15",
                    "category": "estimated_tax",
                    "completed": False,
                },
                {
                    "jurisdiction": "DE",
                    "name": "Franchise Tax",
                    "due_date": "2025-03-01",
                    "category": "filing",
                    "completed": True,
                },
            ]
        ),
        encoding="utf-8",
    )

    out_dir = tmp_path / "packet"
    deliverable = TaxTrackerDeliverable(config_root=config_root)
    result = deliverable.generate(_ctx(out_dir), object(), prompts={})

    assert result.success is True
    assert result.warnings == []

    deliverable_dir = out_dir / "08_Estimated_Tax_Payments"
    metadata_path = out_dir / "_meta" / "estimated_tax_metadata.json"
    expected_paths = {
        deliverable_dir / "cpa" / "estimated_tax_tracker_2025.csv",
        deliverable_dir / "cpa" / "estimated_tax_tracker_2025.pdf",
        deliverable_dir / "cpa" / "tax_deadlines_2025.csv",
        deliverable_dir / "cpa" / "tax_deadlines_2025.pdf",
        deliverable_dir / "dev" / "tax_tracker_2025.json",
        deliverable_dir / "dev" / "tax_deadlines_2025.json",
        out_dir / "_meta" / "tax_tracker_2025.json",
        out_dir / "_meta" / "tax_deadlines_2025.json",
    }

    assert {Path(item) for item in result.artifacts} == expected_paths
    for expected_path in expected_paths:
        assert expected_path.exists()
    assert metadata_path.exists()

    with (deliverable_dir / "cpa" / "estimated_tax_tracker_2025.csv").open(
        newline="",
        encoding="utf-8",
    ) as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 2
    assert rows[0]["amount"] == "1200.00"
    assert rows[1]["status"] == "paid"

    with (deliverable_dir / "cpa" / "tax_deadlines_2025.csv").open(newline="", encoding="utf-8") as handle:
        deadline_rows = list(csv.DictReader(handle))
    assert len(deadline_rows) == 2
    assert deadline_rows[0]["name"] == "Franchise Tax"
    assert deadline_rows[1]["name"] == "Q1 Estimated"

    metadata_payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert metadata_payload["deliverable"] == "estimated_tax"
    assert metadata_payload["schema_versions"] == {"csv": "1.0"}
    assert metadata_payload["inputs"]["tracker_entry_count"] == 2
    assert metadata_payload["inputs"]["deadline_entry_count"] == 2
    assert sorted(metadata_payload["artifacts"]) == sorted(result.artifacts)


def test_tax_tracker_deliverable_handles_missing_sources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_pdf_writer(monkeypatch)
    config_root = tmp_path / "config"
    config_root.mkdir(parents=True)

    out_dir = tmp_path / "packet"
    deliverable = TaxTrackerDeliverable(config_root=config_root)
    result = deliverable.generate(_ctx(out_dir), object(), prompts={})

    assert result.success is True
    assert len(result.warnings) == 2
    assert all("not found" in warning.lower() for warning in result.warnings)

    artifact_paths = [Path(item) for item in result.artifacts]
    assert len(artifact_paths) == 4
    assert all(path.exists() for path in artifact_paths)
    assert not any(path.name.endswith(".json") for path in artifact_paths)

    tracker_csv = out_dir / "08_Estimated_Tax_Payments" / "cpa" / "estimated_tax_tracker_2025.csv"
    with tracker_csv.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows == []
