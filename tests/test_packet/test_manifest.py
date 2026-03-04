from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from cpapacket.packet.manifest import DeliverableManifestEntry, write_packet_manifest


def test_write_packet_manifest_creates_expected_file(tmp_path: Path) -> None:
    started_at = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    finished_at = datetime(2026, 1, 1, 12, 5, tzinfo=UTC)

    entries = [
        DeliverableManifestEntry(
            key="pnl",
            required=True,
            status="success",
            artifacts=["01_Year-End_Profit_and_Loss/Profit_and_Loss_2025.csv"],
            timing_ms=432,
            warnings=[],
        ),
        DeliverableManifestEntry(
            key="payroll_summary",
            required=False,
            status="missing",
            artifacts=[],
            timing_ms=12,
            warnings=["Gusto auth unavailable"],
        ),
    ]

    output = write_packet_manifest(
        output_root=tmp_path,
        tool_version="0.1.0",
        run_id="run-123",
        year=2025,
        method="accrual",
        started_at=started_at,
        finished_at=finished_at,
        deliverables=entries,
    )

    assert output == tmp_path / "_meta" / "public" / "packet_manifest.json"
    assert output.exists()
    assert not list((tmp_path / "_meta" / "public").glob("*.tmp"))

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["tool_version"] == "0.1.0"
    assert payload["run_id"] == "run-123"
    assert payload["year"] == 2025
    assert payload["method"] == "accrual"
    assert payload["started_at"] == started_at.isoformat()
    assert payload["finished_at"] == finished_at.isoformat()
    assert payload["validation_summary"]["counts_by_status"] == {
        "missing": 1,
        "success": 1,
    }
    assert payload["validation_summary"]["recommended_exit_code"] == 2


def test_write_packet_manifest_recommends_zero_for_clean_run(tmp_path: Path) -> None:
    entries = [
        DeliverableManifestEntry(
            key="pnl",
            required=True,
            status="success",
            artifacts=["a.csv"],
            timing_ms=1,
            warnings=[],
        ),
        DeliverableManifestEntry(
            key="other",
            required=False,
            status="skipped",
            artifacts=[],
            timing_ms=0,
            warnings=[],
        ),
    ]

    output = write_packet_manifest(
        output_root=tmp_path,
        tool_version="0.1.0",
        run_id="run-456",
        year=2025,
        method="cash",
        started_at="2026-01-01T12:00:00Z",
        finished_at="2026-01-01T12:00:01Z",
        deliverables=entries,
    )

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["validation_summary"]["recommended_exit_code"] == 0
    assert payload["validation_summary"]["counts_by_status"] == {
        "skipped": 1,
        "success": 1,
    }
