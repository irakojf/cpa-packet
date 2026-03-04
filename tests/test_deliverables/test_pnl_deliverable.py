from __future__ import annotations

import json
from pathlib import Path

import pytest

from cpapacket.deliverables.pnl import PnlDeliverable


def _sample_report_payload() -> dict[str, object]:
    return {
        "Header": {"ReportName": "ProfitAndLoss"},
        "Rows": {
            "Row": [
                {
                    "Header": {"ColData": [{"value": "Income"}]},
                    "Rows": {
                        "Row": [
                            {
                                "ColData": [
                                    {"value": "Consulting Revenue"},
                                    {"value": "1000.00"},
                                ]
                            },
                            {
                                "Summary": {
                                    "ColData": [
                                        {"value": "Total Income"},
                                        {"value": "1000.00"},
                                    ]
                                }
                            },
                        ]
                    },
                }
            ]
        },
    }


def _sample_sensitive_report_payload() -> dict[str, object]:
    payload = _sample_report_payload()
    payload["Meta"] = {
        "access_token": "tok-123",
        "nested": {"refresh_token": "ref-456"},
        "public_value": "ok",
    }
    return payload


def test_pnl_deliverable_generates_artifacts_and_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fake_write_pdf(path: Path, rows: list[object], year: int) -> None:
        path.write_bytes(b"%PDF-1.4\n% fake test pdf\n")

    monkeypatch.setattr("cpapacket.deliverables.pnl._write_pdf", fake_write_pdf)

    deliverable = PnlDeliverable()
    result = deliverable.generate(
        report_payload=_sample_report_payload(),
        output_root=tmp_path,
        year=2025,
    )

    assert result.success
    assert result.deliverable_key == "pnl"
    assert len(result.artifacts) == 3

    csv_path = (
        tmp_path
        / "01_Year-End_Profit_and_Loss"
        / "Profit_and_Loss_2025-01-01_to_2025-12-31_accrual.csv"
    )
    pdf_path = tmp_path / "01_Year-End_Profit_and_Loss" / "Profit_and_Loss_2025.pdf"
    raw_path = tmp_path / "01_Year-End_Profit_and_Loss" / "Profit_and_Loss_2025_raw.json"
    meta_path = tmp_path / "_meta" / "pnl_metadata.json"

    assert csv_path.exists()
    assert pdf_path.exists()
    assert raw_path.exists()
    assert meta_path.exists()

    metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    assert metadata["deliverable"] == "pnl"
    assert "input_fingerprint" in metadata
    assert len(metadata["input_fingerprint"]) == 64
    assert metadata["schema_versions"] == {"csv": "1.0"}
    assert str(csv_path) in metadata["artifacts"]
    header = csv_path.read_text(encoding="utf-8").splitlines()[0]
    assert header == "section,level,row_type,label,amount,path"


def test_pnl_deliverable_respects_no_raw_flag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fake_write_pdf(path: Path, rows: list[object], year: int) -> None:
        path.write_bytes(b"%PDF-1.4\n% fake test pdf\n")

    monkeypatch.setattr("cpapacket.deliverables.pnl._write_pdf", fake_write_pdf)

    deliverable = PnlDeliverable()
    result = deliverable.generate(
        report_payload=_sample_report_payload(),
        output_root=tmp_path,
        year=2025,
        no_raw=True,
    )

    assert result.success
    assert len(result.artifacts) == 2
    raw_path = (
        tmp_path / "01_Year-End_Profit_and_Loss" / "Profit_and_Loss_2025_raw.json"
    )
    assert not raw_path.exists()


def test_pnl_deliverable_redacts_sensitive_raw_fields(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fake_write_pdf(path: Path, rows: list[object], year: int) -> None:
        path.write_bytes(b"%PDF-1.4\n% fake test pdf\n")

    monkeypatch.setattr("cpapacket.deliverables.pnl._write_pdf", fake_write_pdf)

    deliverable = PnlDeliverable()
    deliverable.generate(
        report_payload=_sample_sensitive_report_payload(),
        output_root=tmp_path,
        year=2025,
        redact=True,
    )

    raw_path = tmp_path / "01_Year-End_Profit_and_Loss" / "Profit_and_Loss_2025_raw.json"
    raw_payload = json.loads(raw_path.read_text(encoding="utf-8"))
    assert raw_payload["Meta"]["access_token"] == "[REDACTED]"
    assert raw_payload["Meta"]["nested"]["refresh_token"] == "[REDACTED]"
    assert raw_payload["Meta"]["public_value"] == "ok"


def test_pnl_deliverable_honors_abort_conflict_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fake_write_pdf(path: Path, rows: list[object], year: int) -> None:
        path.write_bytes(b"%PDF-1.4\n")

    monkeypatch.setattr("cpapacket.deliverables.pnl._write_pdf", fake_write_pdf)

    deliverable = PnlDeliverable()
    deliverable.generate(report_payload=_sample_report_payload(), output_root=tmp_path, year=2025)

    with pytest.raises(FileExistsError):
        deliverable.generate(
            report_payload=_sample_report_payload(),
            output_root=tmp_path,
            year=2025,
            on_conflict="abort",
        )


def test_pnl_deliverable_honors_copy_conflict_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fake_write_pdf(path: Path, rows: list[object], year: int) -> None:
        path.write_bytes(b"%PDF-1.4\n")

    monkeypatch.setattr("cpapacket.deliverables.pnl._write_pdf", fake_write_pdf)

    deliverable = PnlDeliverable()
    deliverable.generate(report_payload=_sample_report_payload(), output_root=tmp_path, year=2025)
    result = deliverable.generate(
        report_payload=_sample_report_payload(),
        output_root=tmp_path,
        year=2025,
        on_conflict="copy",
    )

    copied = [Path(path) for path in result.artifacts if "__copy_" in path]
    assert copied, "expected copy-mode artifacts with __copy_ suffix"
