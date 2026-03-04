from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from typing import Literal

import pytest

from cpapacket.core.context import RunContext
from cpapacket.deliverables.pnl import PnlDeliverable, _write_pdf
from cpapacket.models.normalized import NormalizedRow
from cpapacket.writers.pdf_writer import PdfBodyLine


def _sample_report_payload() -> dict[str, object]:
    return {
        "Header": {
            "ReportName": "ProfitAndLoss",
            "StartPeriod": "2025-01-01",
            "EndPeriod": "2025-12-31",
        },
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


def _sample_company_payload() -> dict[str, object]:
    return {"CompanyInfo": {"CompanyName": "Acme LLC"}}


def _cross_year_long_name_payload() -> dict[str, object]:
    long_label = (
        "Consulting Revenue - Strategic Transformation and Multi-Region Advisory Services "
        "for Enterprise Accounts"
    )
    return {
        "Header": {
            "ReportName": "ProfitAndLoss",
            "StartPeriod": "2024-10-01",
            "EndPeriod": "2025-03-31",
        },
        "Rows": {
            "Row": [
                {
                    "Header": {"ColData": [{"value": "Income"}]},
                    "Rows": {
                        "Row": [
                            {
                                "ColData": [
                                    {"value": long_label},
                                    {"value": "2500.00"},
                                ]
                            },
                            {
                                "Summary": {
                                    "ColData": [
                                        {"value": "Total Income"},
                                        {"value": "2500.00"},
                                    ]
                                }
                            },
                        ]
                    },
                }
            ]
        },
    }


def _empty_report_payload() -> dict[str, object]:
    return {
        "Header": {
            "ReportName": "ProfitAndLoss",
            "StartPeriod": "2025-01-01",
            "EndPeriod": "2025-12-31",
        },
        "Rows": {"Row": []},
    }


class _StubStore:
    def __init__(
        self,
        *,
        pnl_payload: dict[str, object],
        company_payload: dict[str, object],
    ) -> None:
        self.pnl_payload = pnl_payload
        self.company_payload = company_payload

    def get_pnl(self, year: int, method: str) -> dict[str, object]:
        assert year == 2025
        assert method in {"accrual", "cash"}
        return self.pnl_payload

    def get_company_info(self) -> dict[str, object]:
        return self.company_payload


def _ctx(
    tmp_path: Path,
    *,
    on_conflict: Literal["prompt", "overwrite", "copy", "abort"] = "abort",
    no_raw: bool = False,
    redact: bool = False,
    method: Literal["accrual", "cash"] = "accrual",
) -> RunContext:
    return RunContext(
        year=2025,
        year_source="explicit",
        out_dir=tmp_path,
        method=method,
        non_interactive=True,
        on_conflict=on_conflict,
        no_raw=no_raw,
        redact=redact,
    )


def test_pnl_deliverable_generates_artifacts_and_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fake_write_pdf(
        path: Path,
        rows: list[object],
        *,
        company_name: str,
        date_range_label: str,
    ) -> None:
        assert rows
        assert company_name == "Acme LLC"
        assert date_range_label == "2025-01-01 to 2025-12-31 (accrual basis)"
        path.write_bytes(b"%PDF-1.4\n% fake test pdf\n")

    monkeypatch.setattr("cpapacket.deliverables.pnl._write_pdf", fake_write_pdf)

    deliverable = PnlDeliverable()
    store = _StubStore(
        pnl_payload=_sample_report_payload(),
        company_payload=_sample_company_payload(),
    )
    result = deliverable.generate(
        _ctx(tmp_path),
        store,
        prompts={},
    )

    assert result.success
    assert result.deliverable_key == "pnl"
    assert len(result.artifacts) == 3

    csv_path = (
        tmp_path
        / "01_Year-End_Profit_and_Loss"
        / "Profit_and_Loss_2025-01-01_to_2025-12-31_accrual.csv"
    )
    pdf_path = (
        tmp_path
        / "01_Year-End_Profit_and_Loss"
        / "Profit_and_Loss_2025-01-01_to_2025-12-31_accrual.pdf"
    )
    raw_path = (
        tmp_path
        / "01_Year-End_Profit_and_Loss"
        / "Profit_and_Loss_2025-01-01_to_2025-12-31_accrual_raw.json"
    )
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
    def fake_write_pdf(
        path: Path,
        rows: list[object],
        *,
        company_name: str,
        date_range_label: str,
    ) -> None:
        del rows, company_name, date_range_label
        path.write_bytes(b"%PDF-1.4\n% fake test pdf\n")

    monkeypatch.setattr("cpapacket.deliverables.pnl._write_pdf", fake_write_pdf)

    deliverable = PnlDeliverable()
    store = _StubStore(
        pnl_payload=_sample_report_payload(),
        company_payload=_sample_company_payload(),
    )
    result = deliverable.generate(
        _ctx(tmp_path, no_raw=True),
        store,
        prompts={},
    )

    assert result.success
    assert len(result.artifacts) == 2
    raw_path = (
        tmp_path
        / "01_Year-End_Profit_and_Loss"
        / "Profit_and_Loss_2025-01-01_to_2025-12-31_accrual_raw.json"
    )
    assert not raw_path.exists()


def test_pnl_deliverable_redacts_sensitive_raw_fields(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fake_write_pdf(
        path: Path,
        rows: list[object],
        *,
        company_name: str,
        date_range_label: str,
    ) -> None:
        del rows, company_name, date_range_label
        path.write_bytes(b"%PDF-1.4\n% fake test pdf\n")

    monkeypatch.setattr("cpapacket.deliverables.pnl._write_pdf", fake_write_pdf)

    deliverable = PnlDeliverable()
    store = _StubStore(
        pnl_payload=_sample_sensitive_report_payload(),
        company_payload=_sample_company_payload(),
    )
    deliverable.generate(
        _ctx(tmp_path, redact=True),
        store,
        prompts={},
    )

    raw_path = (
        tmp_path
        / "01_Year-End_Profit_and_Loss"
        / "Profit_and_Loss_2025-01-01_to_2025-12-31_accrual_raw.json"
    )
    raw_payload = json.loads(raw_path.read_text(encoding="utf-8"))
    assert raw_payload["Meta"]["access_token"] == "[REDACTED]"
    assert raw_payload["Meta"]["nested"]["refresh_token"] == "[REDACTED]"
    assert raw_payload["Meta"]["public_value"] == "ok"


def test_pnl_deliverable_empty_report_writes_zero_summary_and_warning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured_rows: list[NormalizedRow] = []

    def fake_write_pdf(
        path: Path,
        rows: list[NormalizedRow],
        *,
        company_name: str,
        date_range_label: str,
    ) -> None:
        del company_name, date_range_label
        captured_rows.extend(rows)
        path.write_bytes(b"%PDF-1.4\n% fake test pdf\n")

    monkeypatch.setattr("cpapacket.deliverables.pnl._write_pdf", fake_write_pdf)

    deliverable = PnlDeliverable()
    store = _StubStore(
        pnl_payload=_empty_report_payload(),
        company_payload=_sample_company_payload(),
    )
    result = deliverable.generate(_ctx(tmp_path), store, prompts={})

    assert result.success
    assert result.warnings == ["P&L report normalized to zero rows."]

    csv_path = (
        tmp_path
        / "01_Year-End_Profit_and_Loss"
        / "Profit_and_Loss_2025-01-01_to_2025-12-31_accrual.csv"
    )
    csv_lines = csv_path.read_text(encoding="utf-8").splitlines()
    assert csv_lines[-1].endswith(",total,No transactions found,0.00,No transactions found")

    assert captured_rows
    assert captured_rows[0].label == "No transactions found"
    assert captured_rows[0].row_type == "total"
    assert captured_rows[0].amount == Decimal("0")

    metadata = json.loads((tmp_path / "_meta" / "pnl_metadata.json").read_text(encoding="utf-8"))
    assert metadata["warnings"] == ["P&L report normalized to zero rows."]


def test_pnl_deliverable_honors_abort_conflict_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fake_write_pdf(
        path: Path,
        rows: list[object],
        *,
        company_name: str,
        date_range_label: str,
    ) -> None:
        del rows, company_name, date_range_label
        path.write_bytes(b"%PDF-1.4\n")

    monkeypatch.setattr("cpapacket.deliverables.pnl._write_pdf", fake_write_pdf)

    deliverable = PnlDeliverable()
    store = _StubStore(
        pnl_payload=_sample_report_payload(),
        company_payload=_sample_company_payload(),
    )
    deliverable.generate(_ctx(tmp_path, on_conflict="abort"), store, prompts={})

    with pytest.raises(FileExistsError):
        deliverable.generate(
            _ctx(tmp_path, on_conflict="abort"),
            store,
            prompts={},
        )


def test_pnl_deliverable_honors_copy_conflict_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fake_write_pdf(
        path: Path,
        rows: list[object],
        *,
        company_name: str,
        date_range_label: str,
    ) -> None:
        del rows, company_name, date_range_label
        path.write_bytes(b"%PDF-1.4\n")

    monkeypatch.setattr("cpapacket.deliverables.pnl._write_pdf", fake_write_pdf)

    deliverable = PnlDeliverable()
    store = _StubStore(
        pnl_payload=_sample_report_payload(),
        company_payload=_sample_company_payload(),
    )
    deliverable.generate(_ctx(tmp_path, on_conflict="abort"), store, prompts={})
    result = deliverable.generate(
        _ctx(tmp_path, on_conflict="copy"),
        store,
        prompts={},
    )

    copied = [Path(path) for path in result.artifacts if "__copy_" in path]
    assert copied, "expected copy-mode artifacts with __copy_ suffix"


def test_write_pdf_uses_structured_body_lines(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    captured: dict[str, object] = {}

    def fake_write_report(
        self: object,
        output_path: str | Path,
        *,
        company_name: str,
        report_title: str,
        date_range_label: str,
        body_lines: list[PdfBodyLine],
    ) -> Path:
        captured["company_name"] = company_name
        captured["report_title"] = report_title
        captured["date_range_label"] = date_range_label
        captured["body_lines"] = body_lines
        destination = Path(output_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b"%PDF-1.4\n")
        return destination

    monkeypatch.setattr(
        "cpapacket.deliverables.pnl.PdfWriter.write_report",
        fake_write_report,
    )

    rows = [
        NormalizedRow(
            section="Income",
            label="Income",
            amount=Decimal("1000.00"),
            row_type="header",
            level=0,
            path="Income",
        ),
        NormalizedRow(
            section="Income",
            label="Service Revenue",
            amount=Decimal("1000.00"),
            row_type="account",
            level=1,
            path="Income > Service Revenue",
        ),
        NormalizedRow(
            section="Income",
            label="Total Income",
            amount=Decimal("1000.00"),
            row_type="total",
            level=0,
            path="Income > Total Income",
        ),
    ]

    output_path = tmp_path / "report.pdf"
    _write_pdf(
        output_path,
        rows,
        company_name="Acme LLC",
        date_range_label="2025-01-01 to 2025-12-31 (accrual basis)",
    )

    assert output_path.exists()
    assert captured["company_name"] == "Acme LLC"
    assert captured["report_title"] == "Profit and Loss"
    assert captured["date_range_label"] == "2025-01-01 to 2025-12-31 (accrual basis)"
    lines = captured["body_lines"]
    assert isinstance(lines, list)
    assert [line.row_type for line in lines] == ["header", "account", "total"]
    assert [line.level for line in lines] == [0, 1, 0]


def test_pnl_deliverable_supports_cross_year_range_and_preserves_long_csv_labels(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict[str, str] = {}

    def fake_write_pdf(
        path: Path,
        rows: list[PdfBodyLine],
        *,
        company_name: str,
        date_range_label: str,
    ) -> None:
        del rows, company_name
        captured["date_range_label"] = date_range_label
        path.write_bytes(b"%PDF-1.4\n% fake test pdf\n")

    monkeypatch.setattr("cpapacket.deliverables.pnl._write_pdf", fake_write_pdf)

    deliverable = PnlDeliverable()
    store = _StubStore(
        pnl_payload=_cross_year_long_name_payload(),
        company_payload=_sample_company_payload(),
    )
    result = deliverable.generate(_ctx(tmp_path), store, prompts={})

    assert result.success
    csv_path = (
        tmp_path
        / "01_Year-End_Profit_and_Loss"
        / "Profit_and_Loss_2024-10-01_to_2025-03-31_accrual.csv"
    )
    assert csv_path.exists()
    csv_text = csv_path.read_text(encoding="utf-8")
    assert (
        "Consulting Revenue - Strategic Transformation and Multi-Region Advisory Services "
        "for Enterprise Accounts"
    ) in csv_text
    assert captured["date_range_label"] == "2024-10-01 to 2025-03-31 (accrual basis)"
