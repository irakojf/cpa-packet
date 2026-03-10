from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

import pytest

from cpapacket.core.context import RunContext
from cpapacket.deliverables.retained_earnings import RetainedEarningsDeliverable


class _Provider:
    def __init__(self) -> None:
        self.balance_sheet_calls: list[tuple[int, str]] = []
        self.pnl_calls: list[tuple[int, str]] = []
        self.gl_calls: list[tuple[int, int]] = []

    def get_balance_sheet(self, year: int, as_of: str) -> dict[str, Any]:
        self.balance_sheet_calls.append((year, as_of))
        retained_earnings = "1000.00"
        net_income = "0.00" if year == 2024 else "200.00"
        return {
            "Rows": {
                "Row": [
                    {
                        "Header": {"ColData": [{"value": "Equity"}]},
                        "Rows": {
                            "Row": [
                                {
                                    "ColData": [
                                        {"value": "Retained Earnings"},
                                        {"value": retained_earnings},
                                    ]
                                },
                                {
                                    "ColData": [
                                        {"value": "Net Income"},
                                        {"value": net_income},
                                    ]
                                }
                            ]
                        },
                    }
                ]
            }
        }

    def get_pnl(self, year: int, method: str) -> dict[str, Any]:
        self.pnl_calls.append((year, method))
        return {
            "Rows": {
                "Row": [
                    {
                        "Summary": {
                            "ColData": [
                                {"value": "Net Income"},
                                {"value": "200.00"},
                            ]
                        }
                    }
                ]
            }
        }

    def get_general_ledger(self, year: int, month: int) -> dict[str, Any]:
        self.gl_calls.append((year, month))
        if month == 1:
            rows = [
                {
                    "TxnId": "R1",
                    "TxnDate": "2025-01-10",
                    "TxnType": "JournalEntry",
                    "DocNum": "R1",
                    "AccountName": "Shareholder Distributions",
                    "AccountType": "Equity",
                    "Payee": "Owner Person",
                    "Memo": "distribution",
                    "Amount": "100.00",
                }
            ]
        else:
            rows = []
        return {"Rows": {"Row": rows}}

    def get_general_ledger_with_source(
        self,
        year: int,
        month: int,
    ) -> tuple[dict[str, Any], str]:
        return self.get_general_ledger(year, month), "api"


def _ctx(tmp_path: Path, *, no_raw: bool = False) -> RunContext:
    return RunContext(
        year=2025,
        year_source="explicit",
        out_dir=tmp_path,
        method="accrual",
        non_interactive=True,
        on_conflict="abort",
        no_raw=no_raw,
        owner_keywords=["owner"],
    )


def test_retained_earnings_deliverable_generates_artifacts_and_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _fake_write_pdf(**kwargs):  # type: ignore[no-untyped-def]
        path = Path(kwargs["path"])
        path.write_bytes(b"%PDF-1.4\n%stub\n")

    monkeypatch.setattr(
        "cpapacket.deliverables.retained_earnings.write_rollforward_pdf",
        _fake_write_pdf,
    )

    provider = _Provider()
    result = RetainedEarningsDeliverable().generate(_ctx(tmp_path), provider, prompts={})

    assert result.success is True
    assert provider.balance_sheet_calls == [(2024, "2024-12-31"), (2025, "2025-12-31")]
    assert provider.pnl_calls == [(2025, "accrual")]
    assert provider.gl_calls == [(2025, month) for month in range(1, 13)]

    csv_path = next(
        Path(path) for path in result.artifacts if path.endswith("Book_Equity_Rollforward_2025.csv")
    )
    tie_out_path = next(
        Path(path) for path in result.artifacts if path.endswith("Equity_Tie_Out_to_QBO_2025.csv")
    )
    data_path = next(Path(path) for path in result.artifacts if path.endswith("_data.json"))
    pdf_path = next(Path(path) for path in result.artifacts if path.endswith(".pdf"))
    cpa_notes_path = next(Path(path) for path in result.artifacts if path.endswith("CPA_NOTES.md"))
    metadata_path = tmp_path / "_meta" / "retained_earnings_metadata.json"

    assert csv_path.exists()
    assert tie_out_path.exists()
    assert data_path.exists()
    assert pdf_path.exists()
    assert cpa_notes_path.exists()
    assert metadata_path.exists()

    with csv_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    assert rows[0]["year"] == "2025"
    assert rows[0]["status"] == "Balanced"
    assert rows[0]["miscoded_distribution_count"] == "0"

    payload = json.loads(data_path.read_text(encoding="utf-8"))
    assert payload["year"] == 2025
    assert payload["rollforward"]["expected_ending_book_equity_bucket"] == "1200.00"
    assert payload["rollforward"]["actual_ending_book_equity_bucket"] == "1200.00"

    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert metadata["deliverable"] == "retained_earnings"
    assert metadata["schema_versions"] == {"csv": "3.0"}
    assert "input_fingerprint" in metadata


def test_retained_earnings_deliverable_skips_data_json_when_no_raw(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _fake_write_pdf(**kwargs):  # type: ignore[no-untyped-def]
        path = Path(kwargs["path"])
        path.write_bytes(b"%PDF-1.4\n%stub\n")

    monkeypatch.setattr(
        "cpapacket.deliverables.retained_earnings.write_rollforward_pdf",
        _fake_write_pdf,
    )

    provider = _Provider()
    result = RetainedEarningsDeliverable().generate(
        _ctx(tmp_path, no_raw=True),
        provider,
        prompts={},
    )

    assert result.success is True
    assert not any(path.endswith("_data.json") for path in result.artifacts)
