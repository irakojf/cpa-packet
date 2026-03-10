from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

import pytest

from cpapacket.core.context import RunContext
from cpapacket.deliverables.payroll_recon import PayrollReconDeliverable


class _Provider:
    def __init__(
        self,
        *,
        payroll_runs: list[dict[str, Any]],
        accounts_payload: dict[str, Any],
    ) -> None:
        self._payroll_runs = payroll_runs
        self._accounts_payload = accounts_payload
        self.payroll_calls: list[int] = []
        self.accounts_calls = 0

    def get_payroll_runs(self, year: int) -> list[dict[str, Any]]:
        self.payroll_calls.append(year)
        return self._payroll_runs

    def get_accounts(self) -> dict[str, Any]:
        self.accounts_calls += 1
        return self._accounts_payload


def _ctx(tmp_path: Path, *, gusto_available: bool = True, no_raw: bool = False) -> RunContext:
    return RunContext(
        year=2025,
        year_source="explicit",
        out_dir=tmp_path,
        method="accrual",
        non_interactive=True,
        on_conflict="abort",
        no_raw=no_raw,
        gusto_available=gusto_available,
    )


def _sample_payroll_runs() -> list[dict[str, Any]]:
    return [
        {
            "uuid": "run-1",
            "pay_period_start_date": "2025-01-01",
            "pay_period_end_date": "2025-01-15",
            "check_date": "2025-01-20",
            "totals": {
                "gross_pay": "1000.00",
                "employer_taxes": "100.00",
            },
            "employee_compensations": [
                {
                    "employee_uuid": "emp-1",
                    "employee_name": "Alice",
                    "regular_pay": "1000.00",
                    "employee_401k": "0.00",
                    "employer_401k": "50.00",
                }
            ],
        }
    ]


def _sample_accounts_payload(*, balance: str) -> dict[str, Any]:
    return {
        "QueryResponse": {
            "Account": [
                {
                    "Name": "Payroll Expense",
                    "AccountType": "Expense",
                    "CurrentBalance": balance,
                }
            ]
        }
    }


def test_payroll_recon_deliverable_generates_outputs_and_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_write_table_report(self, output_path, **kwargs):  # type: ignore[no-untyped-def]
        del self, kwargs
        path = Path(output_path)
        path.write_bytes(b"%PDF-1.4\n%stub\n")
        return path

    monkeypatch.setattr(
        "cpapacket.deliverables.payroll_recon.PdfWriter.write_table_report",
        fake_write_table_report,
    )

    provider = _Provider(
        payroll_runs=_sample_payroll_runs(),
        accounts_payload=_sample_accounts_payload(balance="1149.98"),
    )

    deliverable = PayrollReconDeliverable()
    result = deliverable.generate(_ctx(tmp_path), provider, prompts={})

    assert result.success is True
    assert len(result.artifacts) == 3
    assert provider.payroll_calls == [2025]
    assert provider.accounts_calls == 1
    assert any("mismatch detected" in warning.lower() for warning in result.warnings)

    csv_path = Path(result.artifacts[0])
    pdf_path = Path(result.artifacts[1])
    json_path = Path(result.artifacts[2])
    metadata_path = tmp_path / "_meta" / "payroll_recon_metadata.json"

    assert csv_path.exists()
    assert pdf_path.exists()
    assert json_path.exists()
    assert metadata_path.exists()

    with csv_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    assert rows[0]["gusto_total"] == "1150.00"
    assert rows[0]["qbo_total"] == "1149.98"
    assert rows[0]["variance"] == "-0.02"
    assert rows[0]["status"] == "MISMATCH"

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["status"] == "MISMATCH"
    assert payload["tolerance"] == "0.01"

    metadata_payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert metadata_payload["deliverable"] == "payroll_recon"
    assert metadata_payload["schema_versions"] == {"csv": "1.0"}
    assert metadata_payload["artifacts"]
    assert "input_fingerprint" in metadata_payload
    assert metadata_payload["warnings"]


def test_payroll_recon_deliverable_skips_when_gusto_unavailable(tmp_path: Path) -> None:
    provider = _Provider(
        payroll_runs=_sample_payroll_runs(),
        accounts_payload=_sample_accounts_payload(balance="1150.00"),
    )

    deliverable = PayrollReconDeliverable()
    result = deliverable.generate(
        _ctx(tmp_path, gusto_available=False),
        provider,
        prompts={},
    )

    assert result.success is True
    assert result.artifacts == []
    assert "Skipped payroll reconciliation; Gusto not connected." in result.warnings
    assert provider.payroll_calls == []
    assert provider.accounts_calls == 0


def test_payroll_recon_deliverable_respects_no_raw(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_write_table_report(self, output_path, **kwargs):  # type: ignore[no-untyped-def]
        del self, kwargs
        path = Path(output_path)
        path.write_bytes(b"%PDF-1.4\n%stub\n")
        return path

    monkeypatch.setattr(
        "cpapacket.deliverables.payroll_recon.PdfWriter.write_table_report",
        fake_write_table_report,
    )

    provider = _Provider(
        payroll_runs=_sample_payroll_runs(),
        accounts_payload=_sample_accounts_payload(balance="1150.00"),
    )

    deliverable = PayrollReconDeliverable()
    result = deliverable.generate(_ctx(tmp_path, no_raw=True), provider, prompts={})

    assert result.success is True
    assert len(result.artifacts) == 2
    assert not any(path.endswith(".json") for path in result.artifacts)
