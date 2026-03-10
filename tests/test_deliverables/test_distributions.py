from __future__ import annotations

import csv
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from cpapacket.core.context import RunContext
from cpapacket.data.providers import DataProviders
from cpapacket.data.store import SessionDataStore
from cpapacket.deliverables.distributions import DistributionsDeliverable


class _Provider:
    def __init__(self, *, months: dict[int, list[dict[str, Any]]] | None = None) -> None:
        self._months = months or {}
        self.calls: list[tuple[int, int]] = []

    def get_general_ledger(self, year: int, month: int) -> dict[str, Any]:
        self.calls.append((year, month))
        return {"Rows": {"Row": self._months.get(month, [])}}

    def get_general_ledger_with_source(
        self,
        year: int,
        month: int,
    ) -> tuple[dict[str, Any], str]:
        return self.get_general_ledger(year, month), "api"

    def get_company_info(self) -> dict[str, Any]:
        return {"CompanyInfo": {"LegalName": "Ira Ko LLC"}}

    def get_balance_sheet(self, year: int, as_of: str) -> dict[str, Any]:
        del year, as_of
        return {"Rows": {"Row": []}}


class _HttpQboClient:
    def __init__(self, client: httpx.Client) -> None:
        self._client = client
        self._config = type("Config", (), {"realm_id": "test-realm"})()

    def request(
        self,
        method: str,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> httpx.Response:
        return self._client.request(method, endpoint, params=params, json=json_body)


def _ctx(
    tmp_path: Path,
    *,
    owner_keywords: list[str] | None = None,
    no_raw: bool = False,
) -> RunContext:
    return RunContext(
        year=2025,
        year_source="explicit",
        out_dir=tmp_path,
        method="accrual",
        non_interactive=True,
        on_conflict="abort",
        no_raw=no_raw,
        owner_keywords=owner_keywords or [],
    )


def test_distributions_deliverable_generates_summary_and_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_write_report(self, output_path, **kwargs):  # type: ignore[no-untyped-def]
        del self, kwargs
        path = Path(output_path)
        path.write_bytes(b"%PDF-1.4\n%stub\n")
        return path

    monkeypatch.setattr(
        "cpapacket.deliverables.distributions.PdfWriter.write_report",
        fake_write_report,
    )

    month_rows: dict[int, list[dict[str, Any]]] = {}
    for month in range(1, 13):
        month_rows[month] = [
            {
                "TxnId": f"D-{month}",
                "TxnDate": f"2025-{month:02d}-15",
                "TxnType": "JournalEntry",
                "DocNum": f"DOC-{month}",
                "AccountName": "Shareholder Distributions",
                "AccountType": "Equity",
                "Payee": "Owner Person",
                "Memo": "distribution",
                "Amount": "100.00",
            }
        ]

    provider = _Provider(months=month_rows)
    deliverable = DistributionsDeliverable()
    result = deliverable.generate(_ctx(tmp_path, owner_keywords=["owner"]), provider, prompts={})

    assert result.success is True
    assert len(result.artifacts) == 6
    assert provider.calls == [(2025, month) for month in range(1, 13)]

    summary_csv = Path(result.artifacts[0])
    summary_pdf = Path(result.artifacts[1])
    miscoded_csv = Path(result.artifacts[2])
    activity_csv = next(
        Path(path) for path in result.artifacts if path.endswith("distribution_activity_2025.csv")
    )
    bridge_csv = next(
        Path(path)
        for path in result.artifacts
        if path.endswith("distribution_balance_bridge_2025.csv")
    )
    summary_json = Path(result.artifacts[-1])
    metadata_path = tmp_path / "_meta" / "distributions_metadata.json"

    assert summary_csv.exists()
    assert summary_pdf.exists()
    assert activity_csv.exists()
    assert bridge_csv.exists()
    assert miscoded_csv.exists()
    assert summary_json.exists()
    assert metadata_path.exists()

    with summary_csv.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    assert rows[0]["distribution_total"] == "1200.00"
    assert rows[0]["distribution_balance_sheet_change"] == "0.00"
    assert rows[0]["bridge_status"] == "Review"
    assert rows[0]["owner_keywords"] == "owner"

    summary_payload = json.loads(summary_json.read_text(encoding="utf-8"))
    assert summary_payload["distribution_total"] == "1200.00"
    assert summary_payload["distribution_balance_sheet_change"] == "0.00"
    assert summary_payload["owner_keywords"] == ["owner"]

    metadata_payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert metadata_payload["deliverable"] == "distributions"
    assert metadata_payload["schema_versions"] == {"csv": "2.0"}
    assert metadata_payload["artifacts"]
    assert "input_fingerprint" in metadata_payload


def test_distributions_deliverable_falls_back_to_default_owner_keywords(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_write_report(self, output_path, **kwargs):  # type: ignore[no-untyped-def]
        del self, kwargs
        path = Path(output_path)
        path.write_bytes(b"%PDF-1.4\n%stub\n")
        return path

    monkeypatch.setattr(
        "cpapacket.deliverables.distributions.PdfWriter.write_report",
        fake_write_report,
    )

    provider = _Provider(
        months={
            1: [
                {
                    "TxnId": "M-1",
                    "TxnDate": "2025-01-10",
                    "TxnType": "Expense",
                    "DocNum": "EXP-1",
                    "AccountName": "Meals Expense",
                    "AccountType": "Expense",
                    "Payee": "Owner Person",
                    "Memo": "owner draw",
                    "Amount": "1500.00",
                }
            ]
        }
    )
    deliverable = DistributionsDeliverable()
    result = deliverable.generate(_ctx(tmp_path), provider, prompts={})

    assert result.success is True
    assert any("default owner/shareholder keywords" in warning for warning in result.warnings)

    summary_json = (
        tmp_path / "06_Shareholder_Distributions" / "dev" / "distributions_summary_2025.json"
    )
    payload = json.loads(summary_json.read_text(encoding="utf-8"))
    assert payload["owner_keywords"] == ["owner", "shareholder"]
    assert Decimal(payload["distribution_total"]) == Decimal("0.00")


def test_distributions_deliverable_pipeline_writes_miscoded_csv_content(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_write_report(self, output_path, **kwargs):  # type: ignore[no-untyped-def]
        del self, kwargs
        path = Path(output_path)
        path.write_bytes(b"%PDF-1.4\n%stub\n")
        return path

    monkeypatch.setattr(
        "cpapacket.deliverables.distributions.PdfWriter.write_report",
        fake_write_report,
    )

    provider = _Provider(
        months={
            2: [
                {
                    "TxnId": "MC-1",
                    "TxnDate": "2025-02-14",
                    "TxnType": "Expense",
                    "DocNum": "EXP-99",
                    "AccountName": "Meals Expense",
                    "AccountType": "Expense",
                    "Payee": "Alex Owner",
                    "Memo": "personal reimbursement",
                    "Amount": "1200.00",
                }
            ]
        }
    )
    deliverable = DistributionsDeliverable()

    result = deliverable.generate(_ctx(tmp_path, owner_keywords=["alex"]), provider, prompts={})

    assert result.success is True
    miscoded_csv = next(Path(path) for path in result.artifacts if "likely_miscoded" in path)
    with miscoded_csv.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 1
    assert rows[0]["txn_id"] == "MC-1"
    assert rows[0]["payee"] == "Alex Owner"
    assert rows[0]["amount"] == "1200.00"
    assert rows[0]["confidence"] == "High"
    assert "R1_OWNER_PAYEE_EXPENSE" in rows[0]["reason_codes"]
    assert "R2_MEMO_KEYWORD_EXPENSE" in rows[0]["reason_codes"]
    assert "R4_ROUND_NUMBER_OWNER" in rows[0]["reason_codes"]
    assert "R5_HIGH_AMOUNT" in rows[0]["reason_codes"]


def test_distributions_deliverable_csv_outputs_match_golden_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_write_report(self, output_path, **kwargs):  # type: ignore[no-untyped-def]
        del self, kwargs
        path = Path(output_path)
        path.write_bytes(b"%PDF-1.4\n%stub\n")
        return path

    monkeypatch.setattr(
        "cpapacket.deliverables.distributions.PdfWriter.write_report",
        fake_write_report,
    )

    provider = _Provider(
        months={
            2: [
                {
                    "TxnId": "MC-1",
                    "TxnDate": "2025-02-14",
                    "TxnType": "Expense",
                    "DocNum": "EXP-99",
                    "AccountName": "Meals Expense",
                    "AccountType": "Expense",
                    "Payee": "Alex Owner",
                    "Memo": "personal reimbursement",
                    "Amount": "1200.00",
                }
            ]
        }
    )
    deliverable = DistributionsDeliverable()
    result = deliverable.generate(_ctx(tmp_path, owner_keywords=["alex"]), provider, prompts={})

    summary_csv = next(
        Path(path) for path in result.artifacts if path.endswith("distributions_summary_2025.csv")
    )
    miscoded_csv = next(
        Path(path)
        for path in result.artifacts
        if path.endswith("likely_miscoded_distributions_2025.csv")
    )
    fixtures_dir = Path("tests/fixtures/distributions")
    expected_summary = (fixtures_dir / "distributions_summary_2025_golden.csv").read_text(
        encoding="utf-8"
    )
    expected_miscoded = (fixtures_dir / "likely_miscoded_distributions_2025_golden.csv").read_text(
        encoding="utf-8"
    )

    assert summary_csv.read_text(encoding="utf-8") == expected_summary
    assert miscoded_csv.read_text(encoding="utf-8") == expected_miscoded


def test_distributions_deliverable_pipeline_via_respx_and_datastore(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_write_report(self, output_path, **kwargs):  # type: ignore[no-untyped-def]
        del self, kwargs
        path = Path(output_path)
        path.write_bytes(b"%PDF-1.4\n%stub\n")
        return path

    monkeypatch.setattr(
        "cpapacket.deliverables.distributions.PdfWriter.write_report",
        fake_write_report,
    )

    def general_ledger_response(request: httpx.Request) -> httpx.Response:
        month = int(request.url.params["start_date"].split("-")[1])
        if month == 2:
            payload: dict[str, Any] = {
                "Rows": {
                    "Row": [
                        {
                            "TxnId": "MC-1",
                            "TxnDate": "2025-02-14",
                            "TxnType": "Expense",
                            "DocNum": "EXP-99",
                            "AccountName": "Meals Expense",
                            "AccountType": "Expense",
                            "Payee": "Alex Owner",
                            "Memo": "personal reimbursement",
                            "Amount": "1200.00",
                        }
                    ]
                }
            }
        else:
            payload = {"Rows": {"Row": []}}
        return httpx.Response(200, json=payload)

    def company_info_response(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"CompanyInfo": {"LegalName": "Ira Ko LLC"}})

    def balance_sheet_response(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"Rows": {"Row": []}})

    store = SessionDataStore(cache_dir=tmp_path / "_meta" / "private" / "cache")
    with httpx.Client(base_url="https://qbo.api.test") as http_client:
        providers = DataProviders(
            store=store,
            qbo_client=_HttpQboClient(http_client),
            gusto_client=None,
        )
        deliverable = DistributionsDeliverable()

        with respx.mock(assert_all_called=True) as router:
            route = router.get("https://qbo.api.test/reports/GeneralLedger").mock(
                side_effect=general_ledger_response
            )
            company_route = router.get("https://qbo.api.test/companyinfo/test-realm").mock(
                side_effect=company_info_response
            )
            balance_sheet_route = router.get("https://qbo.api.test/reports/BalanceSheet").mock(
                side_effect=balance_sheet_response
            )
            result = deliverable.generate(_ctx(tmp_path, owner_keywords=["alex"]), providers, {})

    assert result.success
    assert route.call_count == 12
    assert company_route.call_count == 1
    assert balance_sheet_route.call_count == 2

    summary_csv = next(
        Path(path) for path in result.artifacts if path.endswith("distributions_summary_2025.csv")
    )
    miscoded_csv = next(
        Path(path)
        for path in result.artifacts
        if path.endswith("likely_miscoded_distributions_2025.csv")
    )
    fixtures_dir = Path("tests/fixtures/distributions")
    expected_summary = (fixtures_dir / "distributions_summary_2025_golden.csv").read_text(
        encoding="utf-8"
    )
    expected_miscoded = (fixtures_dir / "likely_miscoded_distributions_2025_golden.csv").read_text(
        encoding="utf-8"
    )

    assert summary_csv.read_text(encoding="utf-8") == expected_summary
    assert miscoded_csv.read_text(encoding="utf-8") == expected_miscoded
