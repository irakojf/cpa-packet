from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from cpapacket.core.context import RunContext
from cpapacket.data.providers import DataProviders
from cpapacket.data.store import SessionDataStore
from cpapacket.deliverables.balance_sheet import (
    BalanceSheetDeliverable,
    PriorBalanceSheetDeliverable,
)


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


def _run_context(tmp_path: Path) -> RunContext:
    return RunContext(
        year=2025,
        year_source="explicit",
        out_dir=tmp_path,
        method="accrual",
        non_interactive=True,
        on_conflict="abort",
        incremental=False,
        force=False,
        no_cache=False,
        no_raw=False,
        redact=False,
        include_debug=False,
        verbose=False,
        quiet=False,
        plain=False,
        skip=[],
        owner_keywords=[],
        gusto_available=False,
    )


def test_balance_sheet_pipeline_generates_outputs_and_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = json.loads(Path("tests/fixtures/qbo/balance_sheet_2025.json").read_text("utf-8"))
    cache_dir = tmp_path / "_meta" / "private" / "cache"
    store = SessionDataStore(cache_dir=cache_dir)
    captured_body_lines: list[Any] = []

    def fake_write_report(
        self: object,
        output_path: str | Path,
        *,
        company_name: str,
        report_title: str,
        date_range_label: str,
        body_lines: list[Any],
    ) -> Path:
        captured_body_lines.extend(body_lines)
        destination = Path(output_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b"%PDF-1.4\n")
        return destination

    monkeypatch.setattr(
        "cpapacket.deliverables.balance_sheet.PdfWriter.write_report",
        fake_write_report,
    )

    with httpx.Client(base_url="https://api.example.test") as http_client:
        providers = DataProviders(
            store=store,
            qbo_client=_HttpQboClient(http_client),
            gusto_client=None,
        )
        deliverable = BalanceSheetDeliverable()

        with respx.mock(assert_all_called=True) as router:
            route = router.get("https://api.example.test/reports/BalanceSheet").mock(
                return_value=httpx.Response(200, json=fixture)
            )
            router.get("https://api.example.test/companyinfo/test-realm").mock(
                return_value=httpx.Response(200, json={"CompanyInfo": {"CompanyName": "Acme LLC"}})
            )
            result = deliverable.generate(_run_context(tmp_path), providers, prompts={})

    csv_path = (
        tmp_path / "02_Year-End_Balance_Sheet" / "cpa" / "Balance_Sheet_2025-12-31.csv"
    )
    pdf_path = (
        tmp_path / "02_Year-End_Balance_Sheet" / "cpa" / "Balance_Sheet_2025-12-31.pdf"
    )
    raw_path = (
        tmp_path / "02_Year-End_Balance_Sheet" / "dev" / "Balance_Sheet_2025-12-31_raw.json"
    )
    meta_path = tmp_path / "_meta" / "balance_sheet_metadata.json"

    assert route.call_count == 1
    assert csv_path.exists()
    assert pdf_path.exists()
    assert raw_path.exists()
    assert meta_path.exists()
    assert result.success
    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        assert reader.fieldnames == ["section", "level", "row_type", "label", "amount", "path"]
        first_row = next(reader, None)
    assert first_row is not None
    assert first_row["section"] == "Assets"
    assert captured_body_lines
    line_text = [line.text for line in captured_body_lines]
    assert "Balance Equation Summary" in line_text
    assert any("Assets" in text for text in line_text)
    assert any("Liabilities + Equity" in text for text in line_text)
    assert any("Difference" in text for text in line_text)

    metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    assert metadata["deliverable"] == "balance_sheet"
    assert metadata["warnings"] == []
    assert str(csv_path) in metadata["artifacts"]


def test_balance_sheet_pipeline_writes_warning_for_equation_mismatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = json.loads(Path("tests/fixtures/qbo/balance_sheet_2025.json").read_text("utf-8"))
    fixture["Rows"]["Row"][2]["Summary"]["ColData"][1]["value"] = "90010.00"

    cache_dir = tmp_path / "_meta" / "private" / "cache"
    store = SessionDataStore(cache_dir=cache_dir)

    def fake_write_report(
        self: object,
        output_path: str | Path,
        *,
        company_name: str,
        report_title: str,
        date_range_label: str,
        body_lines: list[Any],
    ) -> Path:
        destination = Path(output_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b"%PDF-1.4\n")
        return destination

    monkeypatch.setattr(
        "cpapacket.deliverables.balance_sheet.PdfWriter.write_report",
        fake_write_report,
    )

    with httpx.Client(base_url="https://api.example.test") as http_client:
        providers = DataProviders(
            store=store,
            qbo_client=_HttpQboClient(http_client),
            gusto_client=None,
        )
        deliverable = BalanceSheetDeliverable()

        with respx.mock(assert_all_called=True) as router:
            router.get("https://api.example.test/reports/BalanceSheet").mock(
                return_value=httpx.Response(200, json=fixture)
            )
            router.get("https://api.example.test/companyinfo/test-realm").mock(
                return_value=httpx.Response(200, json={"CompanyInfo": {"CompanyName": "Acme LLC"}})
            )
            result = deliverable.generate(_run_context(tmp_path), providers, prompts={})

    meta_path = tmp_path / "_meta" / "balance_sheet_metadata.json"
    metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    warnings = metadata["warnings"]
    assert warnings
    assert "Balance equation mismatch" in warnings[0]
    assert result.warnings


def test_prior_balance_sheet_uses_previous_year_as_of_date(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = json.loads(Path("tests/fixtures/qbo/balance_sheet_2025.json").read_text("utf-8"))
    cache_dir = tmp_path / "_meta" / "private" / "cache"
    store = SessionDataStore(cache_dir=cache_dir)

    def fake_write_report(
        self: object,
        output_path: str | Path,
        *,
        company_name: str,
        report_title: str,
        date_range_label: str,
        body_lines: list[Any],
    ) -> Path:
        destination = Path(output_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b"%PDF-1.4\n")
        return destination

    monkeypatch.setattr(
        "cpapacket.deliverables.balance_sheet.PdfWriter.write_report",
        fake_write_report,
    )

    with httpx.Client(base_url="https://api.example.test") as http_client:
        providers = DataProviders(
            store=store,
            qbo_client=_HttpQboClient(http_client),
            gusto_client=None,
        )
        deliverable = PriorBalanceSheetDeliverable()

        with respx.mock(assert_all_called=True) as router:
            route = router.get("https://api.example.test/reports/BalanceSheet").mock(
                return_value=httpx.Response(200, json=fixture)
            )
            router.get("https://api.example.test/companyinfo/test-realm").mock(
                return_value=httpx.Response(200, json={"CompanyInfo": {"CompanyName": "Acme LLC"}})
            )
            result = deliverable.generate(_run_context(tmp_path), providers, prompts={})

    assert result.success
    assert route.call_count == 1
    assert route.calls[0].request.url.params["start_date"] == "2024-01-01"
    assert route.calls[0].request.url.params["end_date"] == "2024-12-31"
    prior_csv = (
        tmp_path / "02_Year-End_Balance_Sheet" / "cpa" / "Balance_Sheet_2024-12-31.csv"
    )
    assert prior_csv.exists()
    prior_meta = tmp_path / "_meta" / "prior_balance_sheet_metadata.json"
    metadata = json.loads(prior_meta.read_text(encoding="utf-8"))
    assert metadata["deliverable"] == "prior_balance_sheet"


def test_prior_balance_sheet_writes_placeholder_when_report_is_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    empty_fixture: dict[str, Any] = {"Rows": {"Row": []}}
    cache_dir = tmp_path / "_meta" / "private" / "cache"
    store = SessionDataStore(cache_dir=cache_dir)

    def fake_write_report(
        self: object,
        output_path: str | Path,
        *,
        company_name: str,
        report_title: str,
        date_range_label: str,
        body_lines: list[Any],
    ) -> Path:
        destination = Path(output_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b"%PDF-1.4\n")
        return destination

    monkeypatch.setattr(
        "cpapacket.deliverables.balance_sheet.PdfWriter.write_report",
        fake_write_report,
    )

    with httpx.Client(base_url="https://api.example.test") as http_client:
        providers = DataProviders(
            store=store,
            qbo_client=_HttpQboClient(http_client),
            gusto_client=None,
        )
        deliverable = PriorBalanceSheetDeliverable()

        with respx.mock(assert_all_called=True) as router:
            router.get("https://api.example.test/reports/BalanceSheet").mock(
                return_value=httpx.Response(200, json=empty_fixture)
            )
            router.get("https://api.example.test/companyinfo/test-realm").mock(
                return_value=httpx.Response(200, json={"CompanyInfo": {"CompanyName": "Acme LLC"}})
            )
            result = deliverable.generate(_run_context(tmp_path), providers, prompts={})

    assert result.success
    prior_csv = (
        tmp_path / "02_Year-End_Balance_Sheet" / "cpa" / "Balance_Sheet_2024-12-31.csv"
    )
    with prior_csv.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        row = next(reader, None)
    assert row is not None
    assert row["label"] == "No prior-year data available"
    prior_meta = tmp_path / "_meta" / "prior_balance_sheet_metadata.json"
    metadata = json.loads(prior_meta.read_text(encoding="utf-8"))
    assert metadata["warnings"] == ["Balance sheet report normalized to zero rows."]


def test_balance_sheet_csv_matches_golden_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = json.loads(Path("tests/fixtures/qbo/balance_sheet_2025.json").read_text("utf-8"))
    cache_dir = tmp_path / "_meta" / "private" / "cache"
    store = SessionDataStore(cache_dir=cache_dir)

    def fake_write_report(
        self: object,
        output_path: str | Path,
        *,
        company_name: str,
        report_title: str,
        date_range_label: str,
        body_lines: list[Any],
    ) -> Path:
        destination = Path(output_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b"%PDF-1.4\n")
        return destination

    monkeypatch.setattr(
        "cpapacket.deliverables.balance_sheet.PdfWriter.write_report",
        fake_write_report,
    )

    with httpx.Client(base_url="https://api.example.test") as http_client:
        providers = DataProviders(
            store=store,
            qbo_client=_HttpQboClient(http_client),
            gusto_client=None,
        )
        deliverable = BalanceSheetDeliverable()

        with respx.mock(assert_all_called=True) as router:
            router.get("https://api.example.test/reports/BalanceSheet").mock(
                return_value=httpx.Response(200, json=fixture)
            )
            router.get("https://api.example.test/companyinfo/test-realm").mock(
                return_value=httpx.Response(200, json={"CompanyInfo": {"CompanyName": "Acme LLC"}})
            )
            result = deliverable.generate(_run_context(tmp_path), providers, prompts={})

    assert result.success
    csv_path = (
        tmp_path / "02_Year-End_Balance_Sheet" / "cpa" / "Balance_Sheet_2025-12-31.csv"
    )
    expected = (
        "section,level,row_type,label,amount,path\n"
        "Assets,0,header,Assets,0.00,Assets\n"
        "Assets,1,account,Checking,65000.00,Assets > Checking\n"
        "Assets,1,account,Accounts Receivable,22000.00,Assets > Accounts Receivable\n"
        "Assets,1,account,Prepaid Expenses,13000.00,Assets > Prepaid Expenses\n"
        "Assets,1,account,Equipment,50000.00,Assets > Equipment\n"
        "Assets,0,total,Total Assets,150000.00,Total Assets\n"
        "Liabilities,0,header,Liabilities,0.00,Liabilities\n"
        "Liabilities,1,account,Accounts Payable,19000.00,Liabilities > Accounts Payable\n"
        "Liabilities,1,account,Credit Card,8000.00,Liabilities > Credit Card\n"
        "Liabilities,1,account,Payroll Liabilities,11000.00,Liabilities > Payroll Liabilities\n"
        "Liabilities,1,account,Term Loan,22000.00,Liabilities > Term Loan\n"
        "Liabilities,0,total,Total Liabilities,60000.00,Total Liabilities\n"
        "Equity,0,header,Equity,0.00,Equity\n"
        "Equity,1,account,Owner Contributions,70000.00,Equity > Owner Contributions\n"
        "Equity,1,account,Retained Earnings,20000.00,Equity > Retained Earnings\n"
        "Equity,0,total,Total Equity,90000.00,Total Equity\n"
    )
    assert csv_path.read_text(encoding="utf-8") == expected


def test_balance_sheet_csv_snapshot_handles_unbalanced_fixture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = json.loads(Path("tests/fixtures/qbo/balance_sheet_2025.json").read_text("utf-8"))
    fixture["Rows"]["Row"][2]["Summary"]["ColData"][1]["value"] = "90010.00"

    cache_dir = tmp_path / "_meta" / "private" / "cache"
    store = SessionDataStore(cache_dir=cache_dir)

    def fake_write_report(
        self: object,
        output_path: str | Path,
        *,
        company_name: str,
        report_title: str,
        date_range_label: str,
        body_lines: list[Any],
    ) -> Path:
        destination = Path(output_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b"%PDF-1.4\n")
        return destination

    monkeypatch.setattr(
        "cpapacket.deliverables.balance_sheet.PdfWriter.write_report",
        fake_write_report,
    )

    with httpx.Client(base_url="https://api.example.test") as http_client:
        providers = DataProviders(
            store=store,
            qbo_client=_HttpQboClient(http_client),
            gusto_client=None,
        )
        deliverable = BalanceSheetDeliverable()

        with respx.mock(assert_all_called=True) as router:
            router.get("https://api.example.test/reports/BalanceSheet").mock(
                return_value=httpx.Response(200, json=fixture)
            )
            router.get("https://api.example.test/companyinfo/test-realm").mock(
                return_value=httpx.Response(200, json={"CompanyInfo": {"CompanyName": "Acme LLC"}})
            )
            result = deliverable.generate(_run_context(tmp_path), providers, prompts={})

    assert result.success
    csv_path = (
        tmp_path / "02_Year-End_Balance_Sheet" / "cpa" / "Balance_Sheet_2025-12-31.csv"
    )
    csv_text = csv_path.read_text(encoding="utf-8")
    assert "Total Equity,90010.00" in csv_text
