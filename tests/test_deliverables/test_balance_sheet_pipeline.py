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
from cpapacket.deliverables.balance_sheet import BalanceSheetDeliverable


class _HttpQboClient:
    def __init__(self, client: httpx.Client) -> None:
        self._client = client

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
            route = router.get("https://api.example.test/reports/BalanceSheet").mock(
                return_value=httpx.Response(200, json=fixture)
            )
            router.get("https://api.example.test/companyinfo").mock(
                return_value=httpx.Response(200, json={"CompanyInfo": {"CompanyName": "Acme LLC"}})
            )
            result = deliverable.generate(_run_context(tmp_path), providers, prompts={})

    csv_path = tmp_path / "02_Year-End_Balance_Sheet" / "Balance_Sheet_2025-12-31.csv"
    pdf_path = tmp_path / "02_Year-End_Balance_Sheet" / "Balance_Sheet_2025-12-31.pdf"
    raw_path = tmp_path / "02_Year-End_Balance_Sheet" / "Balance_Sheet_2025-12-31_raw.json"
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
            router.get("https://api.example.test/companyinfo").mock(
                return_value=httpx.Response(200, json={"CompanyInfo": {"CompanyName": "Acme LLC"}})
            )
            result = deliverable.generate(_run_context(tmp_path), providers, prompts={})

    meta_path = tmp_path / "_meta" / "balance_sheet_metadata.json"
    metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    warnings = metadata["warnings"]
    assert warnings
    assert "Balance equation mismatch" in warnings[0]
    assert result.warnings
