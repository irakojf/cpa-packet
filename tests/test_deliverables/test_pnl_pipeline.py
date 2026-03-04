from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from cpapacket.core.context import RunContext
from cpapacket.data.providers import DataProviders
from cpapacket.data.store import SessionDataStore
from cpapacket.deliverables.pnl import PnlDeliverable


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


def _run_deliverable_with_fixture(
    tmp_path: Path, fixture_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[respx.MockRoute, Path, Path, Path, Path, Any]:
    fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
    cache_dir = tmp_path / "_meta" / "private" / "cache"
    store = SessionDataStore(cache_dir=cache_dir)

    fake_pdf_target: dict[str, Path] = {}

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
        fake_pdf_target["path"] = destination
        return destination

    monkeypatch.setattr("cpapacket.deliverables.pnl.PdfWriter.write_report", fake_write_report)

    with httpx.Client(base_url="https://api.example.test") as http_client:
        qbo_client = _HttpQboClient(http_client)
        providers = DataProviders(store=store, qbo_client=qbo_client, gusto_client=None)
        deliverable = PnlDeliverable()

        with respx.mock(assert_all_called=True) as router:
            route = router.get("https://api.example.test/reports/ProfitAndLoss").mock(
                return_value=httpx.Response(200, json=fixture)
            )
            router.get("https://api.example.test/companyinfo").mock(
                return_value=httpx.Response(200, json={"CompanyInfo": {"CompanyName": "Acme LLC"}})
            )

            result = deliverable.generate(_run_context(tmp_path), providers, prompts={})

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

    return route, csv_path, pdf_path, raw_path, meta_path, result


def test_pnl_pipeline_generates_csv_pdf_and_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (
        route,
        csv_path,
        pdf_path,
        raw_path,
        meta_path,
        result,
    ) = _run_deliverable_with_fixture(
        tmp_path, Path("tests/fixtures/qbo/profit_and_loss_annual.json"), monkeypatch
    )

    assert route.call_count == 1
    assert csv_path.exists()
    assert pdf_path.exists()
    assert raw_path.exists()
    assert meta_path.exists()

    metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    assert metadata["deliverable"] == "pnl"
    assert str(csv_path) in metadata["artifacts"]
    assert str(pdf_path) in metadata["artifacts"]
    assert raw_path.exists()
    assert result.success


def test_pnl_csv_snapshot_matches_golden(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    route, csv_path, _, _, _, result = _run_deliverable_with_fixture(
        tmp_path, Path("tests/fixtures/qbo/profit_and_loss_annual.json"), monkeypatch
    )

    lines = csv_path.read_text(encoding="utf-8").splitlines()
    assert lines[0] == "section,level,row_type,label,amount,path"
    assert lines[1].startswith("Income,0,header,Income,30000.00,Income")
    assert any("Product Revenue" in line for line in lines)
    assert "2025-01-01_to_2025-12-31" in csv_path.name
    assert route.call_count == 1
    assert result.success


def test_pnl_csv_snapshot_handles_empty_fixture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    route, csv_path, _, _, _, result = _run_deliverable_with_fixture(
        tmp_path, Path("tests/fixtures/qbo/pnl_empty_2025.json"), monkeypatch
    )

    lines = csv_path.read_text(encoding="utf-8").splitlines()
    assert lines[0] == "section,level,row_type,label,amount,path"
    assert lines[1:] == [
        "Uncategorized,0,total,No transactions found,0.00,No transactions found"
    ]
    assert route.call_count == 1
    assert result.success
