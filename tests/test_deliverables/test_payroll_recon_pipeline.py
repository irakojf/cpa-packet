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
from cpapacket.deliverables.payroll_recon import PayrollReconDeliverable

_GOLDEN_CSV_PATH = Path("tests/fixtures/gusto/payroll_reconciliation_2025_golden.csv")


class _HttpGustoClient:
    def __init__(self, client: httpx.Client) -> None:
        self._client = client

    def request(
        self,
        method: str,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        required: bool = True,
    ) -> httpx.Response | None:
        response = self._client.request(method, endpoint, params=params, json=json_body)
        if not required and response.status_code == 404:
            return None
        return response


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
        non_interactive=True,
        on_conflict="abort",
        no_raw=False,
        gusto_available=True,
    )


def _payroll_runs_fixture() -> list[dict[str, Any]]:
    return [
        {
            "uuid": "run-1",
            "pay_period_start_date": "2025-01-01",
            "pay_period_end_date": "2025-01-15",
            "check_date": "2025-01-20",
            "totals": {"gross_pay": "1000.00", "employer_taxes": "100.00"},
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


def _qbo_accounts_fixture(*, payroll_balance: str) -> dict[str, Any]:
    return {
        "QueryResponse": {
            "Account": [
                {
                    "Name": "Payroll Expense",
                    "AccountType": "Expense",
                    "CurrentBalance": payroll_balance,
                }
            ]
        }
    }


def _run_deliverable(
    tmp_path: Path,
    *,
    payroll_balance: str,
) -> tuple[respx.MockRoute, respx.MockRoute, Any]:
    store = SessionDataStore(cache_dir=tmp_path / "_meta" / "private" / "cache")

    with (
        httpx.Client(base_url="https://qbo.api.test") as qbo_http_client,
        httpx.Client(base_url="https://gusto.api.test") as gusto_http_client,
    ):
        providers = DataProviders(
            store=store,
            qbo_client=_HttpQboClient(qbo_http_client),
            gusto_client=_HttpGustoClient(gusto_http_client),
        )
        deliverable = PayrollReconDeliverable()

        with respx.mock(assert_all_called=True) as router:
            payroll_route = router.get("https://gusto.api.test/payrolls").mock(
                return_value=httpx.Response(200, json=_payroll_runs_fixture())
            )
            accounts_route = router.get("https://qbo.api.test/query").mock(
                return_value=httpx.Response(
                    200,
                    json=_qbo_accounts_fixture(payroll_balance=payroll_balance),
                )
            )
            result = deliverable.generate(_run_context(tmp_path), providers, prompts={})

    return payroll_route, accounts_route, result


def test_payroll_recon_pipeline_reconciled_writes_outputs(
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

    payroll_route, accounts_route, result = _run_deliverable(tmp_path, payroll_balance="1150.00")

    assert payroll_route.call_count == 1
    assert accounts_route.call_count == 1
    assert result.success
    assert result.warnings == []
    assert len(result.artifacts) == 3

    csv_path = tmp_path / "10_Payroll_Reconciliation" / "cpa" / "payroll_reconciliation_2025.csv"
    json_path = tmp_path / "10_Payroll_Reconciliation" / "dev" / "payroll_reconciliation_2025.json"
    metadata_path = tmp_path / "_meta" / "payroll_recon_metadata.json"
    assert csv_path.exists()
    assert "2025" in csv_path.name
    assert json_path.exists()
    assert metadata_path.exists()

    assert csv_path.read_text(encoding="utf-8") == _GOLDEN_CSV_PATH.read_text(encoding="utf-8")

    with csv_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    assert rows[0]["status"] == "RECONCILED"
    assert rows[0]["variance"] == "0.00"

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["status"] == "RECONCILED"


def test_payroll_recon_pipeline_mismatch_emits_warning(
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

    _, _, result = _run_deliverable(tmp_path, payroll_balance="1200.00")

    assert result.success
    assert any("mismatch detected" in warning.lower() for warning in result.warnings)

    csv_path = tmp_path / "10_Payroll_Reconciliation" / "cpa" / "payroll_reconciliation_2025.csv"
    assert "2025" in csv_path.name
    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        assert reader.fieldnames == [
            "year",
            "gusto_total",
            "qbo_total",
            "variance",
            "status",
            "tolerance",
        ]
        rows = list(reader)
    assert rows[0]["status"] == "MISMATCH"
    assert rows[0]["variance"] == "50.00"
