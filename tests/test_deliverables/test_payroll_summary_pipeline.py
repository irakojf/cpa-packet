from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import respx

from cpapacket.core.context import RunContext
from cpapacket.core.filesystem import sanitize_filesystem_name
from cpapacket.data.providers import DataProviders
from cpapacket.data.store import SessionDataStore
from cpapacket.deliverables.payroll_summary import PayrollSummaryDeliverable

_PAYROLL_FIXTURE_PATH = Path("tests/fixtures/gusto/payroll_2025.json")
_GOLDEN_COMPANY_CSV_PATH = Path("tests/fixtures/gusto/payroll_summary_company_2025_golden.csv")
_GOLDEN_EMPLOYEE_CSV_PATH = Path("tests/fixtures/gusto/payroll_summary_employee_2025_golden.csv")


class _UnusedQboClient:
    def request(
        self,
        method: str,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> object:
        del method, endpoint, params, json_body
        raise AssertionError("QBO should not be called in payroll pipeline tests")


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


def _run_context(tmp_path: Path) -> RunContext:
    return RunContext(
        year=2025,
        year_source="explicit",
        out_dir=tmp_path,
        non_interactive=True,
        on_conflict="abort",
        no_raw=False,
        redact=False,
        gusto_available=True,
    )


def _load_payroll_runs() -> list[dict[str, Any]]:
    payload = json.loads(_PAYROLL_FIXTURE_PATH.read_text(encoding="utf-8"))
    runs = payload.get("payrolls")
    if not isinstance(runs, list):
        raise AssertionError("Expected payroll fixture to contain a payrolls list")
    return [item for item in runs if isinstance(item, dict)]


def _run_deliverable_with_runs(
    tmp_path: Path, runs: list[dict[str, Any]]
) -> tuple[respx.MockRoute, Path, Path, Path, Path, Any]:
    store = SessionDataStore(cache_dir=tmp_path / "_meta" / "private" / "cache")

    with httpx.Client(base_url="https://api.example.test") as http_client:
        providers = DataProviders(
            store=store,
            qbo_client=_UnusedQboClient(),
            gusto_client=_HttpGustoClient(http_client),
        )
        deliverable = PayrollSummaryDeliverable()

        with respx.mock(assert_all_called=True) as router:
            route = router.get("https://api.example.test/payrolls").mock(
                return_value=httpx.Response(200, json=runs)
            )
            result = deliverable.generate(_run_context(tmp_path), providers, prompts={})

    company_dir = tmp_path / "04_Annual_Payroll_Summary" / "cpa" / "00_Company_Summary"
    employees_dir = tmp_path / "04_Annual_Payroll_Summary" / "cpa" / "Employees"
    company_csv = company_dir / "Annual_Payroll_Summary_2025.csv"
    company_pdf = company_dir / "Annual_Payroll_Summary_2025.pdf"
    employee_csv = (
        employees_dir / "Rivera_Alex_emp_emp-001" / "Payroll_Breakdown_Alex_Rivera_2025.csv"
    )
    meta_path = (
        tmp_path / "_meta" / "private" / "deliverables" / "payroll_summary_2025_metadata.json"
    )
    return route, company_csv, company_pdf, employee_csv, meta_path, result


def _run_deliverable(tmp_path: Path) -> tuple[respx.MockRoute, Path, Path, Path, Path, Any]:
    return _run_deliverable_with_runs(tmp_path, _load_payroll_runs())


def test_payroll_pipeline_writes_csv_pdf_and_metadata(tmp_path: Path) -> None:
    route, company_csv, company_pdf, employee_csv, meta_path, result = _run_deliverable(tmp_path)

    assert route.call_count == 1
    assert result.success
    assert company_csv.exists()
    assert company_pdf.exists()
    assert employee_csv.exists()
    assert meta_path.exists()

    metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    assert metadata["deliverable"] == "payroll_summary_2025"
    assert metadata["inputs"]["source_run_count"] == 2
    assert str(company_csv) in metadata["artifacts"]
    assert str(company_pdf) in metadata["artifacts"]
    assert str(employee_csv) in metadata["artifacts"]


def test_payroll_pipeline_csv_matches_golden_snapshots(tmp_path: Path) -> None:
    _, company_csv, _, employee_csv, _, result = _run_deliverable(tmp_path)

    expected_company = _GOLDEN_COMPANY_CSV_PATH.read_text(encoding="utf-8")
    expected_employee = _GOLDEN_EMPLOYEE_CSV_PATH.read_text(encoding="utf-8")

    assert result.success
    assert company_csv.read_text(encoding="utf-8") == expected_company
    assert employee_csv.read_text(encoding="utf-8") == expected_employee


def test_payroll_pipeline_no_runs_emits_warning_and_zero_company_summary(
    tmp_path: Path,
) -> None:
    route, company_csv, _, _, _, result = _run_deliverable_with_runs(tmp_path, [])

    assert route.call_count == 1
    assert result.success
    assert result.warnings == ["No payroll runs found; generated zero-value payroll summary."]
    assert company_csv.exists()
    assert company_csv.read_text(encoding="utf-8").splitlines()[1] == (
        "2025,0,0.00,0.00,0.00,0.00,0.00,0.00"
    )
    employees_dir = tmp_path / "04_Annual_Payroll_Summary" / "cpa" / "Employees"
    assert list(employees_dir.iterdir()) == []


def test_payroll_pipeline_sanitizes_folder_name_but_preserves_employee_display_name(
    tmp_path: Path,
) -> None:
    employee_name = "Zoë / Ops:Lead"
    employee_id = "emp-special"
    runs: list[dict[str, Any]] = [
        {
            "uuid": "payroll-special-1",
            "pay_period_start_date": "2025-01-01",
            "pay_period_end_date": "2025-01-15",
            "check_date": "2025-01-20",
            "totals": {"employee_taxes": "120.00", "employer_taxes": "80.00"},
            "employee_compensations": [
                {
                    "employee_uuid": employee_id,
                    "employee_name": employee_name,
                    "regular_pay": "1000.00",
                    "bonus_pay": "0.00",
                    "overtime_pay": "0.00",
                    "employee_401k": "50.00",
                    "employer_401k": "30.00",
                }
            ],
        }
    ]

    _, _, _, _, _, result = _run_deliverable_with_runs(tmp_path, runs)

    expected_folder = sanitize_filesystem_name(f"Ops:Lead_Zoë_emp_{employee_id}")
    expected_csv_name = (
        f"Payroll_Breakdown_{sanitize_filesystem_name(employee_name)}_2025.csv"
    )
    employee_csv = (
        tmp_path / "04_Annual_Payroll_Summary" / "cpa" / "Employees" / expected_folder / expected_csv_name
    )
    assert result.success
    assert employee_csv.exists()
    assert employee_name in employee_csv.read_text(encoding="utf-8")


def test_payroll_pipeline_includes_mid_year_hire_and_termination_rows(tmp_path: Path) -> None:
    runs: list[dict[str, Any]] = [
        {
            "uuid": "payroll-2025-01",
            "pay_period_start_date": "2025-01-01",
            "pay_period_end_date": "2025-01-15",
            "check_date": "2025-01-20",
            "totals": {"employee_taxes": "100.00", "employer_taxes": "60.00"},
            "employee_compensations": [
                {
                    "employee_uuid": "emp-early",
                    "employee_name": "Early Hire",
                    "regular_pay": "1000.00",
                    "bonus_pay": "0.00",
                    "overtime_pay": "0.00",
                    "employee_401k": "25.00",
                    "employer_401k": "15.00",
                }
            ],
        },
        {
            "uuid": "payroll-2025-11",
            "pay_period_start_date": "2025-11-01",
            "pay_period_end_date": "2025-11-15",
            "check_date": "2025-11-20",
            "totals": {"employee_taxes": "90.00", "employer_taxes": "54.00"},
            "employee_compensations": [
                {
                    "employee_uuid": "emp-late",
                    "employee_name": "Late Joiner",
                    "regular_pay": "900.00",
                    "bonus_pay": "0.00",
                    "overtime_pay": "0.00",
                    "employee_401k": "20.00",
                    "employer_401k": "12.00",
                }
            ],
        },
    ]

    _, company_csv, _, _, meta_path, result = _run_deliverable_with_runs(tmp_path, runs)

    early_folder = sanitize_filesystem_name("Hire_Early_emp_emp-early")
    late_folder = sanitize_filesystem_name("Joiner_Late_emp_emp-late")
    employees_dir = tmp_path / "04_Annual_Payroll_Summary" / "cpa" / "Employees"

    assert result.success
    assert (employees_dir / early_folder).exists()
    assert (employees_dir / late_folder).exists()
    assert ",2,1900.00," in company_csv.read_text(encoding="utf-8")

    metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    assert metadata["inputs"]["source_run_count"] == 2
    assert metadata["inputs"]["employee_count"] == 2
