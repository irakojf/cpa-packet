from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

from cpapacket.deliverables.payroll_summary import (
    normalize_employee_breakdowns,
    normalize_gusto_payload,
    normalize_payroll_runs,
)


def _load_fixture() -> dict[str, object]:
    payload = json.loads(Path("tests/fixtures/gusto/payroll_2025.json").read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def test_normalize_gusto_payload_produces_expected_run_and_employee_counts() -> None:
    payload = _load_fixture()

    runs, employee_rows = normalize_gusto_payload(payload)

    assert len(runs) == 2
    assert len(employee_rows) == 7
    assert runs[0].run_id == "payroll-2025-01"
    assert runs[1].run_id == "payroll-2025-02"


def test_normalize_payroll_runs_extracts_required_company_totals() -> None:
    payload = _load_fixture()
    payrolls = payload["payrolls"]
    assert isinstance(payrolls, list)

    runs = normalize_payroll_runs(payrolls)

    assert runs[0].wages == Decimal("18500.00")
    assert runs[0].employee_taxes == Decimal("2915.00")
    assert runs[0].employer_taxes == Decimal("1820.00")
    assert runs[0].employee_retirement_deferral == Decimal("850.00")
    assert runs[0].employer_retirement_contribution == Decimal("510.00")

    assert runs[1].wages == Decimal("19200.00")
    assert runs[1].employee_retirement_deferral == Decimal("935.00")
    assert runs[1].employer_retirement_contribution == Decimal("561.00")


def test_normalize_employee_breakdowns_allocates_taxes_when_missing_per_employee_fields() -> None:
    raw_runs = [
        {
            "uuid": "run-1",
            "pay_period_start_date": "2025-01-01",
            "pay_period_end_date": "2025-01-15",
            "check_date": "2025-01-20",
            "totals": {
                "employee_taxes": "40.00",
                "employer_taxes": "20.00",
            },
            "employee_compensations": [
                {
                    "employee_uuid": "emp-a",
                    "employee_name": "A",
                    "regular_pay": "100.00",
                    "bonus_pay": "0.00",
                    "overtime_pay": "0.00",
                    "employee_401k": "5.00",
                    "employer_401k": "3.00",
                },
                {
                    "employee_uuid": "emp-b",
                    "employee_name": "B",
                    "regular_pay": "300.00",
                    "bonus_pay": "0.00",
                    "overtime_pay": "0.00",
                    "employee_401k": "15.00",
                    "employer_401k": "9.00",
                },
            ],
        }
    ]

    rows = normalize_employee_breakdowns(raw_runs)

    assert len(rows) == 2
    by_id = {row.employee_id: row for row in rows}
    assert by_id["emp-a"].employee_taxes == Decimal("10.00")
    assert by_id["emp-b"].employee_taxes == Decimal("30.00")
    assert by_id["emp-a"].employer_taxes == Decimal("5.00")
    assert by_id["emp-b"].employer_taxes == Decimal("15.00")


def test_normalize_employee_breakdowns_uses_explicit_employee_taxes_when_present() -> None:
    raw_runs = [
        {
            "uuid": "run-2",
            "employee_compensations": [
                {
                    "employee_uuid": "emp-1",
                    "employee_name": "Alex",
                    "regular_pay": "200.00",
                    "bonus_pay": "0.00",
                    "overtime_pay": "0.00",
                    "employee_taxes": "18.50",
                    "employer_taxes": "9.25",
                    "employee_401k": "10.00",
                    "employer_401k": "6.00",
                }
            ],
            "totals": {"employee_taxes": "99.00", "employer_taxes": "99.00"},
        }
    ]

    rows = normalize_employee_breakdowns(raw_runs)

    assert len(rows) == 1
    assert rows[0].employee_taxes == Decimal("18.50")
    assert rows[0].employer_taxes == Decimal("9.25")


def test_normalize_gusto_payload_handles_missing_payrolls_key() -> None:
    runs, rows = normalize_gusto_payload({})
    assert runs == []
    assert rows == []
