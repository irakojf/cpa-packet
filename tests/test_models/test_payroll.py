from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from pydantic import ValidationError

from cpapacket.models.payroll import (
    CompanyPayrollSummary,
    EmployeePayrollBreakdown,
    PayrollRun,
)


def test_payroll_run_happy_path_quantizes_and_normalizes() -> None:
    run = PayrollRun(
        run_id=" run-001 ",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 15),
        pay_date=date(2025, 1, 20),
        wages="1000.125",
        employee_taxes="100.123",
        employer_taxes="120.126",
        employee_retirement_deferral="50.125",
        employer_retirement_contribution="25.124",
    )

    assert run.run_id == "run-001"
    assert run.wages == Decimal("1000.13")
    assert run.employee_taxes == Decimal("100.12")
    assert run.employer_taxes == Decimal("120.13")
    assert run.employee_retirement_deferral == Decimal("50.13")
    assert run.employer_retirement_contribution == Decimal("25.12")


def test_employee_payroll_breakdown_rejects_blank_or_negative_values() -> None:
    with pytest.raises(ValidationError):
        EmployeePayrollBreakdown(
            run_id=" ",
            employee_id="emp-1",
            employee_name="Taylor",
            wages="500",
            employee_taxes="50",
            employer_taxes="60",
            employee_retirement_deferral="20",
            employer_retirement_contribution="10",
        )

    with pytest.raises(ValidationError):
        EmployeePayrollBreakdown(
            run_id="run-1",
            employee_id="emp-1",
            employee_name="Taylor",
            wages="-1",
            employee_taxes="50",
            employer_taxes="60",
            employee_retirement_deferral="20",
            employer_retirement_contribution="10",
        )


def test_employee_payroll_breakdown_trims_fields_and_quantizes() -> None:
    breakdown = EmployeePayrollBreakdown(
        run_id=" run-9 ",
        employee_id=" emp-9 ",
        employee_name=" Taylor ",
        wages="100.105",
        employee_taxes="10.104",
        employer_taxes="12.106",
        employee_retirement_deferral="5.105",
        employer_retirement_contribution="2.104",
    )

    assert breakdown.run_id == "run-9"
    assert breakdown.employee_id == "emp-9"
    assert breakdown.employee_name == "Taylor"
    assert breakdown.wages == Decimal("100.11")
    assert breakdown.employee_taxes == Decimal("10.10")
    assert breakdown.employer_taxes == Decimal("12.11")
    assert breakdown.employee_retirement_deferral == Decimal("5.11")
    assert breakdown.employer_retirement_contribution == Decimal("2.10")


def test_payroll_run_rejects_blank_and_invalid_amounts() -> None:
    with pytest.raises(ValidationError):
        PayrollRun(
            run_id=" ",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 15),
            pay_date=date(2025, 1, 20),
            wages="100.00",
            employee_taxes="10.00",
            employer_taxes="12.00",
            employee_retirement_deferral="5.00",
            employer_retirement_contribution="2.00",
        )

    with pytest.raises(ValidationError):
        PayrollRun(
            run_id="run-1",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 15),
            pay_date=date(2025, 1, 20),
            wages="not-a-number",
            employee_taxes="10.00",
            employer_taxes="12.00",
            employee_retirement_deferral="5.00",
            employer_retirement_contribution="2.00",
        )

    with pytest.raises(ValidationError):
        PayrollRun(
            run_id="run-1",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 15),
            pay_date=date(2025, 1, 20),
            wages="NaN",
            employee_taxes="10.00",
            employer_taxes="12.00",
            employee_retirement_deferral="5.00",
            employer_retirement_contribution="2.00",
        )


def test_company_payroll_summary_from_runs_separates_categories() -> None:
    runs = [
        PayrollRun(
            run_id="run-1",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 15),
            pay_date=date(2025, 1, 20),
            wages="1000.00",
            employee_taxes="100.00",
            employer_taxes="120.00",
            employee_retirement_deferral="50.00",
            employer_retirement_contribution="25.00",
        ),
        PayrollRun(
            run_id="run-2",
            start_date=date(2025, 1, 16),
            end_date=date(2025, 1, 31),
            pay_date=date(2025, 2, 5),
            wages="1500.00",
            employee_taxes="150.00",
            employer_taxes="180.00",
            employee_retirement_deferral="75.00",
            employer_retirement_contribution="40.00",
        ),
    ]

    summary = CompanyPayrollSummary.from_runs(year=2025, runs=runs)

    assert summary.run_count == 2
    assert summary.wages_total == Decimal("2500.00")
    assert summary.employee_taxes_total == Decimal("250.00")
    assert summary.employer_taxes_total == Decimal("300.00")
    assert summary.employee_retirement_deferral_total == Decimal("125.00")
    assert summary.employer_retirement_contribution_total == Decimal("65.00")


def test_company_payroll_summary_handles_empty_runs() -> None:
    summary = CompanyPayrollSummary.from_runs(year=2025, runs=[])

    assert summary.run_count == 0
    assert summary.wages_total == Decimal("0.00")
    assert summary.employee_taxes_total == Decimal("0.00")
    assert summary.employer_taxes_total == Decimal("0.00")
    assert summary.employee_retirement_deferral_total == Decimal("0.00")
    assert summary.employer_retirement_contribution_total == Decimal("0.00")


def test_company_payroll_summary_rejects_negative_totals() -> None:
    with pytest.raises(ValidationError):
        CompanyPayrollSummary(
            year=2025,
            run_count=1,
            wages_total="-1.00",
            employee_taxes_total="0.00",
            employer_taxes_total="0.00",
            employee_retirement_deferral_total="0.00",
            employer_retirement_contribution_total="0.00",
        )
