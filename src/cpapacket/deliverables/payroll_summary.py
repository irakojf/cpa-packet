"""Payroll normalization helpers for Gusto annual payroll data."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any

from cpapacket.models.payroll import EmployeePayrollBreakdown, PayrollRun

_ZERO = Decimal("0.00")
_CENT = Decimal("0.01")


def normalize_gusto_payload(
    payload: Mapping[str, Any],
) -> tuple[list[PayrollRun], list[EmployeePayrollBreakdown]]:
    """Normalize fixture/API payload to payroll domain models."""
    raw_runs = payload.get("payrolls")
    if not isinstance(raw_runs, list):
        return [], []
    return (
        normalize_payroll_runs(raw_runs),
        normalize_employee_breakdowns(raw_runs),
    )


def normalize_payroll_runs(raw_runs: list[Any]) -> list[PayrollRun]:
    """Normalize Gusto payroll run data into company-level run models."""
    normalized: list[PayrollRun] = []
    for raw in raw_runs:
        if not isinstance(raw, Mapping):
            continue
        run_id = str(raw.get("uuid", "")).strip()
        start_date = _parse_date(raw.get("pay_period_start_date"))
        end_date = _parse_date(raw.get("pay_period_end_date"))
        pay_date = _parse_date(raw.get("check_date"))
        if not run_id or start_date is None or end_date is None or pay_date is None:
            continue

        totals = raw.get("totals")
        totals_mapping = totals if isinstance(totals, Mapping) else {}
        employee_compensations = _employee_compensations(raw)

        wages_total = _to_money(totals_mapping.get("gross_pay"))
        if wages_total == _ZERO:
            wages_total = sum((_employee_wages(comp) for comp in employee_compensations), _ZERO)

        employee_retirement = sum(
            (_to_money(comp.get("employee_401k")) for comp in employee_compensations),
            _ZERO,
        )
        employer_retirement = sum(
            (_to_money(comp.get("employer_401k")) for comp in employee_compensations),
            _ZERO,
        )

        normalized.append(
            PayrollRun(
                run_id=run_id,
                start_date=start_date,
                end_date=end_date,
                pay_date=pay_date,
                wages=wages_total,
                employee_taxes=_to_money(totals_mapping.get("employee_taxes")),
                employer_taxes=_to_money(totals_mapping.get("employer_taxes")),
                employee_retirement_deferral=employee_retirement,
                employer_retirement_contribution=employer_retirement,
            )
        )
    return normalized


def normalize_employee_breakdowns(raw_runs: list[Any]) -> list[EmployeePayrollBreakdown]:
    """Normalize Gusto payroll run data into per-employee run breakdown models."""
    normalized: list[EmployeePayrollBreakdown] = []
    for raw in raw_runs:
        if not isinstance(raw, Mapping):
            continue
        run_id = str(raw.get("uuid", "")).strip()
        if not run_id:
            continue

        employee_compensations = _employee_compensations(raw)
        wages_by_employee = [_employee_wages(comp) for comp in employee_compensations]
        totals = raw.get("totals")
        totals_mapping = totals if isinstance(totals, Mapping) else {}
        run_employee_taxes = _to_money(totals_mapping.get("employee_taxes"))
        run_employer_taxes = _to_money(totals_mapping.get("employer_taxes"))

        employee_taxes_by_employee = _collect_or_allocate_taxes(
            employee_compensations, wages_by_employee, run_employee_taxes, key="employee_taxes"
        )
        employer_taxes_by_employee = _collect_or_allocate_taxes(
            employee_compensations, wages_by_employee, run_employer_taxes, key="employer_taxes"
        )

        for index, compensation in enumerate(employee_compensations):
            employee_id = str(compensation.get("employee_uuid", "")).strip()
            employee_name = str(compensation.get("employee_name", "")).strip()
            if not employee_id or not employee_name:
                continue
            normalized.append(
                EmployeePayrollBreakdown(
                    run_id=run_id,
                    employee_id=employee_id,
                    employee_name=employee_name,
                    wages=wages_by_employee[index],
                    employee_taxes=employee_taxes_by_employee[index],
                    employer_taxes=employer_taxes_by_employee[index],
                    employee_retirement_deferral=_to_money(compensation.get("employee_401k")),
                    employer_retirement_contribution=_to_money(compensation.get("employer_401k")),
                )
            )
    return normalized


def _employee_compensations(raw_run: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    raw_compensations = raw_run.get("employee_compensations")
    if not isinstance(raw_compensations, list):
        return []
    return [item for item in raw_compensations if isinstance(item, Mapping)]


def _collect_or_allocate_taxes(
    employee_compensations: list[Mapping[str, Any]],
    wages_by_employee: list[Decimal],
    run_total: Decimal,
    *,
    key: str,
) -> list[Decimal]:
    parsed: list[Decimal | None] = []
    for compensation in employee_compensations:
        if key in compensation:
            parsed.append(_to_money(compensation.get(key)))
        else:
            parsed.append(None)

    if all(value is not None for value in parsed):
        return [value if value is not None else _ZERO for value in parsed]

    return _allocate_proportional(total=run_total, weights=wages_by_employee)


def _allocate_proportional(*, total: Decimal, weights: list[Decimal]) -> list[Decimal]:
    if not weights:
        return []
    total_weight = sum(weights, _ZERO)
    if total <= _ZERO or total_weight <= _ZERO:
        return [_ZERO for _ in weights]

    allocated: list[Decimal] = []
    running = _ZERO
    for weight in weights[:-1]:
        share = ((total * weight) / total_weight).quantize(_CENT, rounding=ROUND_HALF_UP)
        allocated.append(share)
        running += share
    allocated.append((total - running).quantize(_CENT, rounding=ROUND_HALF_UP))
    return allocated


def _employee_wages(compensation: Mapping[str, Any]) -> Decimal:
    return (
        _to_money(compensation.get("regular_pay"))
        + _to_money(compensation.get("bonus_pay"))
        + _to_money(compensation.get("overtime_pay"))
    ).quantize(_CENT, rounding=ROUND_HALF_UP)


def _to_money(value: Any) -> Decimal:
    if value is None:
        return _ZERO
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return _ZERO
    if not parsed.is_finite():
        return _ZERO
    return parsed.quantize(_CENT, rounding=ROUND_HALF_UP)


def _parse_date(value: Any) -> date | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None
