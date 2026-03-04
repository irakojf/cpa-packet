"""Payroll normalization helpers for Gusto annual payroll data."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from datetime import date
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Protocol, TypedDict, cast

from cpapacket.core.context import RunContext
from cpapacket.core.filesystem import ensure_directory, sanitize_filesystem_name
from cpapacket.core.metadata import (
    DeliverableMetadata,
    compute_input_fingerprint,
    write_deliverable_metadata,
)
from cpapacket.deliverables.base import DeliverableResult
from cpapacket.models.payroll import CompanyPayrollSummary, EmployeePayrollBreakdown, PayrollRun
from cpapacket.utils.constants import DELIVERABLE_FOLDERS, SCHEMA_VERSIONS
from cpapacket.utils.prompts import resolve_output_path
from cpapacket.writers.csv_writer import CsvWriter
from cpapacket.writers.json_writer import JsonWriter
from cpapacket.writers.pdf_writer import PdfBodyLine, PdfWriter

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


def build_company_summary(
    *,
    year: int,
    payroll_runs: list[PayrollRun],
) -> tuple[CompanyPayrollSummary, Decimal]:
    """Build company summary + payroll cost (excluding employee withholdings)."""
    summary = CompanyPayrollSummary.from_runs(year=year, runs=payroll_runs)
    payroll_cost_total = (
        summary.wages_total
        + summary.employer_taxes_total
        + summary.employer_retirement_contribution_total
    ).quantize(_CENT, rounding=ROUND_HALF_UP)
    return summary, payroll_cost_total


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


class _EmployeeTotals(TypedDict):
    employee_name: str
    wages: Decimal
    employee_taxes: Decimal
    employer_taxes: Decimal
    employee_retirement_deferral: Decimal
    employer_retirement_contribution: Decimal


def aggregate_employee_breakdowns(
    breakdowns: list[EmployeePayrollBreakdown],
) -> list[tuple[str, str, Decimal, Decimal, Decimal, Decimal, Decimal]]:
    """Aggregate per-employee various totals across payroll runs."""
    totals: dict[str, _EmployeeTotals] = {}
    for breakdown in breakdowns:
        employee = totals.setdefault(
            breakdown.employee_id,
            {
                "employee_name": breakdown.employee_name,
                "wages": _ZERO,
                "employee_taxes": _ZERO,
                "employer_taxes": _ZERO,
                "employee_retirement_deferral": _ZERO,
                "employer_retirement_contribution": _ZERO,
            },
        )
        employee["wages"] += breakdown.wages
        employee["employee_taxes"] += breakdown.employee_taxes
        employee["employer_taxes"] += breakdown.employer_taxes
        employee["employee_retirement_deferral"] += breakdown.employee_retirement_deferral
        employee["employer_retirement_contribution"] += breakdown.employer_retirement_contribution

    return [
        (
            employee_id,
            data["employee_name"],
            data["wages"],
            data["employee_taxes"],
            data["employer_taxes"],
            data["employee_retirement_deferral"],
            data["employer_retirement_contribution"],
        )
        for employee_id, data in sorted(totals.items())
    ]


def total_401k_contributions(
    runs: list[PayrollRun],
) -> tuple[Decimal, Decimal]:
    """Return sums of employee deferrals and employer contributions across runs."""
    employee_total = sum((run.employee_retirement_deferral for run in runs), Decimal("0.00"))
    employer_total = sum((run.employer_retirement_contribution for run in runs), Decimal("0.00"))
    return employee_total, employer_total


class PayrollDataProvider(Protocol):
    """Provider interface required by PayrollSummaryDeliverable."""

    def get_payroll_runs(self, year: int) -> list[dict[str, Any]]:
        """Return annual payroll runs from Gusto or cache."""


class PayrollSummaryDeliverable:
    """Annual payroll summary deliverable (company + per-employee outputs)."""

    key = "payroll_summary"
    folder = DELIVERABLE_FOLDERS["payroll_summary"]
    required = True
    dependencies: list[str] = []
    requires_gusto = True

    def gather_prompts(self, _ctx: object) -> dict[str, Any]:
        return {}

    def is_current(self, _ctx: object) -> bool:
        return False

    def generate(
        self,
        ctx: RunContext,
        store: PayrollDataProvider,
        prompts: dict[str, Any],
    ) -> DeliverableResult:
        del prompts

        if not ctx.gusto_available:
            return DeliverableResult(
                deliverable_key=self.key,
                success=True,
                warnings=["Skipped payroll summary; Gusto not connected."],
            )

        raw_runs = store.get_payroll_runs(ctx.year)
        runs = normalize_payroll_runs(raw_runs)
        employee_breakdowns = normalize_employee_breakdowns(raw_runs)
        company_summary, payroll_cost_total = build_company_summary(
            year=ctx.year,
            payroll_runs=runs,
        )
        raw_payload: dict[str, Any] = {"year": ctx.year, "payrolls": raw_runs}
        artifacts = write_payroll_output_artifacts(
            ctx=ctx,
            company_summary=company_summary,
            payroll_cost_total=payroll_cost_total,
            payroll_runs=runs,
            employee_breakdowns=employee_breakdowns,
            raw_payload=raw_payload,
        )
        warnings: list[str] = []
        if not runs:
            warnings.append("No payroll runs found; generated zero-value payroll summary.")
        return DeliverableResult(
            deliverable_key=self.key,
            success=True,
            artifacts=artifacts,
            warnings=warnings,
        )


def write_payroll_output_artifacts(
    *,
    ctx: RunContext,
    company_summary: CompanyPayrollSummary,
    payroll_cost_total: Decimal,
    payroll_runs: list[PayrollRun],
    employee_breakdowns: list[EmployeePayrollBreakdown],
    raw_payload: Mapping[str, Any],
) -> list[str]:
    """Write payroll summary artifacts (company + per-employee) and metadata."""
    deliverable_dir = ensure_directory(ctx.out_dir / DELIVERABLE_FOLDERS["payroll_summary"])
    company_dir = ensure_directory(deliverable_dir / "00_Company_Summary")
    employees_dir = ensure_directory(deliverable_dir / "Employees")

    artifacts: list[Path] = []

    company_base_name = f"Annual_Payroll_Summary_{ctx.year}"
    company_csv_path = _resolve_output_path(
        company_dir / f"{company_base_name}.csv",
        on_conflict=ctx.on_conflict,
        non_interactive=ctx.non_interactive,
    )
    company_pdf_path = _resolve_output_path(
        company_dir / f"{company_base_name}.pdf",
        on_conflict=ctx.on_conflict,
        non_interactive=ctx.non_interactive,
    )
    company_raw_path = (
        None
        if ctx.no_raw
        else _resolve_output_path(
            company_dir / f"{company_base_name}.raw.json",
            on_conflict=ctx.on_conflict,
            non_interactive=ctx.non_interactive,
        )
    )

    _write_company_summary_csv(
        path=company_csv_path,
        company_summary=company_summary,
        payroll_cost_total=payroll_cost_total,
    )
    _write_company_summary_pdf(
        path=company_pdf_path,
        year=ctx.year,
        company_summary=company_summary,
        payroll_cost_total=payroll_cost_total,
    )
    if company_raw_path is not None:
        JsonWriter().write_payload(
            company_raw_path,
            payload=raw_payload,
            no_raw=False,
            redact=ctx.redact,
        )

    artifacts.extend([company_csv_path, company_pdf_path])
    if company_raw_path is not None:
        artifacts.append(company_raw_path)

    by_employee: dict[str, list[EmployeePayrollBreakdown]] = defaultdict(list)
    employee_names: dict[str, str] = {}
    for breakdown in employee_breakdowns:
        by_employee[breakdown.employee_id].append(breakdown)
        employee_names[breakdown.employee_id] = breakdown.employee_name

    for employee_id in sorted(by_employee):
        employee_name = employee_names[employee_id]
        employee_folder = _employee_folder_name(
            employee_name=employee_name, employee_id=employee_id
        )
        employee_dir = ensure_directory(employees_dir / employee_folder)
        employee_base_name = (
            f"Payroll_Breakdown_{sanitize_filesystem_name(employee_name)}_{ctx.year}"
        )

        employee_csv_path = _resolve_output_path(
            employee_dir / f"{employee_base_name}.csv",
            on_conflict=ctx.on_conflict,
            non_interactive=ctx.non_interactive,
        )
        employee_pdf_path = _resolve_output_path(
            employee_dir / f"{employee_base_name}.pdf",
            on_conflict=ctx.on_conflict,
            non_interactive=ctx.non_interactive,
        )
        employee_raw_path = (
            None
            if ctx.no_raw
            else _resolve_output_path(
                employee_dir / f"{employee_base_name}.raw.json",
                on_conflict=ctx.on_conflict,
                non_interactive=ctx.non_interactive,
            )
        )

        _write_employee_breakdown_csv(
            path=employee_csv_path,
            employee_id=employee_id,
            employee_name=employee_name,
            employee_rows=by_employee[employee_id],
        )
        _write_employee_breakdown_pdf(
            path=employee_pdf_path,
            year=ctx.year,
            employee_id=employee_id,
            employee_name=employee_name,
            employee_rows=by_employee[employee_id],
        )
        if employee_raw_path is not None:
            _write_employee_breakdown_raw(
                path=employee_raw_path,
                employee_id=employee_id,
                employee_name=employee_name,
                employee_rows=by_employee[employee_id],
                redact=ctx.redact,
            )

        artifacts.extend([employee_csv_path, employee_pdf_path])
        if employee_raw_path is not None:
            artifacts.append(employee_raw_path)

    metadata_key = f"payroll_summary_{ctx.year}"
    metadata_path = _resolve_output_path(
        ctx.out_dir / "_meta" / "private" / "deliverables" / f"{metadata_key}_metadata.json",
        on_conflict=ctx.on_conflict,
        non_interactive=ctx.non_interactive,
    )
    metadata_inputs = {
        "year": ctx.year,
        "company_run_count": company_summary.run_count,
        "employee_count": len(by_employee),
        "company_wages_total": str(company_summary.wages_total),
        "payroll_cost_total": str(payroll_cost_total),
        "no_raw": ctx.no_raw,
        "redact": ctx.redact,
        "source_run_count": len(payroll_runs),
    }
    metadata = DeliverableMetadata(
        deliverable=metadata_key,
        inputs=metadata_inputs,
        input_fingerprint=compute_input_fingerprint(metadata_inputs),
        schema_versions=SCHEMA_VERSIONS.get("payroll_summary", {}),
        artifacts=[str(path) for path in artifacts],
    )
    write_deliverable_metadata(metadata_path, metadata)
    artifacts.append(metadata_path)

    return [str(path) for path in artifacts]


def _write_company_summary_csv(
    *,
    path: Path,
    company_summary: CompanyPayrollSummary,
    payroll_cost_total: Decimal,
) -> None:
    CsvWriter().write_rows(
        path,
        fieldnames=[
            "year",
            "run_count",
            "wages_total",
            "employee_taxes_total",
            "employer_taxes_total",
            "employee_retirement_deferral_total",
            "employer_retirement_contribution_total",
            "payroll_cost_total",
        ],
        rows=[
            {
                "year": company_summary.year,
                "run_count": company_summary.run_count,
                "wages_total": company_summary.wages_total,
                "employee_taxes_total": company_summary.employee_taxes_total,
                "employer_taxes_total": company_summary.employer_taxes_total,
                "employee_retirement_deferral_total": (
                    company_summary.employee_retirement_deferral_total
                ),
                "employer_retirement_contribution_total": (
                    company_summary.employer_retirement_contribution_total
                ),
                "payroll_cost_total": payroll_cost_total,
            }
        ],
    )


def _write_company_summary_pdf(
    *,
    path: Path,
    year: int,
    company_summary: CompanyPayrollSummary,
    payroll_cost_total: Decimal,
) -> None:
    PdfWriter().write_report(
        path,
        company_name="Unknown Company",
        report_title="Annual Payroll Summary",
        date_range_label=f"{year}-01-01 to {year}-12-31",
        body_lines=[
            PdfBodyLine(text=f"Run Count: {company_summary.run_count}", row_type="header"),
            PdfBodyLine(text=f"Wages Total: {company_summary.wages_total:.2f}"),
            PdfBodyLine(text=f"Employee Taxes Total: {company_summary.employee_taxes_total:.2f}"),
            PdfBodyLine(text=f"Employer Taxes Total: {company_summary.employer_taxes_total:.2f}"),
            PdfBodyLine(
                text="Employee 401(k) Deferrals: "
                f"{company_summary.employee_retirement_deferral_total:.2f}"
            ),
            PdfBodyLine(
                text="Employer 401(k) Contributions: "
                f"{company_summary.employer_retirement_contribution_total:.2f}"
            ),
            PdfBodyLine(text=f"Payroll Cost Total: {payroll_cost_total:.2f}", row_type="total"),
        ],
    )


def _write_employee_breakdown_csv(
    *,
    path: Path,
    employee_id: str,
    employee_name: str,
    employee_rows: list[EmployeePayrollBreakdown],
) -> None:
    CsvWriter().write_rows(
        path,
        fieldnames=[
            "employee_id",
            "employee_name",
            "run_id",
            "wages",
            "employee_taxes",
            "employer_taxes",
            "employee_retirement_deferral",
            "employer_retirement_contribution",
        ],
        rows=[
            {
                "employee_id": employee_id,
                "employee_name": employee_name,
                "run_id": row.run_id,
                "wages": row.wages,
                "employee_taxes": row.employee_taxes,
                "employer_taxes": row.employer_taxes,
                "employee_retirement_deferral": row.employee_retirement_deferral,
                "employer_retirement_contribution": row.employer_retirement_contribution,
            }
            for row in sorted(employee_rows, key=lambda item: item.run_id)
        ],
    )


def _write_employee_breakdown_pdf(
    *,
    path: Path,
    year: int,
    employee_id: str,
    employee_name: str,
    employee_rows: list[EmployeePayrollBreakdown],
) -> None:
    totals = _employee_totals(employee_rows)
    PdfWriter().write_report(
        path,
        company_name="Unknown Company",
        report_title=f"Payroll Breakdown - {employee_name}",
        date_range_label=f"{year}-01-01 to {year}-12-31",
        body_lines=[
            PdfBodyLine(text=f"Employee ID: {employee_id}", row_type="header"),
            PdfBodyLine(text=f"Runs Included: {len(employee_rows)}"),
            PdfBodyLine(text=f"Wages Total: {totals['wages']:.2f}"),
            PdfBodyLine(text=f"Employee Taxes Total: {totals['employee_taxes']:.2f}"),
            PdfBodyLine(text=f"Employer Taxes Total: {totals['employer_taxes']:.2f}"),
            PdfBodyLine(
                text=f"Employee 401(k) Deferrals: {totals['employee_retirement_deferral']:.2f}"
            ),
            PdfBodyLine(
                text="Employer 401(k) Contributions: "
                f"{totals['employer_retirement_contribution']:.2f}",
                row_type="total",
            ),
        ],
    )


def _write_employee_breakdown_raw(
    *,
    path: Path,
    employee_id: str,
    employee_name: str,
    employee_rows: list[EmployeePayrollBreakdown],
    redact: bool,
) -> None:
    payload = {
        "employee_id": employee_id,
        "employee_name": employee_name,
        "runs": [
            {
                "run_id": row.run_id,
                "wages": f"{row.wages:.2f}",
                "employee_taxes": f"{row.employee_taxes:.2f}",
                "employer_taxes": f"{row.employer_taxes:.2f}",
                "employee_retirement_deferral": f"{row.employee_retirement_deferral:.2f}",
                "employer_retirement_contribution": f"{row.employer_retirement_contribution:.2f}",
            }
            for row in sorted(employee_rows, key=lambda item: item.run_id)
        ],
    }
    JsonWriter().write_payload(path, payload=payload, no_raw=False, redact=redact)


def _employee_folder_name(*, employee_name: str, employee_id: str) -> str:
    name_parts = [part for part in employee_name.strip().split() if part]
    if len(name_parts) >= 2:
        base = f"{name_parts[-1]}_{name_parts[0]}"
    elif name_parts:
        base = name_parts[0]
    else:
        base = "Unknown"
    return cast(str, sanitize_filesystem_name(f"{base}_emp_{employee_id}"))


def _employee_totals(employee_rows: list[EmployeePayrollBreakdown]) -> dict[str, Decimal]:
    return {
        "wages": sum((row.wages for row in employee_rows), _ZERO),
        "employee_taxes": sum((row.employee_taxes for row in employee_rows), _ZERO),
        "employer_taxes": sum((row.employer_taxes for row in employee_rows), _ZERO),
        "employee_retirement_deferral": sum(
            (row.employee_retirement_deferral for row in employee_rows),
            _ZERO,
        ),
        "employer_retirement_contribution": sum(
            (row.employer_retirement_contribution for row in employee_rows),
            _ZERO,
        ),
    }


def _resolve_output_path(path: Path, *, on_conflict: str, non_interactive: bool) -> Path:
    normalized_conflict = None if on_conflict == "prompt" else on_conflict
    return cast(
        Path,
        resolve_output_path(
            path,
            on_conflict=normalized_conflict,
            non_interactive=non_interactive,
        ),
    )
