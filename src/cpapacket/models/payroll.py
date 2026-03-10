"""Payroll domain models used by payroll-summary and reconciliation workflows."""

from __future__ import annotations

from datetime import date
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation

from pydantic import BaseModel, ConfigDict, Field, field_validator

_TWO_PLACES = Decimal("0.01")


def _coerce_money(value: object) -> Decimal:
    try:
        decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("must be a valid decimal value") from exc

    if not decimal_value.is_finite():
        raise ValueError("must be finite")
    if decimal_value < Decimal("0"):
        raise ValueError("must be >= 0")
    return decimal_value.quantize(_TWO_PLACES, rounding=ROUND_HALF_UP)


class PayrollRun(BaseModel):
    """Company-level totals for a single payroll run."""

    model_config = ConfigDict(frozen=True)

    run_id: str
    start_date: date
    end_date: date
    pay_date: date
    wages: Decimal = Field(ge=Decimal("0.00"))
    employee_taxes: Decimal = Field(ge=Decimal("0.00"))
    employer_taxes: Decimal = Field(ge=Decimal("0.00"))
    employee_retirement_deferral: Decimal = Field(ge=Decimal("0.00"))
    employer_retirement_contribution: Decimal = Field(ge=Decimal("0.00"))

    @field_validator("run_id")
    @classmethod
    def _non_blank_id(cls, value: str) -> str:
        trimmed = value.strip()
        if not trimmed:
            raise ValueError("must not be blank")
        return trimmed

    @field_validator(
        "wages",
        "employee_taxes",
        "employer_taxes",
        "employee_retirement_deferral",
        "employer_retirement_contribution",
        mode="before",
    )
    @classmethod
    def _coerce_totals(cls, value: object) -> Decimal:
        return _coerce_money(value)


class EmployeePayrollBreakdown(BaseModel):
    """Per-employee payroll breakdown for one payroll run."""

    model_config = ConfigDict(frozen=True)

    run_id: str
    employee_id: str
    employee_name: str
    wages: Decimal = Field(ge=Decimal("0.00"))
    employee_taxes: Decimal = Field(ge=Decimal("0.00"))
    employer_taxes: Decimal = Field(ge=Decimal("0.00"))
    employee_retirement_deferral: Decimal = Field(ge=Decimal("0.00"))
    employer_retirement_contribution: Decimal = Field(ge=Decimal("0.00"))

    @field_validator("run_id", "employee_id", "employee_name")
    @classmethod
    def _non_blank_required_fields(cls, value: str) -> str:
        trimmed = value.strip()
        if not trimmed:
            raise ValueError("must not be blank")
        return trimmed

    @field_validator(
        "wages",
        "employee_taxes",
        "employer_taxes",
        "employee_retirement_deferral",
        "employer_retirement_contribution",
        mode="before",
    )
    @classmethod
    def _coerce_totals(cls, value: object) -> Decimal:
        return _coerce_money(value)


class CompanyPayrollSummary(BaseModel):
    """Tax-year company payroll totals split by required accounting categories."""

    model_config = ConfigDict(frozen=True)

    year: int = Field(ge=1)
    run_count: int = Field(ge=0)
    wages_total: Decimal = Field(ge=Decimal("0.00"))
    employee_taxes_total: Decimal = Field(ge=Decimal("0.00"))
    employer_taxes_total: Decimal = Field(ge=Decimal("0.00"))
    employee_retirement_deferral_total: Decimal = Field(ge=Decimal("0.00"))
    employer_retirement_contribution_total: Decimal = Field(ge=Decimal("0.00"))

    @field_validator(
        "wages_total",
        "employee_taxes_total",
        "employer_taxes_total",
        "employee_retirement_deferral_total",
        "employer_retirement_contribution_total",
        mode="before",
    )
    @classmethod
    def _coerce_totals(cls, value: object) -> Decimal:
        return _coerce_money(value)

    @classmethod
    def from_runs(cls, *, year: int, runs: list[PayrollRun]) -> CompanyPayrollSummary:
        """Build a yearly company summary from normalized payroll runs."""
        return cls(
            year=year,
            run_count=len(runs),
            wages_total=sum((run.wages for run in runs), Decimal("0.00")),
            employee_taxes_total=sum((run.employee_taxes for run in runs), Decimal("0.00")),
            employer_taxes_total=sum((run.employer_taxes for run in runs), Decimal("0.00")),
            employee_retirement_deferral_total=sum(
                (run.employee_retirement_deferral for run in runs),
                Decimal("0.00"),
            ),
            employer_retirement_contribution_total=sum(
                (run.employer_retirement_contribution for run in runs),
                Decimal("0.00"),
            ),
        )
