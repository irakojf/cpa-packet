"""Payroll reconciliation core calculation helpers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any, Literal, Protocol

from cpapacket.deliverables.payroll_summary import normalize_payroll_runs
from cpapacket.models.payroll import PayrollRun
from cpapacket.utils.constants import PAYROLL_RECON_TOLERANCE

_TWO_PLACES = Decimal("0.01")
_ZERO = Decimal("0.00")
_PAYROLL_NAME_KEYWORDS = ("payroll", "salary", "wages", "employer tax", "401(k)", "401k")
_PAYROLL_ACCOUNT_TYPES = ("expense", "cost of goods sold", "cogs")

PayrollReconStatus = Literal["RECONCILED", "MISMATCH"]


@dataclass(frozen=True)
class PayrollReconciliation:
    """Computed payroll reconciliation outcome."""

    gusto_total: Decimal
    qbo_total: Decimal
    variance: Decimal
    status: PayrollReconStatus
    tolerance: Decimal


@dataclass(frozen=True)
class QboPayrollDetection:
    """QBO payroll-account matching summary."""

    total: Decimal
    matched_account_count: int


class PayrollRunsProvider(Protocol):
    """Subset of provider contract required for payroll reconciliation totals."""

    def get_payroll_runs(self, year: int) -> list[dict[str, Any]]: ...


class PayrollAccountsProvider(Protocol):
    """Subset of provider contract required for QBO payroll account totals."""

    def get_accounts(self) -> dict[str, Any]: ...


def reconcile_payroll_totals(
    *,
    gusto_total: Decimal,
    qbo_total: Decimal,
    tolerance: Decimal = PAYROLL_RECON_TOLERANCE,
) -> PayrollReconciliation:
    """Compare Gusto and QBO totals and determine reconciliation status."""
    gusto = _coerce_decimal(gusto_total)
    qbo = _coerce_decimal(qbo_total)
    allowed_variance = _coerce_non_negative_decimal(tolerance)

    variance = (qbo - gusto).quantize(_TWO_PLACES, rounding=ROUND_HALF_UP)
    status: PayrollReconStatus = "RECONCILED" if abs(variance) <= allowed_variance else "MISMATCH"
    return PayrollReconciliation(
        gusto_total=gusto,
        qbo_total=qbo,
        variance=variance,
        status=status,
        tolerance=allowed_variance,
    )


def compute_gusto_reconciliation_total(payroll_runs: list[PayrollRun]) -> Decimal:
    """Compute reconciliation Gusto total from normalized payroll runs."""
    total = Decimal("0.00")
    for run in payroll_runs:
        total += run.wages + run.employer_taxes + run.employer_retirement_contribution
    return total.quantize(_TWO_PLACES, rounding=ROUND_HALF_UP)


def fetch_gusto_reconciliation_total(*, providers: PayrollRunsProvider, year: int) -> Decimal:
    """Fetch annual Gusto payroll runs via providers and compute reconciliation total."""
    raw_runs = providers.get_payroll_runs(year)
    runs = normalize_payroll_runs(raw_runs)
    return compute_gusto_reconciliation_total(runs)


def compute_qbo_payroll_total(accounts_payload: Mapping[str, Any]) -> Decimal:
    """Compute QBO payroll total from account payload and account balances."""
    return detect_qbo_payroll_accounts(accounts_payload).total


def fetch_qbo_payroll_total(*, providers: PayrollAccountsProvider) -> Decimal:
    """Fetch QBO accounts via providers and compute payroll-account total."""
    return compute_qbo_payroll_total(providers.get_accounts())


def detect_qbo_payroll_accounts(accounts_payload: Mapping[str, Any]) -> QboPayrollDetection:
    """Detect matching QBO payroll accounts and compute summed total."""
    matching_accounts = _matching_qbo_payroll_accounts(accounts_payload)
    total = _ZERO
    for account in matching_accounts:
        total += _extract_account_balance(account)
    return QboPayrollDetection(
        total=total.quantize(_TWO_PLACES, rounding=ROUND_HALF_UP),
        matched_account_count=len(matching_accounts),
    )


def collect_payroll_recon_edge_warnings(
    *,
    reconciliation: PayrollReconciliation,
    matched_qbo_accounts: int,
) -> list[str]:
    """Return non-blocking warnings for known payroll reconciliation edge cases."""
    warnings: list[str] = []
    if matched_qbo_accounts == 0 and reconciliation.gusto_total > _ZERO:
        warnings.append(
            "No matching QBO payroll accounts were found; "
            "variance likely reflects full Gusto total."
        )
    if reconciliation.status == "RECONCILED" and reconciliation.variance != _ZERO:
        warnings.append(
            "Minor payroll variance is within tolerance; possible Gusto-to-QBO sync lag."
        )
    if reconciliation.status == "MISMATCH" and reconciliation.variance > reconciliation.tolerance:
        warnings.append(
            "QBO payroll total exceeds Gusto beyond tolerance; check for manual payroll journals."
        )
    return warnings


def _coerce_decimal(value: object) -> Decimal:
    try:
        decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("must be a valid decimal value") from exc

    if not decimal_value.is_finite():
        raise ValueError("must be finite")
    return decimal_value.quantize(_TWO_PLACES, rounding=ROUND_HALF_UP)


def _coerce_non_negative_decimal(value: object) -> Decimal:
    decimal_value = _coerce_decimal(value)
    if decimal_value < Decimal("0"):
        raise ValueError("tolerance must be >= 0")
    return decimal_value


def _is_payroll_account(account: Mapping[str, Any]) -> bool:
    account_type = str(account.get("AccountType", "")).strip().lower()
    if account_type not in _PAYROLL_ACCOUNT_TYPES:
        return False

    name = str(account.get("Name", "")).strip().lower()
    return any(keyword in name for keyword in _PAYROLL_NAME_KEYWORDS)


def _extract_account_balance(account: Mapping[str, Any]) -> Decimal:
    for key in ("CurrentBalance", "Balance"):
        if key not in account:
            continue
        raw = account.get(key)
        if raw is None:
            continue
        with_balance = str(raw).strip()
        if with_balance == "":
            continue
        try:
            return _coerce_decimal(with_balance)
        except ValueError:
            return _ZERO
    return _ZERO


def _matching_qbo_payroll_accounts(accounts_payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    query_response = accounts_payload.get("QueryResponse")
    if not isinstance(query_response, Mapping):
        return []
    accounts = query_response.get("Account")
    if not isinstance(accounts, list):
        return []

    output: list[Mapping[str, Any]] = []
    for account in accounts:
        if not isinstance(account, Mapping):
            continue
        if _is_payroll_account(account):
            output.append(account)
    return output
