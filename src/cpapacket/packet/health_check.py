"""QBO data health pre-check orchestration and report output."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import IO, Any, Literal, Protocol, cast

from pydantic import BaseModel, ConfigDict, Field

from cpapacket.core.filesystem import atomic_write
from cpapacket.utils.constants import BALANCE_EQUATION_TOLERANCE

HealthCheckSeverity = Literal["warning"]
_UNCATEGORIZED_ACCOUNT_NAMES = {"uncategorized income", "uncategorized expense"}
_UNDEPOSITED_FUNDS_ACCOUNT = "undeposited funds"
_SUSPENSE_ACCOUNT_NAMES = {"ask my accountant", "suspense"}
_OPEN_ITEM_ACCOUNT_TYPES = {"accountsreceivable", "accountspayable"}
_ZERO = Decimal("0.00")
_SYNC_STATUS_OK = {"completed", "synced", "success", "succeeded"}


class DataHealthIssue(BaseModel):
    """One non-blocking data quality issue discovered during pre-check."""

    model_config = ConfigDict(frozen=True)

    code: str
    title: str
    message: str
    severity: HealthCheckSeverity = "warning"
    metadata: dict[str, str] = Field(default_factory=dict)


class DataHealthReport(BaseModel):
    """Complete result payload for one data health pre-check run."""

    model_config = ConfigDict(frozen=True)

    year: int = Field(ge=2000, le=9999)
    generated_at: str
    issues: list[DataHealthIssue] = Field(default_factory=list)
    check_names: list[str] = Field(default_factory=list)

    @property
    def has_issues(self) -> bool:
        return bool(self.issues)


@dataclass(frozen=True, slots=True)
class DataHealthCheckContext:
    """Inputs available to each health check implementation."""

    year: int
    providers: Any
    gusto_connected: bool
    generated_at: datetime = field(default_factory=lambda: datetime.now(UTC))


class DataHealthCheck(Protocol):
    """Callable protocol for health check implementations."""

    def __call__(
        self, context: DataHealthCheckContext
    ) -> DataHealthIssue | list[DataHealthIssue] | None:
        """Run a single check and return issue(s), if any."""


def run_data_health_precheck(
    *,
    context: DataHealthCheckContext,
    checks: Sequence[DataHealthCheck],
) -> DataHealthReport:
    """Execute all checks and collect warning-level issues.

    All check failures are converted into warning issues to keep this pre-check
    non-blocking by design.
    """
    issues: list[DataHealthIssue] = []
    check_names: list[str] = []

    for check in checks:
        check_name = getattr(check, "__name__", check.__class__.__name__)
        check_names.append(check_name)

        try:
            outcome = check(context)
        except Exception as exc:
            issues.append(
                DataHealthIssue(
                    code=f"{check_name}_error",
                    title="Health Check Execution Warning",
                    message=f"{check_name} failed during pre-check.",
                    metadata={"error": str(exc)},
                )
            )
            continue

        if outcome is None:
            continue
        if isinstance(outcome, list):
            issues.extend(outcome)
            continue
        issues.append(outcome)

    return DataHealthReport(
        year=context.year,
        generated_at=context.generated_at.isoformat(),
        issues=issues,
        check_names=check_names,
    )


def render_data_health_report(report: DataHealthReport) -> str:
    """Render a plain-text data health report for `_meta/public` output."""
    lines: list[str] = [
        "cpapacket data health check",
        f"year: {report.year}",
        f"generated_at: {report.generated_at}",
        f"checks_executed: {len(report.check_names)}",
    ]

    if not report.issues:
        lines.append("status: clean")
        lines.append("No data quality warnings detected.")
        return "\n".join(lines) + "\n"

    lines.append("status: warnings")
    lines.append(f"warning_count: {len(report.issues)}")
    lines.append("")

    for index, issue in enumerate(report.issues, start=1):
        lines.append(f"{index}. [{issue.code}] {issue.title}")
        lines.append(f"   {issue.message}")
        if issue.metadata:
            for key in sorted(issue.metadata):
                lines.append(f"   - {key}: {issue.metadata[key]}")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def write_data_health_report(*, output_root: str | Path, report: DataHealthReport) -> Path:
    """Write `_meta/public/data_health_check.txt` atomically and return its path."""
    destination = Path(output_root) / "_meta" / "public" / "data_health_check.txt"
    destination.parent.mkdir(parents=True, exist_ok=True)

    payload = render_data_health_report(report)
    with atomic_write(destination, mode="w", encoding="utf-8", newline="\n") as handle:
        text_handle = cast(IO[str], handle)
        text_handle.write(payload)

    return destination


def prompt_message() -> str:
    """Canonical interactive warning prompt text."""
    return "Data quality issues detected. Continue anyway?"


def should_continue_after_report(
    *,
    report: DataHealthReport,
    non_interactive: bool,
    confirm: Callable[[str], bool] | None = None,
) -> bool:
    """Decide whether build/check execution should continue.

    In non-interactive mode we always continue after logging warnings.
    In interactive mode with warnings, default behavior is "No" unless the
    caller-provided confirm callback explicitly returns True.
    """
    if not report.has_issues:
        return True
    if non_interactive:
        return True
    if confirm is None:
        return False
    return bool(confirm(prompt_message()))


def decimal_metadata(value: Decimal) -> str:
    """Normalize Decimal values for issue metadata serialization."""
    return format(value, "f")


def check_uncategorized_transactions(context: DataHealthCheckContext) -> DataHealthIssue | None:
    """Warn when GL rows indicate activity in uncategorized accounts."""
    providers = context.providers
    issue_count = 0
    amount_total = _ZERO

    for month in range(1, 13):
        payload = providers.get_general_ledger(context.year, month)
        for account_name, amount in _iter_general_ledger_amounts(payload):
            if account_name.strip().lower() in _UNCATEGORIZED_ACCOUNT_NAMES:
                issue_count += 1
                amount_total += abs(amount)

    if issue_count == 0:
        return None

    return DataHealthIssue(
        code="uncategorized_transactions",
        title="Uncategorized Transactions",
        message=(
            "Found uncategorized activity in QuickBooks during the selected tax year. "
            "Review and classify these entries before final packet delivery."
        ),
        metadata={
            "count": str(issue_count),
            "dollar_total": decimal_metadata(amount_total.quantize(Decimal("0.01"))),
        },
    )


def check_undeposited_funds_balance(context: DataHealthCheckContext) -> DataHealthIssue | None:
    """Warn when Undeposited Funds has non-zero year-end balance."""
    as_of = f"{context.year}-12-31"
    payload = context.providers.get_balance_sheet(context.year, as_of)
    balance = _sum_named_amounts(payload=payload, names={_UNDEPOSITED_FUNDS_ACCOUNT})

    if abs(balance) <= BALANCE_EQUATION_TOLERANCE:
        return None

    return DataHealthIssue(
        code="undeposited_funds_balance",
        title="Undeposited Funds Balance",
        message=(
            "Undeposited Funds has a non-zero ending balance as of year-end. "
            "Review bank deposit matching before final packet delivery."
        ),
        metadata={"as_of": as_of, "balance": decimal_metadata(balance.quantize(Decimal("0.01")))},
    )


def check_suspense_accounts_balance(context: DataHealthCheckContext) -> DataHealthIssue | None:
    """Warn when suspense-like accounts have non-zero year-end balance."""
    as_of = f"{context.year}-12-31"
    balance_payload = context.providers.get_balance_sheet(context.year, as_of)
    suspense_balance = _sum_named_amounts(payload=balance_payload, names=_SUSPENSE_ACCOUNT_NAMES)

    if abs(suspense_balance) <= BALANCE_EQUATION_TOLERANCE:
        return None

    return DataHealthIssue(
        code="suspense_balance",
        title="Suspense/Ask My Accountant Balance",
        message=(
            "Suspense-style accounts have a non-zero ending balance at year-end. "
            "Review these balances before final packet delivery."
        ),
        metadata={
            "as_of": as_of,
            "balance": decimal_metadata(suspense_balance.quantize(Decimal("0.01"))),
        },
    )


def check_open_prior_year_items(context: DataHealthCheckContext) -> DataHealthIssue | None:
    """Warn on AR/AP items dated before the target year start."""
    cutoff = date(context.year, 1, 1)
    dedupe_keys: set[tuple[str, str, str, str]] = set()
    open_item_count = 0
    open_item_total = _ZERO

    for month in range(1, 13):
        payload = context.providers.get_general_ledger(context.year, month)
        for entry in _iter_gl_entries(payload):
            entry_date = _parse_iso_date(entry.txn_date)
            if entry_date is None or entry_date >= cutoff:
                continue
            if entry.account_type.strip().lower() not in _OPEN_ITEM_ACCOUNT_TYPES:
                continue

            dedupe_key = (
                entry.txn_id.strip(),
                entry.txn_date.strip(),
                entry.account_type.strip().lower(),
                decimal_metadata(entry.amount.quantize(Decimal("0.01"))),
            )
            if dedupe_key in dedupe_keys:
                continue

            dedupe_keys.add(dedupe_key)
            open_item_count += 1
            open_item_total += abs(entry.amount)

    if open_item_count == 0:
        return None

    return DataHealthIssue(
        code="open_prior_year_items",
        title="Open Prior-Year Items",
        message=(
            "Found AR/AP entries dated before the start of the selected tax year. "
            "Review unpaid invoices and bills carried into the current year."
        ),
        metadata={
            "as_of": cutoff.isoformat(),
            "count": str(open_item_count),
            "dollar_total": decimal_metadata(open_item_total.quantize(Decimal("0.01"))),
        },
    )


def check_payroll_sync_status(context: DataHealthCheckContext) -> DataHealthIssue | None:
    """Warn when latest Gusto payroll run is not confirmed synced to QBO."""
    if not context.gusto_connected:
        return None

    providers = context.providers
    get_runs = getattr(providers, "get_payroll_runs", None)
    if not callable(get_runs):
        return DataHealthIssue(
            code="payroll_sync_status",
            title="Payroll Sync Status",
            message=(
                "Gusto is connected but payroll sync status could not be checked "
                "(provider does not expose payroll runs)."
            ),
        )

    runs = get_runs(context.year)
    if not isinstance(runs, list) or not runs:
        return DataHealthIssue(
            code="payroll_sync_status",
            title="Payroll Sync Status",
            message=(
                "Gusto is connected but no payroll runs were found to verify QBO sync status."
            ),
        )

    latest = _latest_payroll_run(runs)
    if latest is None:
        return DataHealthIssue(
            code="payroll_sync_status",
            title="Payroll Sync Status",
            message=(
                "Gusto is connected but payroll run metadata was invalid; "
                "unable to verify QBO sync status."
            ),
        )

    synced = _is_payroll_synced_to_qbo(latest)
    if synced:
        return None

    return DataHealthIssue(
        code="payroll_sync_status",
        title="Payroll Sync Status",
        message=(
            "Latest payroll run does not appear synced to QuickBooks. "
            "Complete payroll sync before packet delivery."
        ),
        metadata={
            "latest_payroll_uuid": str(latest.get("uuid", "")),
            "check_date": str(latest.get("check_date", "")),
        },
    )


def _latest_payroll_run(runs: list[dict[str, Any]]) -> dict[str, Any] | None:
    with_dates: list[tuple[date, dict[str, Any]]] = []
    for run in runs:
        if not isinstance(run, dict):
            continue
        check_date_raw = run.get("check_date")
        check_date = _parse_iso_date(str(check_date_raw)) if check_date_raw is not None else None
        if check_date is None:
            continue
        with_dates.append((check_date, run))
    if not with_dates:
        return None
    with_dates.sort(key=lambda item: item[0], reverse=True)
    return with_dates[0][1]


def _is_payroll_synced_to_qbo(run: dict[str, Any]) -> bool:
    for key in ("qbo_synced", "qbo_sync_completed"):
        value = run.get(key)
        if isinstance(value, bool):
            return value
    for key in ("qbo_sync_status", "sync_status"):
        value = run.get(key)
        if isinstance(value, str):
            return value.strip().lower() in _SYNC_STATUS_OK
    return False


def _iter_general_ledger_amounts(payload: dict[str, Any]) -> list[tuple[str, Decimal]]:
    rows = _extract_row_list(payload.get("Rows"))
    output: list[tuple[str, Decimal]] = []
    _walk_general_ledger_rows(rows=rows, output=output)
    return output


def _sum_named_amounts(*, payload: dict[str, Any], names: set[str]) -> Decimal:
    total = _ZERO
    normalized_names = {name.strip().lower() for name in names}
    for account_name, amount in _iter_general_ledger_amounts(payload):
        if account_name.strip().lower() in normalized_names:
            total += amount
    return total


def _walk_general_ledger_rows(
    *, rows: list[dict[str, Any]], output: list[tuple[str, Decimal]]
) -> None:
    for row in rows:
        row_type = str(row.get("type", "")).strip().lower()
        if row_type == "section":
            header_col_data = _extract_col_data(row.get("Header"))
            account_name = _col_value(header_col_data, 0)
            amount = _parse_qbo_amount(_col_value(header_col_data, 1))
            if account_name and amount is not None:
                output.append((account_name, amount))
            _walk_general_ledger_rows(rows=_extract_row_list(row.get("Rows")), output=output)
            continue

        col_data = _extract_col_data(row)
        account_name = _col_value(col_data, 0)
        amount = _parse_qbo_amount(_col_value(col_data, 1))
        if not account_name:
            account_name = str(row.get("AccountName", "")).strip()
        if amount is None:
            amount = _parse_qbo_amount(str(row.get("Amount", "")).strip())
        if account_name and amount is not None:
            output.append((account_name, amount))


def _extract_row_list(rows_node: Any) -> list[dict[str, Any]]:
    if not isinstance(rows_node, dict):
        return []
    rows = rows_node.get("Row", [])
    if not isinstance(rows, list):
        return []
    return [item for item in rows if isinstance(item, dict)]


def _extract_col_data(node: Any) -> list[dict[str, Any]]:
    if not isinstance(node, dict):
        return []
    col_data = node.get("ColData", [])
    if not isinstance(col_data, list):
        return []
    return [item for item in col_data if isinstance(item, dict)]


def _col_value(col_data: list[dict[str, Any]], index: int) -> str:
    if index < 0 or index >= len(col_data):
        return ""
    raw_value = col_data[index].get("value", "")
    return str(raw_value).strip()


def _parse_qbo_amount(raw_value: str) -> Decimal | None:
    cleaned = raw_value.strip()
    if not cleaned:
        return None
    negative = cleaned.startswith("(") and cleaned.endswith(")")
    normalized = cleaned.strip("()").replace(",", "").replace("$", "")
    try:
        amount = Decimal(normalized)
    except (InvalidOperation, ValueError):
        return None
    return -amount if negative else amount


@dataclass(frozen=True, slots=True)
class _GeneralLedgerEntry:
    txn_id: str
    txn_date: str
    account_type: str
    amount: Decimal


def _iter_gl_entries(payload: dict[str, Any]) -> list[_GeneralLedgerEntry]:
    rows = _extract_row_list(payload.get("Rows"))
    output: list[_GeneralLedgerEntry] = []
    _walk_gl_entries(rows=rows, output=output)
    return output


def _walk_gl_entries(*, rows: list[dict[str, Any]], output: list[_GeneralLedgerEntry]) -> None:
    for row in rows:
        nested_rows = _extract_row_list(row.get("Rows"))
        if nested_rows:
            _walk_gl_entries(rows=nested_rows, output=output)

        txn_id = str(row.get("TxnId", "")).strip()
        txn_date = str(row.get("TxnDate", "")).strip()
        account_type = str(row.get("AccountType", "")).strip()
        amount = _parse_qbo_amount(str(row.get("Amount", "")).strip())
        if txn_id and txn_date and account_type and amount is not None:
            output.append(
                _GeneralLedgerEntry(
                    txn_id=txn_id,
                    txn_date=txn_date,
                    account_type=account_type,
                    amount=amount,
                )
            )


def _parse_iso_date(raw_value: str) -> date | None:
    cleaned = raw_value.strip()
    if not cleaned:
        return None
    try:
        return date.fromisoformat(cleaned)
    except ValueError:
        return None
