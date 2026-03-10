"""Retained earnings reconciliation helpers."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Protocol

from cpapacket.core.filesystem import atomic_write, ensure_directory
from cpapacket.deliverables.general_ledger import (
    fetch_general_ledger_monthly_slices,
    merge_general_ledger_monthly_slices,
)
from cpapacket.models.distributions import MiscodedDistributionCandidate
from cpapacket.models.general_ledger import GeneralLedgerRow
from cpapacket.models.retained_earnings import RetainedEarningsRollforward
from cpapacket.reconciliation.miscode_detector import MiscodeDetector
from cpapacket.utils.constants import DELIVERABLE_FOLDERS, RETAINED_EARNINGS_TOLERANCE

_DISTRIBUTION_ACCOUNT_KEYWORDS = (
    "distribution",
    "draw",
    "dividend",
    "shareholder",
    "stockholder",
    "member",
    "partner",
)
_NON_DISTRIBUTION_EQUITY_KEYWORDS = (
    "retained earnings",
    "common stock",
    "preferred stock",
    "paid in capital",
    "additional paid in capital",
    "opening balance equity",
)


@dataclass(frozen=True)
class ReMiscodingIntegrationResult:
    """Result of retained-earnings miscoded distribution integration."""

    candidates: list[MiscodedDistributionCandidate]
    csv_path: Path
    wrote_csv: bool


@dataclass(frozen=True)
class RetainedEarningsSourceData:
    """Cross-deliverable retained-earnings source values from provider layer."""

    beginning_retained_earnings: Decimal
    net_income: Decimal
    distributions: Decimal
    actual_ending_retained_earnings: Decimal
    gl_rows: list[GeneralLedgerRow]


class RetainedEarningsDataProvider(Protocol):
    """Provider contract for retained-earnings rollforward source retrieval."""

    def get_balance_sheet(self, year: int, as_of: date | str) -> dict[str, Any]:
        """Return QBO balance sheet payload for year/as-of."""

    def get_pnl(self, year: int, method: str) -> dict[str, Any]:
        """Return QBO P&L payload for year/method."""

    def get_general_ledger(self, year: int, month: int) -> dict[str, Any]:
        """Return QBO general ledger payload for year/month."""


def load_re_source_data(
    *,
    year: int,
    provider: RetainedEarningsDataProvider,
) -> RetainedEarningsSourceData:
    """Load cross-deliverable RE source values via the shared provider layer."""
    prior_year = year - 1
    prior_balance_sheet = provider.get_balance_sheet(prior_year, f"{prior_year}-12-31")
    current_balance_sheet = provider.get_balance_sheet(year, f"{year}-12-31")
    pnl_payload = provider.get_pnl(year, "accrual")
    gl_slices = fetch_general_ledger_monthly_slices(year=year, provider=provider)
    gl_rows = list(merge_general_ledger_monthly_slices(gl_slices))

    return RetainedEarningsSourceData(
        beginning_retained_earnings=extract_retained_earnings_from_balance_sheet(
            prior_balance_sheet
        ),
        net_income=extract_net_income_from_pnl_report(pnl_payload),
        distributions=extract_distribution_total(gl_rows),
        actual_ending_retained_earnings=extract_retained_earnings_from_balance_sheet(
            current_balance_sheet
        ),
        gl_rows=gl_rows,
    )


def build_retained_earnings_rollforward(
    *,
    source: RetainedEarningsSourceData,
    structural_flags: list[str],
) -> RetainedEarningsRollforward:
    """Construct a canonical retained earnings rollforward result."""
    expected = (
        source.beginning_retained_earnings
        + source.net_income
        - source.distributions
    )
    difference = expected - source.actual_ending_retained_earnings
    status = (
        "Balanced"
        if difference.copy_abs() <= RETAINED_EARNINGS_TOLERANCE
        else "Mismatch"
    )
    return RetainedEarningsRollforward(
        beginning_re=source.beginning_retained_earnings,
        net_income=source.net_income,
        distributions=source.distributions,
        expected_ending_re=expected,
        actual_ending_re=source.actual_ending_retained_earnings,
        difference=difference,
        status=status,
        flags=structural_flags,
    )


def extract_net_income_from_pnl_report(report_payload: dict[str, object]) -> Decimal:
    """Extract bottom-line net income/loss from QBO P&L report payload."""
    rows_node = report_payload.get("Rows")
    if not isinstance(rows_node, dict):
        return Decimal("0.00")

    rows = rows_node.get("Row")
    if not isinstance(rows, list):
        return Decimal("0.00")

    extracted = _search_net_income_value(rows)
    if extracted is None:
        return Decimal("0.00")
    return extracted.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def extract_retained_earnings_from_balance_sheet(report_payload: dict[str, object]) -> Decimal:
    """Extract retained earnings from a QBO balance-sheet payload."""
    rows_node = report_payload.get("Rows")
    if not isinstance(rows_node, dict):
        return Decimal("0.00")
    rows = rows_node.get("Row")
    if not isinstance(rows, list):
        return Decimal("0.00")

    extracted = _search_retained_earnings_value(rows)
    if extracted is None:
        return Decimal("0.00")
    return extracted.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def extract_distribution_total(gl_rows: list[GeneralLedgerRow]) -> Decimal:
    """Sum GL signed amounts for equity distribution/draw/shareholder rows."""
    total = Decimal("0.00")
    for row in gl_rows:
        if not _is_distribution_equity_row(row):
            continue
        total += row.signed_amount
    return total.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def evaluate_re_structural_flags(
    *,
    net_income: Decimal,
    distributions: Decimal,
    actual_ending_re: Decimal,
    gl_rows: list[GeneralLedgerRow],
) -> list[str]:
    """Return non-blocking retained-earnings structural warning flags."""
    flags: list[str] = []

    if distributions > net_income:
        flags.append("basis_risk_distributions_exceed_net_income")

    if actual_ending_re < Decimal("0"):
        flags.append("negative_ending_retained_earnings")

    if _has_direct_retained_earnings_posting(gl_rows):
        flags.append("direct_retained_earnings_postings_detected")

    return flags


def integrate_miscoded_distributions(
    *,
    gl_rows: list[GeneralLedgerRow],
    owner_keywords: list[str],
    packet_root: Path,
    year: int,
    detector: MiscodeDetector | None = None,
) -> ReMiscodingIntegrationResult:
    """Run shared miscoding detection and ensure shared CSV artifact exists."""
    active_detector = detector or MiscodeDetector()
    candidates = active_detector.scan(gl_rows, owner_keywords)

    distributions_dir = ensure_directory(packet_root / DELIVERABLE_FOLDERS["distributions"])
    csv_path = distributions_dir / f"likely_miscoded_distributions_{year}.csv"

    if csv_path.exists():
        return ReMiscodingIntegrationResult(
            candidates=candidates,
            csv_path=csv_path,
            wrote_csv=False,
        )

    _write_likely_miscoded_csv(csv_path, candidates)
    return ReMiscodingIntegrationResult(candidates=candidates, csv_path=csv_path, wrote_csv=True)


def _write_likely_miscoded_csv(
    path: Path,
    candidates: list[MiscodedDistributionCandidate],
) -> None:
    with atomic_write(path, mode="w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "txn_id",
                "date",
                "transaction_type",
                "payee",
                "memo",
                "account",
                "amount",
                "score",
                "confidence",
                "reason_codes",
            ]
        )
        for candidate in candidates:
            writer.writerow(
                [
                    candidate.txn_id,
                    candidate.date.isoformat(),
                    candidate.transaction_type,
                    candidate.payee or "",
                    candidate.memo or "",
                    candidate.account,
                    f"{candidate.amount:.2f}",
                    candidate.score,
                    candidate.confidence,
                    "|".join(candidate.reason_codes),
                ]
            )


def _has_direct_retained_earnings_posting(gl_rows: list[GeneralLedgerRow]) -> bool:
    for row in gl_rows:
        account_name = row.account_name.lower()
        if "retained earnings" in account_name:
            return True
    return False


def _is_distribution_equity_row(row: GeneralLedgerRow) -> bool:
    account_type = row.account_type.strip().lower()
    account_name = row.account_name.strip().lower()
    memo = (row.memo or "").strip().lower()

    has_equity_signal = "equity" in account_type or "equity" in account_name
    has_distribution_signal = any(
        keyword in account_name or keyword in memo
        for keyword in _DISTRIBUTION_ACCOUNT_KEYWORDS
    )
    has_non_distribution_signal = any(
        keyword in account_name for keyword in _NON_DISTRIBUTION_EQUITY_KEYWORDS
    )

    return has_equity_signal and has_distribution_signal and not has_non_distribution_signal


def _search_net_income_value(rows: list[object]) -> Decimal | None:
    for row in rows:
        if not isinstance(row, dict):
            continue

        for container_key in ("Summary", "Header"):
            container = row.get(container_key)
            value = _extract_net_income_from_coldata(container)
            if value is not None:
                return value

        value = _extract_net_income_from_coldata(row)
        if value is not None:
            return value

        nested_rows = row.get("Rows")
        if isinstance(nested_rows, dict):
            child_rows = nested_rows.get("Row")
            if isinstance(child_rows, list):
                nested_value = _search_net_income_value(child_rows)
                if nested_value is not None:
                    return nested_value
    return None


def _extract_net_income_from_coldata(node: object) -> Decimal | None:
    if not isinstance(node, dict):
        return None
    col_data = node.get("ColData")
    if not isinstance(col_data, list) or len(col_data) < 2:
        return None

    label_raw = col_data[0]
    value_raw = col_data[1]
    if not isinstance(label_raw, dict) or not isinstance(value_raw, dict):
        return None

    label = str(label_raw.get("value", "")).strip().lower()
    if not ("net income" in label or "net loss" in label):
        return None

    return _parse_decimal_amount(str(value_raw.get("value", "")).strip())


def _parse_decimal_amount(raw: str) -> Decimal:
    if raw == "":
        return Decimal("0.00")
    cleaned = raw.replace(",", "").replace("$", "")
    negative = cleaned.startswith("(") and cleaned.endswith(")")
    if negative:
        cleaned = cleaned[1:-1]
    try:
        amount = Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return Decimal("0.00")
    return -amount if negative else amount


def _search_retained_earnings_value(rows: list[object]) -> Decimal | None:
    for row in rows:
        if not isinstance(row, dict):
            continue

        label, value = _extract_label_and_amount(row)
        if label is not None and "retained earnings" in label:
            return value

        header = row.get("Header")
        if isinstance(header, dict):
            label, value = _extract_label_and_amount(header)
            if label is not None and "retained earnings" in label:
                return value

        nested_rows = row.get("Rows")
        if isinstance(nested_rows, dict):
            child_rows = nested_rows.get("Row")
            if isinstance(child_rows, list):
                nested_value = _search_retained_earnings_value(child_rows)
                if nested_value is not None:
                    return nested_value
    return None


def _extract_label_and_amount(node: object) -> tuple[str | None, Decimal]:
    if not isinstance(node, dict):
        return None, Decimal("0.00")
    col_data = node.get("ColData")
    if not isinstance(col_data, list) or len(col_data) < 2:
        return None, Decimal("0.00")
    left = col_data[0]
    right = col_data[1]
    if not isinstance(left, dict) or not isinstance(right, dict):
        return None, Decimal("0.00")
    label = str(left.get("value", "")).strip().lower()
    amount = _parse_decimal_amount(str(right.get("value", "")).strip())
    return label, amount
