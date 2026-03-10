"""Retained earnings reconciliation helpers."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path
from typing import IO, Any, Literal, Protocol, cast

from cpapacket.core.filesystem import atomic_write, ensure_directory
from cpapacket.deliverables.balance_sheet import normalize_balance_sheet_rows
from cpapacket.deliverables.general_ledger import (
    fetch_general_ledger_monthly_slices,
    merge_general_ledger_monthly_slices,
)
from cpapacket.models.distributions import MiscodedDistributionCandidate
from cpapacket.models.general_ledger import GeneralLedgerRow
from cpapacket.models.normalized import NormalizedRow
from cpapacket.models.retained_earnings import RetainedEarningsRollforward
from cpapacket.reconciliation.miscode_detector import MiscodeDetector
from cpapacket.utils.constants import DELIVERABLE_FOLDERS, RETAINED_EARNINGS_TOLERANCE

_CENT = Decimal("0.01")
_ZERO = Decimal("0.00")
_DISTRIBUTION_ACCOUNT_KEYWORDS = (
    "distribution",
    "draw",
    "dividend",
    "shareholder",
    "stockholder",
    "member",
    "partner",
)
_CONTRIBUTION_ACCOUNT_KEYWORDS = (
    "contribution",
    "contributions",
    "capital",
    "paid in capital",
    "additional paid in capital",
)
_SHAREHOLDER_RECEIVABLE_KEYWORDS = (
    "shareholder receivable",
    "due from shareholder",
    "loan to shareholder",
    "shareholder loan",
)
_NON_DISTRIBUTION_EQUITY_KEYWORDS = (
    "retained earnings",
    "common stock",
    "preferred stock",
    "opening balance equity",
)


@dataclass(frozen=True)
class ReMiscodingIntegrationResult:
    """Result of retained-earnings miscoded distribution integration."""

    candidates: list[MiscodedDistributionCandidate]
    csv_path: Path
    wrote_csv: bool


@dataclass(frozen=True)
class EquityTieOutRow:
    """CPA-facing balance-sheet line classification used in equity review outputs."""

    year: int
    as_of_date: str
    source_statement: str
    line_label: str
    classification: str
    amount: Decimal
    included_in_book_equity_bucket: bool
    bucket_component: str
    review_note: str


@dataclass(frozen=True)
class EquityActivityRow:
    """One GL activity row relevant to owner/equity review."""

    date: date
    txn_type: str
    doc_num: str
    payee: str
    account_name: str
    memo: str
    debit: Decimal
    credit: Decimal
    signed_amount: Decimal
    classification: str
    review_flag: str


@dataclass(frozen=True)
class DistributionBridgeDetailRow:
    """Best-effort bridge detail row for distribution reconciliation review."""

    date: date
    txn_type: str
    doc_num: str
    payee: str
    account_name: str
    memo: str
    signed_amount: Decimal
    bridge_bucket: str
    reason: str


@dataclass(frozen=True)
class DistributionBalanceBridge:
    """High-level GL-vs-balance-sheet distribution bridge."""

    prior_distribution_balance: Decimal
    current_distribution_balance: Decimal
    distribution_total_gl: Decimal
    distribution_total_bs_change: Decimal
    difference: Decimal
    status: str


@dataclass(frozen=True)
class RetainedEarningsSourceData:
    """Cross-deliverable equity-review source values from provider layer."""

    beginning_book_equity_bucket: Decimal
    net_income: Decimal
    distributions_gl: Decimal
    distributions_bs_change: Decimal
    contributions: Decimal
    other_direct_equity_postings: Decimal
    actual_ending_book_equity_bucket: Decimal
    shareholder_receivable_ending_balance: Decimal
    gl_rows: list[GeneralLedgerRow]
    equity_tie_out_rows: list[EquityTieOutRow]
    distribution_activity_rows: list[EquityActivityRow]
    shareholder_receivable_rows: list[EquityActivityRow]
    direct_equity_rows: list[EquityActivityRow]
    distribution_bridge_detail_rows: list[DistributionBridgeDetailRow]
    distribution_balance_bridge: DistributionBalanceBridge


class RetainedEarningsDataProvider(Protocol):
    """Provider contract for retained-earnings rollforward source retrieval."""

    def get_balance_sheet(self, year: int, as_of: date | str) -> dict[str, Any]:
        """Return QBO balance sheet payload for year/as-of."""

    def get_pnl(self, year: int, method: str) -> dict[str, Any]:
        """Return QBO P&L payload for year/method."""

    def get_general_ledger(self, year: int, month: int) -> dict[str, Any]:
        """Return QBO general ledger payload for year/month."""

    def get_general_ledger_with_source(
        self,
        year: int,
        month: int,
    ) -> tuple[dict[str, Any], str]:
        """Return QBO general ledger payload and its source marker."""


class DistributionDetector(Protocol):
    """Narrow detector protocol used by miscoded-distribution integration tests."""

    def scan(
        self,
        gl_rows: list[GeneralLedgerRow],
        owner_keywords: list[str],
    ) -> list[MiscodedDistributionCandidate]:
        """Return candidate miscoded distribution rows."""


def load_re_source_data(
    *,
    year: int,
    provider: RetainedEarningsDataProvider,
) -> RetainedEarningsSourceData:
    """Load cross-deliverable equity-review source values via the provider layer."""
    prior_year = year - 1
    prior_as_of = f"{prior_year}-12-31"
    current_as_of = f"{year}-12-31"

    prior_balance_sheet = provider.get_balance_sheet(prior_year, prior_as_of)
    current_balance_sheet = provider.get_balance_sheet(year, current_as_of)
    pnl_payload = provider.get_pnl(year, "accrual")
    gl_slices = fetch_general_ledger_monthly_slices(year=year, provider=provider)
    gl_rows = list(merge_general_ledger_monthly_slices(gl_slices))

    prior_equity_rows = extract_equity_tie_out_rows(
        report_payload=prior_balance_sheet,
        year=year,
        as_of_date=prior_as_of,
        source_statement="prior_balance_sheet",
    )
    current_equity_rows = extract_equity_tie_out_rows(
        report_payload=current_balance_sheet,
        year=year,
        as_of_date=current_as_of,
        source_statement="current_balance_sheet",
    )

    prior_distribution_balance, _ = extract_distribution_balance_from_balance_sheet(
        prior_balance_sheet
    )
    current_distribution_balance, _ = extract_distribution_balance_from_balance_sheet(
        current_balance_sheet
    )
    distributions_gl = extract_distribution_total(gl_rows)
    distributions_bs_change = (
        (current_distribution_balance - prior_distribution_balance)
        .copy_abs()
        .quantize(_CENT, rounding=ROUND_HALF_UP)
    )
    contributions = extract_contribution_total(gl_rows)
    direct_equity_rows = extract_direct_equity_posting_rows(gl_rows)
    other_direct_equity_postings = sum(
        (_equity_effect_amount_from_activity(row) for row in direct_equity_rows),
        _ZERO,
    ).quantize(_CENT, rounding=ROUND_HALF_UP)
    shareholder_receivable_rows = extract_shareholder_receivable_activity_rows(gl_rows)
    shareholder_receivable_ending_balance = (
        extract_shareholder_receivable_balance_from_balance_sheet(current_balance_sheet)
    )

    distribution_balance_bridge = DistributionBalanceBridge(
        prior_distribution_balance=prior_distribution_balance,
        current_distribution_balance=current_distribution_balance,
        distribution_total_gl=distributions_gl,
        distribution_total_bs_change=distributions_bs_change,
        difference=(distributions_gl - distributions_bs_change).quantize(
            _CENT,
            rounding=ROUND_HALF_UP,
        ),
        status=(
            "Balanced"
            if (distributions_gl - distributions_bs_change).copy_abs()
            <= RETAINED_EARNINGS_TOLERANCE
            else "Review"
        ),
    )

    return RetainedEarningsSourceData(
        beginning_book_equity_bucket=_sum_book_equity_bucket(
            prior_equity_rows,
            source_statement="prior_balance_sheet",
        ),
        net_income=extract_net_income_from_pnl_report(pnl_payload),
        distributions_gl=distributions_gl,
        distributions_bs_change=distributions_bs_change,
        contributions=contributions,
        other_direct_equity_postings=other_direct_equity_postings,
        actual_ending_book_equity_bucket=_sum_book_equity_bucket(
            current_equity_rows,
            source_statement="current_balance_sheet",
        ),
        shareholder_receivable_ending_balance=shareholder_receivable_ending_balance,
        gl_rows=gl_rows,
        equity_tie_out_rows=[*prior_equity_rows, *current_equity_rows],
        distribution_activity_rows=extract_distribution_activity_rows(gl_rows),
        shareholder_receivable_rows=shareholder_receivable_rows,
        direct_equity_rows=direct_equity_rows,
        distribution_bridge_detail_rows=build_distribution_bridge_detail_rows(gl_rows),
        distribution_balance_bridge=distribution_balance_bridge,
    )


def build_retained_earnings_rollforward(
    *,
    source: RetainedEarningsSourceData,
    structural_flags: list[str],
) -> RetainedEarningsRollforward:
    """Construct a canonical book-equity rollforward result."""
    expected_gl_basis = (
        source.beginning_book_equity_bucket
        + source.net_income
        - source.distributions_gl
        + source.contributions
        + source.other_direct_equity_postings
    ).quantize(_CENT, rounding=ROUND_HALF_UP)
    expected_bs_basis = (
        source.beginning_book_equity_bucket
        + source.net_income
        - source.distributions_bs_change
        + source.contributions
        + source.other_direct_equity_postings
    ).quantize(_CENT, rounding=ROUND_HALF_UP)
    gl_basis_difference = (expected_gl_basis - source.actual_ending_book_equity_bucket).quantize(
        _CENT,
        rounding=ROUND_HALF_UP,
    )
    bs_basis_difference = (expected_bs_basis - source.actual_ending_book_equity_bucket).quantize(
        _CENT,
        rounding=ROUND_HALF_UP,
    )
    status: Literal["Balanced", "Review"] = (
        "Balanced"
        if gl_basis_difference.copy_abs() <= RETAINED_EARNINGS_TOLERANCE
        and bs_basis_difference.copy_abs() <= RETAINED_EARNINGS_TOLERANCE
        else "Review"
    )
    return RetainedEarningsRollforward(
        beginning_book_equity_bucket=source.beginning_book_equity_bucket,
        current_year_net_income=source.net_income,
        current_year_distributions_gl=source.distributions_gl,
        current_year_distributions_bs_change=source.distributions_bs_change,
        current_year_contributions=source.contributions,
        other_direct_equity_postings=source.other_direct_equity_postings,
        expected_ending_book_equity_bucket_gl_basis=expected_gl_basis,
        expected_ending_book_equity_bucket_bs_basis=expected_bs_basis,
        actual_ending_book_equity_bucket=source.actual_ending_book_equity_bucket,
        gl_basis_difference=gl_basis_difference,
        bs_basis_difference=bs_basis_difference,
        status=status,
        flags=structural_flags,
    )


def extract_net_income_from_pnl_report(report_payload: dict[str, object]) -> Decimal:
    """Extract bottom-line net income/loss from QBO P&L report payload."""
    rows_node = report_payload.get("Rows")
    if not isinstance(rows_node, dict):
        return _ZERO

    rows = rows_node.get("Row")
    if not isinstance(rows, list):
        return _ZERO

    extracted = _search_net_income_value(rows)
    if extracted is None:
        return _ZERO
    return extracted.quantize(_CENT, rounding=ROUND_HALF_UP)


def extract_retained_earnings_from_balance_sheet(report_payload: dict[str, object]) -> Decimal:
    """Extract the book-equity bucket used by the CPA-facing rollforward."""
    return extract_book_equity_bucket_from_balance_sheet(report_payload)


def extract_book_equity_bucket_from_balance_sheet(report_payload: dict[str, object]) -> Decimal:
    """Extract the stricter book-equity bucket from a QBO balance sheet."""
    rows = extract_equity_tie_out_rows(
        report_payload=report_payload,
        year=0,
        as_of_date="",
        source_statement="balance_sheet",
    )
    return _sum_book_equity_bucket(rows, source_statement="balance_sheet")


def extract_distribution_total(gl_rows: list[GeneralLedgerRow]) -> Decimal:
    """Sum distribution-style equity activity from GL rows on a positive basis."""
    total = _ZERO
    for row in gl_rows:
        if not _is_distribution_equity_row(row):
            continue
        total += _equity_effect_amount(row)
    return total.copy_abs().quantize(_CENT, rounding=ROUND_HALF_UP)


def extract_contribution_total(gl_rows: list[GeneralLedgerRow]) -> Decimal:
    """Sum contribution-style equity activity using equity-effect signs."""
    total = _ZERO
    for row in gl_rows:
        if not _is_contribution_equity_row(row):
            continue
        total += _equity_effect_amount(row)
    return total.quantize(_CENT, rounding=ROUND_HALF_UP)


def extract_distribution_balance_from_balance_sheet(
    report_payload: dict[str, object],
) -> tuple[Decimal, bool]:
    """Extract the current balance of distribution-style equity accounts from QBO BS."""
    tie_out_rows = extract_equity_tie_out_rows(
        report_payload=report_payload,
        year=0,
        as_of_date="",
        source_statement="balance_sheet",
    )
    balance = sum(
        (row.amount for row in tie_out_rows if row.classification == "distribution_equity"),
        _ZERO,
    ).quantize(_CENT, rounding=ROUND_HALF_UP)
    return balance, any(row.classification == "distribution_equity" for row in tie_out_rows)


def extract_shareholder_receivable_balance_from_balance_sheet(
    report_payload: dict[str, object],
) -> Decimal:
    """Extract ending shareholder receivable balance from the balance sheet."""
    rows = normalize_balance_sheet_rows(report_payload)
    balance = sum(
        (
            row.amount
            for row in rows
            if row.row_type == "account" and _is_shareholder_receivable_label(row.label)
        ),
        _ZERO,
    )
    return balance.quantize(_CENT, rounding=ROUND_HALF_UP)


def extract_equity_tie_out_rows(
    *,
    report_payload: dict[str, object],
    year: int,
    as_of_date: str,
    source_statement: str,
) -> list[EquityTieOutRow]:
    """Classify BS account lines into CPA-facing equity review rows."""
    normalized_rows = normalize_balance_sheet_rows(report_payload)
    output: list[EquityTieOutRow] = []
    for row in normalized_rows:
        if row.row_type != "account":
            continue

        classification = _classify_balance_sheet_row(row)
        if classification is None:
            continue

        included = classification in {"retained_earnings", "current_net_income"}
        output.append(
            EquityTieOutRow(
                year=year,
                as_of_date=as_of_date,
                source_statement=source_statement,
                line_label=row.label,
                classification=classification,
                amount=row.amount.quantize(_CENT, rounding=ROUND_HALF_UP),
                included_in_book_equity_bucket=included,
                bucket_component=classification if included else "",
                review_note=_review_note_for_balance_sheet_classification(classification),
            )
        )
    return output


def extract_distribution_activity_rows(gl_rows: list[GeneralLedgerRow]) -> list[EquityActivityRow]:
    """Return GL rows useful for the distributions deliverable review tables."""
    output: list[EquityActivityRow] = []
    for row in gl_rows:
        classification = _distribution_activity_classification(row)
        if classification is None:
            continue
        output.append(
            _to_equity_activity_row(
                row,
                classification=classification,
                review_flag=_distribution_review_flag(classification),
            )
        )
    return output


def extract_shareholder_receivable_activity_rows(
    gl_rows: list[GeneralLedgerRow],
) -> list[EquityActivityRow]:
    """Return shareholder-receivable activity rows for manual review."""
    return [
        _to_equity_activity_row(
            row,
            classification="shareholder_receivable",
            review_flag="shareholder_receivable_review",
        )
        for row in gl_rows
        if _is_shareholder_receivable_row(row)
    ]


def extract_direct_equity_posting_rows(gl_rows: list[GeneralLedgerRow]) -> list[EquityActivityRow]:
    """Return direct/nonstandard equity posting rows for manual review."""
    return [
        _to_equity_activity_row(
            row,
            classification="other_equity",
            review_flag="direct_equity_posting_review",
        )
        for row in gl_rows
        if _is_direct_equity_posting_row(row)
    ]


def build_distribution_bridge_detail_rows(
    gl_rows: list[GeneralLedgerRow],
) -> list[DistributionBridgeDetailRow]:
    """Build a best-effort detail schedule for GL-vs-BS distribution review."""
    rows: list[DistributionBridgeDetailRow] = []
    for row in gl_rows:
        if _is_distribution_equity_row(row):
            bridge_bucket = "in_gl_only"
            reason = "Included in GL distribution total."
        elif _is_contribution_equity_row(row):
            bridge_bucket = "needs_review"
            reason = "Contribution equity activity shown separately from distributions."
        elif _is_direct_equity_posting_row(row):
            bridge_bucket = "needs_review"
            reason = "Direct equity posting may explain GL-vs-BS differences."
        else:
            continue
        rows.append(
            DistributionBridgeDetailRow(
                date=row.date,
                txn_type=row.transaction_type,
                doc_num=row.document_number,
                payee=row.payee or "",
                account_name=row.account_name,
                memo=row.memo or "",
                signed_amount=row.signed_amount,
                bridge_bucket=bridge_bucket,
                reason=reason,
            )
        )
    return rows


def evaluate_re_structural_flags(
    *,
    net_income: Decimal,
    distributions_gl: Decimal,
    distributions_bs_change: Decimal,
    actual_ending_book_equity_bucket: Decimal,
    shareholder_receivable_ending_balance: Decimal,
    gl_rows: list[GeneralLedgerRow],
) -> list[str]:
    """Return non-blocking equity-review structural warning flags."""
    flags: list[str] = []

    if (distributions_gl - distributions_bs_change).copy_abs() > RETAINED_EARNINGS_TOLERANCE:
        flags.append("distributions_gl_vs_bs_mismatch")

    if distributions_gl > net_income or distributions_bs_change > net_income:
        flags.append("distributions_exceed_current_year_income")

    if actual_ending_book_equity_bucket < _ZERO:
        flags.append("negative_ending_book_equity")

    if shareholder_receivable_ending_balance.copy_abs() > RETAINED_EARNINGS_TOLERANCE:
        flags.append("shareholder_receivable_present")

    if _has_direct_retained_earnings_posting(gl_rows):
        flags.append("direct_retained_earnings_postings_detected")

    return flags


def integrate_miscoded_distributions(
    *,
    gl_rows: list[GeneralLedgerRow],
    owner_keywords: list[str],
    packet_root: Path,
    year: int,
    detector: DistributionDetector | None = None,
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
        writer = csv.writer(cast(IO[str], handle))
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


def _sum_book_equity_bucket(
    rows: list[EquityTieOutRow],
    *,
    source_statement: str,
) -> Decimal:
    return sum(
        (
            row.amount
            for row in rows
            if row.source_statement == source_statement and row.included_in_book_equity_bucket
        ),
        _ZERO,
    ).quantize(_CENT, rounding=ROUND_HALF_UP)


def _classify_balance_sheet_row(row: NormalizedRow) -> str | None:
    label = row.label.strip().lower()

    if _is_shareholder_receivable_label(row.label):
        return "shareholder_receivable"
    if row.section != "Equity":
        return None
    if "retained earnings" in label:
        return "retained_earnings"
    if "net income" in label or "net loss" in label:
        return "current_net_income"
    if any(keyword in label for keyword in _CONTRIBUTION_ACCOUNT_KEYWORDS):
        return "contribution_equity"
    if "opening balance equity" in label:
        return "opening_balance_equity"
    if any(keyword in label for keyword in ("distribution", "draw", "dividend")):
        return "distribution_equity"
    return "other_equity"


def _review_note_for_balance_sheet_classification(classification: str) -> str:
    if classification in {"retained_earnings", "current_net_income"}:
        return "Included in book-equity bucket."
    if classification == "distribution_equity":
        return "Tracked separately from the book-equity bucket."
    if classification == "contribution_equity":
        return "Tracked separately as owner contributions."
    if classification == "shareholder_receivable":
        return "Owner-related asset balance requiring CPA review."
    if classification == "opening_balance_equity":
        return "Excluded from book-equity bucket."
    return "Excluded from book-equity bucket; review classification."


def _distribution_activity_classification(row: GeneralLedgerRow) -> str | None:
    if _is_distribution_equity_row(row):
        return "distribution"
    if _is_contribution_equity_row(row):
        return "contribution"
    if _is_owner_related_non_distribution_row(row):
        return "owner_related_non_distribution"
    if _is_direct_equity_posting_row(row):
        return "needs_review"
    return None


def _distribution_review_flag(classification: str) -> str:
    if classification == "distribution":
        return "distribution_activity"
    if classification == "contribution":
        return "contribution_activity"
    if classification == "owner_related_non_distribution":
        return "owner_related_review"
    return "equity_review"


def _to_equity_activity_row(
    row: GeneralLedgerRow,
    *,
    classification: str,
    review_flag: str,
) -> EquityActivityRow:
    return EquityActivityRow(
        date=row.date,
        txn_type=row.transaction_type,
        doc_num=row.document_number,
        payee=row.payee or "",
        account_name=row.account_name,
        memo=row.memo or "",
        debit=row.debit,
        credit=row.credit,
        signed_amount=row.signed_amount,
        classification=classification,
        review_flag=review_flag,
    )


def _equity_effect_amount(row: GeneralLedgerRow) -> Decimal:
    return (row.credit - row.debit).quantize(_CENT, rounding=ROUND_HALF_UP)


def _equity_effect_amount_from_activity(row: EquityActivityRow) -> Decimal:
    return (row.credit - row.debit).quantize(_CENT, rounding=ROUND_HALF_UP)


def _has_direct_retained_earnings_posting(gl_rows: list[GeneralLedgerRow]) -> bool:
    return any("retained earnings" in row.account_name.lower() for row in gl_rows)


def _is_distribution_equity_row(row: GeneralLedgerRow) -> bool:
    account_type = row.account_type.strip().lower()
    account_name = row.account_name.strip().lower()
    memo = (row.memo or "").strip().lower()

    has_equity_signal = "equity" in account_type or "equity" in account_name
    has_distribution_signal = any(
        keyword in account_name or keyword in memo for keyword in _DISTRIBUTION_ACCOUNT_KEYWORDS
    )
    non_distribution_keywords = (
        *_CONTRIBUTION_ACCOUNT_KEYWORDS,
        *_NON_DISTRIBUTION_EQUITY_KEYWORDS,
    )
    has_non_distribution_signal = any(
        keyword in account_name for keyword in non_distribution_keywords
    )

    return has_equity_signal and has_distribution_signal and not has_non_distribution_signal


def _is_contribution_equity_row(row: GeneralLedgerRow) -> bool:
    account_type = row.account_type.strip().lower()
    account_name = row.account_name.strip().lower()
    has_equity_signal = "equity" in account_type or "equity" in account_name
    return has_equity_signal and any(
        keyword in account_name for keyword in _CONTRIBUTION_ACCOUNT_KEYWORDS
    )


def _is_shareholder_receivable_row(row: GeneralLedgerRow) -> bool:
    haystacks = (
        row.account_name.strip().lower(),
        (row.memo or "").strip().lower(),
    )
    return any(
        keyword in haystacks[0] or keyword in haystacks[1]
        for keyword in _SHAREHOLDER_RECEIVABLE_KEYWORDS
    )


def _is_shareholder_receivable_label(label: str) -> bool:
    lowered = label.strip().lower()
    return any(keyword in lowered for keyword in _SHAREHOLDER_RECEIVABLE_KEYWORDS)


def _is_direct_equity_posting_row(row: GeneralLedgerRow) -> bool:
    account_type = row.account_type.strip().lower()
    account_name = row.account_name.strip().lower()
    if not ("equity" in account_type or "equity" in account_name):
        return False
    if _is_distribution_equity_row(row) or _is_contribution_equity_row(row):
        return False
    return not any(keyword in account_name for keyword in ("net income",))


def _is_owner_related_non_distribution_row(row: GeneralLedgerRow) -> bool:
    haystacks = (
        row.account_name.strip().lower(),
        (row.memo or "").strip().lower(),
        (row.payee or "").strip().lower(),
    )
    has_owner_signal = any("owner" in text or "shareholder" in text for text in haystacks)
    return has_owner_signal and not (
        _is_distribution_equity_row(row)
        or _is_contribution_equity_row(row)
        or _is_shareholder_receivable_row(row)
    )


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
        return _ZERO
    cleaned = raw.replace(",", "").replace("$", "")
    negative = cleaned.startswith("(") and cleaned.endswith(")")
    if negative:
        cleaned = cleaned[1:-1]
    try:
        amount = Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return _ZERO
    return -amount if negative else amount
