"""Retained earnings reconciliation helpers."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

from cpapacket.core.filesystem import atomic_write, ensure_directory
from cpapacket.models.distributions import MiscodedDistributionCandidate
from cpapacket.models.general_ledger import GeneralLedgerRow
from cpapacket.reconciliation.miscode_detector import MiscodeDetector
from cpapacket.utils.constants import DELIVERABLE_FOLDERS


@dataclass(frozen=True)
class ReMiscodingIntegrationResult:
    """Result of retained-earnings miscoded distribution integration."""

    candidates: list[MiscodedDistributionCandidate]
    csv_path: Path
    wrote_csv: bool


def extract_distribution_total(gl_rows: list[GeneralLedgerRow]) -> Decimal:
    """Sum GL signed amounts for equity distribution/draw/shareholder rows."""
    total = Decimal("0.00")
    for row in gl_rows:
        account_type = row.account_type.lower()
        account_name = row.account_name.lower()
        if "equity" not in account_type:
            continue
        if not any(keyword in account_name for keyword in ("distribution", "draw", "shareholder")):
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
        return ReMiscodingIntegrationResult(candidates=candidates, csv_path=csv_path, wrote_csv=False)

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
