"""Unified miscoded-distribution detection engine."""

from __future__ import annotations

from collections.abc import Iterable
from decimal import ROUND_HALF_UP, Decimal
from typing import Literal

from cpapacket.models.distributions import MiscodedDistributionCandidate
from cpapacket.models.general_ledger import GeneralLedgerRow
from cpapacket.utils.constants import (
    MISCODE_CONFIDENCE_HIGH,
    MISCODE_CONFIDENCE_LOW,
    MISCODE_CONFIDENCE_MEDIUM,
    MISCODE_HIGH_AMOUNT_THRESHOLD,
    MISCODE_ROUND_NUMBER_DIVISOR,
)

_REASON_R1 = "R1_OWNER_PAYEE_EXPENSE"
_REASON_R2 = "R2_MEMO_KEYWORD_EXPENSE"
_REASON_R3 = "R3_TRANSFER_NON_EQUITY_HIGH"
_REASON_R4 = "R4_ROUND_NUMBER_OWNER"
_REASON_R5 = "R5_HIGH_AMOUNT"

_MEMO_KEYWORDS = ("distribution", "owner draw", "reimbursement", "personal", "transfer")
_EQUITY_HINTS = ("equity", "distribution", "draw", "shareholder")


class MiscodeDetector:
    """Scores GL rows against miscoded-distribution heuristics."""

    def scan(
        self,
        gl_rows: Iterable[GeneralLedgerRow],
        owner_keywords: list[str] | tuple[str, ...],
    ) -> list[MiscodedDistributionCandidate]:
        owner_tokens = _normalize_tokens(owner_keywords)
        output: list[MiscodedDistributionCandidate] = []

        for row in gl_rows:
            reason_codes: list[str] = []
            score = 0
            amount = abs(row.signed_amount).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

            is_expense = _is_expense_account(row)
            is_non_equity = not _is_equity_account(row)
            owner_payee = _is_owner_or_shareholder_payee(row, owner_tokens)

            if owner_payee and is_expense:
                reason_codes.append(_REASON_R1)
                score += 3

            if is_expense and _memo_has_keyword(row.memo):
                reason_codes.append(_REASON_R2)
                score += 2

            if (
                _looks_like_transfer_from_bank(row)
                and is_non_equity
                and amount > MISCODE_HIGH_AMOUNT_THRESHOLD
            ):
                reason_codes.append(_REASON_R3)
                score += 2

            if owner_payee and _is_round_number(amount):
                reason_codes.append(_REASON_R4)
                score += 1

            if score > 0 and amount > MISCODE_HIGH_AMOUNT_THRESHOLD:
                reason_codes.append(_REASON_R5)
                score += 1

            if score < MISCODE_CONFIDENCE_LOW:
                continue

            output.append(
                MiscodedDistributionCandidate(
                    txn_id=row.txn_id,
                    date=row.date,
                    transaction_type=row.transaction_type,
                    payee=row.payee,
                    memo=row.memo,
                    account=row.account_name,
                    amount=amount,
                    reason_codes=reason_codes,
                    confidence=_confidence_label(score),
                    score=score,
                )
            )

        return sorted(output, key=lambda item: (item.score, item.amount, item.date), reverse=True)


def _normalize_tokens(items: list[str] | tuple[str, ...]) -> tuple[str, ...]:
    tokens: list[str] = []
    for item in items:
        token = item.strip().lower()
        if token:
            tokens.append(token)
    return tuple(tokens)


def _is_expense_account(row: GeneralLedgerRow) -> bool:
    account_type = row.account_type.lower()
    return "expense" in account_type


def _is_equity_account(row: GeneralLedgerRow) -> bool:
    account_type = row.account_type.lower()
    account_name = row.account_name.lower()
    return any(hint in account_type or hint in account_name for hint in _EQUITY_HINTS)


def _is_owner_or_shareholder_payee(row: GeneralLedgerRow, owner_tokens: tuple[str, ...]) -> bool:
    payee = (row.payee or "").strip().lower()
    if not payee:
        return False
    if "owner" in payee or "shareholder" in payee:
        return True
    return any(token in payee for token in owner_tokens)


def _memo_has_keyword(memo: str | None) -> bool:
    text = (memo or "").lower()
    return any(keyword in text for keyword in _MEMO_KEYWORDS)


def _looks_like_transfer_from_bank(row: GeneralLedgerRow) -> bool:
    tx_type = row.transaction_type.lower()
    account_name = row.account_name.lower()
    memo = (row.memo or "").lower()
    return "transfer" in tx_type and ("bank" in account_name or "bank" in memo)


def _is_round_number(amount: Decimal) -> bool:
    divisor = Decimal(MISCODE_ROUND_NUMBER_DIVISOR)
    if divisor <= Decimal("0"):
        return False
    return amount % divisor == Decimal("0")


def _confidence_label(score: int) -> Literal["High", "Medium", "Low"]:
    if score >= MISCODE_CONFIDENCE_HIGH:
        return "High"
    if score >= MISCODE_CONFIDENCE_MEDIUM:
        return "Medium"
    return "Low"
