from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from pydantic import ValidationError

from cpapacket.models.distributions import MiscodedDistributionCandidate


def test_miscoded_distribution_candidate_happy_path() -> None:
    candidate = MiscodedDistributionCandidate(
        txn_id="TXN-1",
        date=date(2025, 1, 15),
        transaction_type="Expense",
        payee="Owner Person",
        memo="draw",
        account="Office Expense",
        amount="1250.125",
        reason_codes=[" R1_OWNER_PAYEE_EXPENSE "],
        confidence="Medium",
        score=4,
    )

    assert candidate.amount == Decimal("1250.13")
    assert candidate.reason_codes == ["R1_OWNER_PAYEE_EXPENSE"]


def test_miscoded_distribution_candidate_validation_guards() -> None:
    with pytest.raises(ValidationError):
        MiscodedDistributionCandidate(
            txn_id="   ",
            date=date(2025, 1, 1),
            transaction_type="Expense",
            account="Expense",
            amount="10",
            reason_codes=["R1"],
            confidence="Low",
            score=1,
        )

    with pytest.raises(ValidationError):
        MiscodedDistributionCandidate(
            txn_id="TXN-2",
            date=date(2025, 1, 1),
            transaction_type="Expense",
            account="Expense",
            amount="-1",
            reason_codes=["R1"],
            confidence="Low",
            score=1,
        )

    with pytest.raises(ValidationError):
        MiscodedDistributionCandidate(
            txn_id="TXN-3",
            date=date(2025, 1, 1),
            transaction_type="Expense",
            account="Expense",
            amount="1",
            reason_codes=["   "],
            confidence="Low",
            score=1,
        )

    with pytest.raises(ValidationError):
        MiscodedDistributionCandidate(
            txn_id="TXN-4",
            date=date(2025, 1, 1),
            transaction_type="Expense",
            account="Expense",
            amount="1",
            reason_codes=["R1"],
            confidence="Low",
            score=-1,
        )


def test_miscoded_distribution_candidate_accepts_all_confidence_levels() -> None:
    for confidence in ("Low", "Medium", "High"):
        candidate = MiscodedDistributionCandidate(
            txn_id=f"TXN-{confidence}",
            date=date(2025, 1, 1),
            transaction_type="Expense",
            account="Expense",
            amount="10",
            reason_codes=[" R1 ", "R2"],
            confidence=confidence,
            score=1,
        )
        assert candidate.confidence == confidence
        assert candidate.reason_codes == ["R1", "R2"]


def test_miscoded_distribution_candidate_is_frozen() -> None:
    candidate = MiscodedDistributionCandidate(
        txn_id="TXN-FROZEN",
        date=date(2025, 1, 1),
        transaction_type="Expense",
        account="Expense",
        amount="10",
        reason_codes=["R1"],
        confidence="Low",
        score=1,
    )

    with pytest.raises(ValidationError):
        candidate.score = 2  # type: ignore[misc]
