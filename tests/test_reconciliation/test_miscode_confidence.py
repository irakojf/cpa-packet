from __future__ import annotations

from datetime import date
from decimal import Decimal

from cpapacket.models.general_ledger import GeneralLedgerRow
from cpapacket.reconciliation.miscode_detector import MiscodeDetector


def _row(
    *,
    txn_id: str,
    account_type: str,
    account_name: str,
    payee: str,
    memo: str,
    amount: str,
    transaction_type: str = "Expense",
) -> GeneralLedgerRow:
    return GeneralLedgerRow(
        txn_id=txn_id,
        date=date(2025, 1, 1),
        transaction_type=transaction_type,
        document_number=f"DOC-{txn_id}",
        account_name=account_name,
        account_type=account_type,
        payee=payee,
        memo=memo,
        debit=Decimal(amount),
        credit=Decimal("0"),
    )


def test_confidence_low_at_threshold_score() -> None:
    detector = MiscodeDetector()
    rows = [
        _row(
            txn_id="LOW",
            account_type="Expense",
            account_name="Office Expense",
            payee="Vendor",
            memo="personal reimbursement",
            amount="250.00",
        )
    ]

    candidates = detector.scan(rows, owner_keywords=["alex"])

    assert len(candidates) == 1
    assert candidates[0].score == 2
    assert candidates[0].confidence == "Low"


def test_confidence_medium_at_threshold_score() -> None:
    detector = MiscodeDetector()
    rows = [
        _row(
            txn_id="MEDIUM",
            account_type="Expense",
            account_name="Meals Expense",
            payee="Alex Owner",
            memo="vendor payment",
            amount="200.00",
        )
    ]

    candidates = detector.scan(rows, owner_keywords=["alex"])

    assert len(candidates) == 1
    assert candidates[0].score == 4
    assert candidates[0].confidence == "Medium"


def test_confidence_high_at_threshold_score() -> None:
    detector = MiscodeDetector()
    rows = [
        _row(
            txn_id="HIGH",
            account_type="Expense",
            account_name="Travel Expense",
            payee="Alex Owner",
            memo="personal expense",
            amount="200.00",
        )
    ]

    candidates = detector.scan(rows, owner_keywords=["alex"])

    assert len(candidates) == 1
    assert candidates[0].score == 6
    assert candidates[0].confidence == "High"


def test_confidence_below_low_threshold_not_flagged() -> None:
    detector = MiscodeDetector()
    rows = [
        _row(
            txn_id="BELOW",
            account_type="Bank",
            account_name="Checking",
            payee="Alex Owner",
            memo="standard transfer",
            amount="200.00",
            transaction_type="JournalEntry",
        )
    ]

    candidates = detector.scan(rows, owner_keywords=["alex"])

    assert candidates == []
