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
    transaction_type: str = "Transfer",
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


def test_miscode_detector_applies_rules_and_confidence() -> None:
    detector = MiscodeDetector()
    rows = [
        _row(
            txn_id="A",
            account_type="Expense",
            account_name="Business Bank Fees",
            payee="Alex Owner",
            memo="Owner draw transfer",
            amount="1200",
        ),
        _row(
            txn_id="B",
            account_type="Expense",
            account_name="Travel Expense",
            payee="Vendor",
            memo="normal",
            amount="30",
            transaction_type="Expense",
        ),
    ]

    candidates = detector.scan(rows, owner_keywords=["alex"])
    assert len(candidates) == 1

    flagged = candidates[0]
    assert flagged.txn_id == "A"
    assert flagged.score >= 6
    assert flagged.confidence == "High"
    assert "R1_OWNER_PAYEE_EXPENSE" in flagged.reason_codes
    assert "R2_MEMO_KEYWORD_EXPENSE" in flagged.reason_codes
    assert "R3_TRANSFER_NON_EQUITY_HIGH" in flagged.reason_codes
    assert "R4_ROUND_NUMBER_OWNER" in flagged.reason_codes
    assert "R5_HIGH_AMOUNT" in flagged.reason_codes


def test_miscode_detector_ignores_low_score_rows() -> None:
    detector = MiscodeDetector()
    rows = [
        _row(
            txn_id="C",
            account_type="Expense",
            account_name="Meals Expense",
            payee="Unrelated Vendor",
            memo="team lunch",
            amount="45",
            transaction_type="Expense",
        )
    ]

    candidates = detector.scan(rows, owner_keywords=["alex"])
    assert candidates == []
