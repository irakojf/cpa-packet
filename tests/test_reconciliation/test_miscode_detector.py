from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

import cpapacket.reconciliation.miscode_detector as detector_module
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


def test_miscode_detector_rule_r1_owner_payee_expense() -> None:
    detector = MiscodeDetector()
    rows = [
        _row(
            txn_id="R1",
            account_type="Expense",
            account_name="Office Supplies",
            payee="Alex Owner",
            memo="vendor invoice",
            amount="123.45",
            transaction_type="Expense",
        )
    ]

    candidates = detector.scan(rows, owner_keywords=["alex"])

    assert len(candidates) == 1
    assert candidates[0].reason_codes == ["R1_OWNER_PAYEE_EXPENSE"]


def test_miscode_detector_rule_r2_memo_keyword_expense() -> None:
    detector = MiscodeDetector()
    rows = [
        _row(
            txn_id="R2",
            account_type="Expense",
            account_name="Office Expense",
            payee="Unrelated Vendor",
            memo="personal reimbursement",
            amount="123.45",
            transaction_type="Expense",
        )
    ]

    candidates = detector.scan(rows, owner_keywords=["alex"])

    assert len(candidates) == 1
    assert candidates[0].reason_codes == ["R2_MEMO_KEYWORD_EXPENSE"]


def test_miscode_detector_rule_r3_transfer_non_equity_high() -> None:
    detector = MiscodeDetector()
    rows = [
        _row(
            txn_id="R3",
            account_type="Other Current Liability",
            account_name="Operating Bank Clearing",
            payee="Unrelated Vendor",
            memo="wire settlement",
            amount="1500.00",
            transaction_type="Transfer",
        )
    ]

    candidates = detector.scan(rows, owner_keywords=["alex"])

    assert len(candidates) == 1
    assert "R3_TRANSFER_NON_EQUITY_HIGH" in candidates[0].reason_codes


def test_miscode_detector_rule_r4_round_number_owner(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("cpapacket.reconciliation.miscode_detector.MISCODE_CONFIDENCE_LOW", 1)

    detector = MiscodeDetector()
    rows = [
        _row(
            txn_id="R4",
            account_type="Other Current Liability",
            account_name="Clearing Account",
            payee="Alex Owner",
            memo="standard transfer",
            amount="200.00",
            transaction_type="JournalEntry",
        )
    ]

    candidates = detector.scan(rows, owner_keywords=["alex"])

    assert len(candidates) == 1
    assert candidates[0].reason_codes == ["R4_ROUND_NUMBER_OWNER"]


def test_miscode_detector_rule_r5_high_amount_companion_signal() -> None:
    detector = MiscodeDetector()
    rows = [
        _row(
            txn_id="R5",
            account_type="Expense",
            account_name="Office Expense",
            payee="Unrelated Vendor",
            memo="personal purchase",
            amount="1001.00",
            transaction_type="Expense",
        )
    ]

    candidates = detector.scan(rows, owner_keywords=["alex"])

    assert len(candidates) == 1
    assert "R5_HIGH_AMOUNT" in candidates[0].reason_codes


def test_is_equity_account_detects_distribution_keyword() -> None:
    row = _row(
        txn_id="EQ-DIST",
        account_type="Other Current Asset",
        account_name="Owner Distribution",
        payee="Vendor",
        memo="memo",
        amount="10.00",
    )

    assert detector_module._is_equity_account(row)


def test_is_equity_account_detects_draw_keyword() -> None:
    row = _row(
        txn_id="EQ-DRAW",
        account_type="Other Current Asset",
        account_name="Owner Draw",
        payee="Vendor",
        memo="memo",
        amount="10.00",
    )

    assert detector_module._is_equity_account(row)


def test_is_equity_account_detects_shareholder_keyword() -> None:
    row = _row(
        txn_id="EQ-SHAREHOLDER",
        account_type="Liability",
        account_name="Shareholder Contribution",
        payee="Vendor",
        memo="memo",
        amount="10.00",
    )

    assert detector_module._is_equity_account(row)


def test_is_equity_account_excludes_non_equity_accounts() -> None:
    row = _row(
        txn_id="NOT-EQ",
        account_type="Bank",
        account_name="Operating Checking",
        payee="Vendor",
        memo="memo",
        amount="10.00",
    )

    assert detector_module._is_equity_account(row) is False


def test_is_equity_account_is_case_insensitive() -> None:
    row = _row(
        txn_id="EQ-CASE",
        account_type="other current asset",
        account_name="SHAREHOLDER DRAW",
        payee="Vendor",
        memo="memo",
        amount="10.00",
    )

    assert detector_module._is_equity_account(row)


def test_miscode_detector_combined_rules_accumulate_max_score() -> None:
    detector = MiscodeDetector()
    rows = [
        _row(
            txn_id="COMBINED-MAX",
            account_type="Expense",
            account_name="Business Bank Fees",
            payee="Alex Owner",
            memo="distribution transfer",
            amount="1200.00",
            transaction_type="Transfer",
        )
    ]

    candidates = detector.scan(rows, owner_keywords=["alex"])

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.score == 9
    assert candidate.confidence == "High"
    assert set(candidate.reason_codes) == {
        "R1_OWNER_PAYEE_EXPENSE",
        "R2_MEMO_KEYWORD_EXPENSE",
        "R3_TRANSFER_NON_EQUITY_HIGH",
        "R4_ROUND_NUMBER_OWNER",
        "R5_HIGH_AMOUNT",
    }


def test_miscode_detector_minimum_threshold_is_inclusive() -> None:
    detector = MiscodeDetector()
    rows = [
        _row(
            txn_id="THRESHOLD-LOW",
            account_type="Expense",
            account_name="Office Expense",
            payee="Vendor",
            memo="distribution",
            amount="55.10",
            transaction_type="Expense",
        )
    ]

    candidates = detector.scan(rows, owner_keywords=["alex"])

    assert len(candidates) == 1
    assert candidates[0].score == 2
    assert candidates[0].confidence == "Low"
    assert candidates[0].reason_codes == ["R2_MEMO_KEYWORD_EXPENSE"]


def test_miscode_detector_below_threshold_is_not_flagged() -> None:
    detector = MiscodeDetector()
    rows = [
        _row(
            txn_id="THRESHOLD-BELOW",
            account_type="Asset",
            account_name="Operating Clearing",
            payee="Alex Owner",
            memo="standard journal",
            amount="200.00",
            transaction_type="JournalEntry",
        )
    ]

    candidates = detector.scan(rows, owner_keywords=["alex"])

    assert candidates == []
