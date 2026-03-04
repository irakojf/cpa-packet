from __future__ import annotations

from decimal import Decimal

import pytest

from cpapacket.deliverables.general_ledger_normalizer import normalize_general_ledger_report


def test_normalize_general_ledger_report_maps_qbo_rows_to_model_fields() -> None:
    payload = {
        "Rows": {
            "Row": [
                {
                    "TxnId": "txn-1",
                    "TxnDate": "2025-01-15",
                    "TxnType": "JournalEntry",
                    "DocNum": "JE-1",
                    "AccountName": "Cash",
                    "AccountType": "Bank",
                    "Payee": "Acme LLC",
                    "Memo": "Unicode memo: café ☕",
                    "Amount": "125.50",
                },
                {
                    "TxnId": "txn-2",
                    "TxnDate": "2025-01-16",
                    "Description": "Negative amount example",
                    "DocNum": "JE-2",
                    "AccountName": "Accounts Payable",
                    "AccountType": "AccountsPayable",
                    "Payee": "   ",
                    "Memo": "",
                    "Amount": "-25.00",
                },
                {
                    "TxnId": "txn-3",
                    "TxnDate": "2025-01-17",
                    "TransactionType": "Bill",
                    "DocNum": "BL-1",
                    "AccountName": "Office Expense",
                    "AccountType": "Expense",
                    "Debit": "0.00",
                    "Credit": "10.25",
                },
            ]
        }
    }

    rows = normalize_general_ledger_report(payload)

    assert len(rows) == 3
    assert rows[0].payee == "Acme LLC"
    assert rows[0].memo == "Unicode memo: café ☕"
    assert rows[0].debit == Decimal("125.50")
    assert rows[0].credit == Decimal("0.00")

    assert rows[1].payee is None
    assert rows[1].memo is None
    assert rows[1].debit == Decimal("0.00")
    assert rows[1].credit == Decimal("25.00")

    assert rows[2].debit == Decimal("0.00")
    assert rows[2].credit == Decimal("10.25")


def test_normalize_general_ledger_report_rejects_invalid_dates() -> None:
    payload = {
        "Rows": {
            "Row": [
                {
                    "TxnId": "txn-1",
                    "TxnDate": "bad-date",
                    "DocNum": "JE-1",
                    "AccountName": "Cash",
                    "AccountType": "Bank",
                    "Amount": "1.00",
                }
            ]
        }
    }

    with pytest.raises(ValueError, match="invalid date value"):
        normalize_general_ledger_report(payload)


def test_normalize_general_ledger_report_rejects_invalid_decimal_values() -> None:
    payload = {
        "Rows": {
            "Row": [
                {
                    "TxnId": "txn-1",
                    "TxnDate": "2025-01-15",
                    "DocNum": "JE-1",
                    "AccountName": "Cash",
                    "AccountType": "Bank",
                    "Amount": "NaN",
                }
            ]
        }
    }

    with pytest.raises(ValueError, match="invalid decimal value"):
        normalize_general_ledger_report(payload)
