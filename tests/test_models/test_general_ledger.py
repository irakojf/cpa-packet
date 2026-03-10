from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from pydantic import ValidationError

from cpapacket.models.general_ledger import GeneralLedgerRow


def test_general_ledger_row_happy_path_and_signed_amount() -> None:
    row = GeneralLedgerRow(
        txn_id=" txn-001 ",
        date=date(2025, 1, 15),
        transaction_type=" Journal Entry ",
        document_number=" JE-99 ",
        account_name=" Cash ",
        account_type=" Asset ",
        payee=" Vendor X ",
        memo=" Monthly adjustment ",
        debit="100.127",
        credit="25.126",
    )

    assert row.txn_id == "txn-001"
    assert row.transaction_type == "Journal Entry"
    assert row.document_number == "JE-99"
    assert row.account_name == "Cash"
    assert row.account_type == "Asset"
    assert row.payee == "Vendor X"
    assert row.memo == "Monthly adjustment"
    assert row.debit == Decimal("100.13")
    assert row.credit == Decimal("25.13")
    assert row.signed_amount == Decimal("75.00")


def test_general_ledger_row_rejects_blank_required_text_fields() -> None:
    with pytest.raises(ValidationError):
        GeneralLedgerRow(
            txn_id=" ",
            date=date(2025, 1, 15),
            transaction_type="Journal Entry",
            document_number="JE-1",
            account_name="Cash",
            account_type="Asset",
            debit="1.00",
            credit="0.00",
        )


def test_general_ledger_row_rejects_negative_or_non_finite_money() -> None:
    with pytest.raises(ValidationError):
        GeneralLedgerRow(
            txn_id="txn",
            date=date(2025, 1, 15),
            transaction_type="Journal Entry",
            document_number="JE-1",
            account_name="Cash",
            account_type="Asset",
            debit="-1.00",
            credit="0.00",
        )

    with pytest.raises(ValidationError):
        GeneralLedgerRow(
            txn_id="txn",
            date=date(2025, 1, 15),
            transaction_type="Journal Entry",
            document_number="JE-1",
            account_name="Cash",
            account_type="Asset",
            debit="NaN",
            credit="0.00",
        )


def test_general_ledger_row_optional_text_is_normalized() -> None:
    row = GeneralLedgerRow(
        txn_id="txn",
        date=date(2025, 1, 15),
        transaction_type="Journal Entry",
        document_number="JE-1",
        account_name="Cash",
        account_type="Asset",
        payee="   ",
        memo="   ",
        debit="5.00",
        credit="2.00",
    )

    assert row.payee is None
    assert row.memo is None


def test_general_ledger_row_optional_text_defaults_to_none_when_omitted() -> None:
    row = GeneralLedgerRow(
        txn_id="txn",
        date=date(2025, 1, 15),
        transaction_type="Journal Entry",
        document_number="JE-1",
        account_name="Cash",
        account_type="Asset",
        debit="5.00",
        credit="2.00",
    )

    assert row.payee is None
    assert row.memo is None


def test_general_ledger_row_is_frozen() -> None:
    row = GeneralLedgerRow(
        txn_id="txn",
        date=date(2025, 1, 15),
        transaction_type="Journal Entry",
        document_number="JE-1",
        account_name="Cash",
        account_type="Asset",
        debit="1.00",
        credit="0.00",
    )

    with pytest.raises(ValidationError):
        row.debit = Decimal("2.00")
