from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

import pytest

from cpapacket.deliverables.general_ledger import (
    GeneralLedgerMonthlySlice,
    GeneralLedgerSliceError,
    fetch_general_ledger_monthly_slices,
    merge_general_ledger_monthly_slices,
)
from cpapacket.models.general_ledger import GeneralLedgerRow


class _Provider:
    def __init__(self, *, fail_month: int | None = None) -> None:
        self.fail_month = fail_month
        self.calls: list[tuple[int, int]] = []

    def get_general_ledger(self, year: int, month: int) -> dict[str, Any]:
        self.calls.append((year, month))
        if self.fail_month == month:
            raise RuntimeError("upstream error")
        return {"month": month, "year": year}


def test_fetch_general_ledger_monthly_slices_fetches_all_months_in_order() -> None:
    provider = _Provider()
    completed = fetch_general_ledger_monthly_slices(year=2025, provider=provider)

    assert [slice_.month for slice_ in completed] == list(range(1, 13))
    assert provider.calls == [(2025, month) for month in range(1, 13)]


def test_fetch_general_ledger_monthly_slices_supports_resume_start_month() -> None:
    provider = _Provider()
    completed = fetch_general_ledger_monthly_slices(
        year=2025,
        provider=provider,
        start_month=5,
    )

    assert [slice_.month for slice_ in completed] == list(range(5, 13))
    assert provider.calls == [(2025, month) for month in range(5, 13)]


def test_fetch_general_ledger_monthly_slices_raises_with_completed_context() -> None:
    provider = _Provider(fail_month=4)

    with pytest.raises(GeneralLedgerSliceError) as exc_info:
        fetch_general_ledger_monthly_slices(year=2025, provider=provider)

    error = exc_info.value
    assert error.failed_month == 4
    assert [slice_.month for slice_ in error.completed_slices] == [1, 2, 3]
    assert isinstance(error.cause, RuntimeError)


def test_fetch_general_ledger_monthly_slices_invokes_progress_callback() -> None:
    provider = _Provider()
    progress: list[int] = []

    fetch_general_ledger_monthly_slices(
        year=2025,
        provider=provider,
        start_month=11,
        end_month=12,
        progress_callback=progress.append,
    )

    assert progress == [11, 12]


def test_fetch_general_ledger_monthly_slices_rejects_invalid_ranges() -> None:
    invalid_ranges = [(0, 12), (1, 13), (8, 3)]
    for start_month, end_month in invalid_ranges:
        with pytest.raises(ValueError):
            fetch_general_ledger_monthly_slices(
                year=2025,
                provider=_Provider(),
                start_month=start_month,
                end_month=end_month,
            )


def _row(
    *,
    txn_id: str,
    doc_num: str,
    account_name: str = "Cash",
    payee: str | None = None,
    memo: str | None = None,
) -> GeneralLedgerRow:
    return GeneralLedgerRow(
        txn_id=txn_id,
        date=date(2025, 1, 15),
        transaction_type="JournalEntry",
        document_number=doc_num,
        account_name=account_name,
        account_type="Bank",
        payee=payee,
        memo=memo,
        debit=Decimal("100.00"),
        credit=Decimal("0.00"),
    )


def _row_with_missing_txn_id(*, doc_num: str, memo: str) -> GeneralLedgerRow:
    # Use model_construct to simulate upstream rows where txn_id is missing.
    return GeneralLedgerRow.model_construct(
        txn_id="",
        date=date(2025, 1, 15),
        transaction_type="JournalEntry",
        document_number=doc_num,
        account_name="Cash",
        account_type="Bank",
        payee=None,
        memo=memo,
        debit=Decimal("100.00"),
        credit=Decimal("0.00"),
    )


def test_merge_general_ledger_monthly_slices_dedupes_by_txn_id() -> None:
    jan = GeneralLedgerMonthlySlice(month=1, payload={"month": 1})
    feb = GeneralLedgerMonthlySlice(month=2, payload={"month": 2})

    normalized_by_month = {
        1: [_row(txn_id="txn-1", doc_num="JAN-1"), _row(txn_id="txn-2", doc_num="JAN-2")],
        2: [_row(txn_id="txn-2", doc_num="FEB-DUP"), _row(txn_id="txn-3", doc_num="FEB-3")],
    }

    def _normalizer(payload: dict[str, Any]) -> list[GeneralLedgerRow]:
        return normalized_by_month[payload["month"]]

    merged = merge_general_ledger_monthly_slices((feb, jan), normalizer=_normalizer)

    assert [row.txn_id for row in merged] == ["txn-1", "txn-2", "txn-3"]
    assert [row.document_number for row in merged] == ["JAN-1", "JAN-2", "FEB-3"]


def test_merge_general_ledger_monthly_slices_uses_composite_hash_when_txn_id_blank() -> None:
    jan = GeneralLedgerMonthlySlice(month=1, payload={"month": 1})
    feb = GeneralLedgerMonthlySlice(month=2, payload={"month": 2})

    normalized_by_month = {
        1: [_row_with_missing_txn_id(doc_num="DOC-1", memo="memo")],
        2: [_row_with_missing_txn_id(doc_num="DOC-1", memo="memo")],
    }

    def _normalizer(payload: dict[str, Any]) -> list[GeneralLedgerRow]:
        return normalized_by_month[payload["month"]]

    merged = merge_general_ledger_monthly_slices((jan, feb), normalizer=_normalizer)
    assert len(merged) == 1
    assert merged[0].document_number == "DOC-1"


def test_merge_general_ledger_monthly_slices_keeps_distinct_blank_txn_rows() -> None:
    jan = GeneralLedgerMonthlySlice(month=1, payload={"month": 1})
    feb = GeneralLedgerMonthlySlice(month=2, payload={"month": 2})

    normalized_by_month = {
        1: [_row_with_missing_txn_id(doc_num="DOC-1", memo="memo-a")],
        2: [_row_with_missing_txn_id(doc_num="DOC-2", memo="memo-b")],
    }

    def _normalizer(payload: dict[str, Any]) -> list[GeneralLedgerRow]:
        return normalized_by_month[payload["month"]]

    merged = merge_general_ledger_monthly_slices((jan, feb), normalizer=_normalizer)

    assert [row.document_number for row in merged] == ["DOC-1", "DOC-2"]
