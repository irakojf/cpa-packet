from __future__ import annotations

import csv
import threading
import time
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

import cpapacket.deliverables.general_ledger as general_ledger_module
from cpapacket.core.limiter import LimiterConfig, ServiceLimiter
from cpapacket.core.retry import RetryPolicy
from cpapacket.deliverables.general_ledger import (
    GeneralLedgerMonthlySlice,
    GeneralLedgerSliceError,
    _iter_csv_rows,
    fetch_general_ledger_monthly_slices,
    merge_general_ledger_monthly_slices,
)
from cpapacket.models.general_ledger import GeneralLedgerRow
from cpapacket.writers.csv_writer import CsvWriter


class _Provider:
    def __init__(self, *, fail_month: int | None = None) -> None:
        self.fail_month = fail_month
        self.calls: list[tuple[int, int]] = []

    def get_general_ledger(self, year: int, month: int) -> dict[str, Any]:
        self.calls.append((year, month))
        if self.fail_month == month:
            raise RuntimeError("upstream error")
        return {"month": month, "year": year}

    def get_general_ledger_with_source(
        self,
        year: int,
        month: int,
    ) -> tuple[dict[str, Any], str]:
        return self.get_general_ledger(year, month), "api"


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


def test_fetch_general_ledger_monthly_slices_retries_timeout_and_succeeds() -> None:
    class _TimeoutOnceProvider:
        def __init__(self) -> None:
            self.calls: list[tuple[int, int]] = []
            self._attempts = 0

        def get_general_ledger(self, year: int, month: int) -> dict[str, Any]:
            self.calls.append((year, month))
            self._attempts += 1
            if self._attempts == 1:
                raise TimeoutError("timed out")
            return {"month": month, "year": year}

        def get_general_ledger_with_source(
            self,
            year: int,
            month: int,
        ) -> tuple[dict[str, Any], str]:
            return self.get_general_ledger(year, month), "api"

    provider = _TimeoutOnceProvider()
    completed = fetch_general_ledger_monthly_slices(
        year=2025,
        provider=provider,
        start_month=1,
        end_month=1,
    )

    assert [slice_.month for slice_ in completed] == [1]
    assert provider.calls == [(2025, 1), (2025, 1)]


def test_fetch_general_ledger_monthly_slices_raises_after_timeout_retry_budget() -> None:
    class _AlwaysTimeoutProvider:
        def __init__(self) -> None:
            self.calls: list[tuple[int, int]] = []

        def get_general_ledger(self, year: int, month: int) -> dict[str, Any]:
            self.calls.append((year, month))
            raise TimeoutError("timed out")

        def get_general_ledger_with_source(
            self,
            year: int,
            month: int,
        ) -> tuple[dict[str, Any], str]:
            return self.get_general_ledger(year, month), "api"

    provider = _AlwaysTimeoutProvider()
    max_attempts = RetryPolicy().max_5xx + 1

    with pytest.raises(GeneralLedgerSliceError) as exc_info:
        fetch_general_ledger_monthly_slices(
            year=2025,
            provider=provider,
            start_month=1,
            end_month=1,
        )

    error = exc_info.value
    assert error.failed_month == 1
    assert error.completed_slices == ()
    assert isinstance(error.cause, TimeoutError)
    assert provider.calls == [(2025, 1)] * max_attempts


def test_fetch_general_ledger_monthly_slices_respects_qbo_concurrency_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _SlowProvider:
        def __init__(self) -> None:
            self.calls: list[tuple[int, int]] = []
            self._active_calls = 0
            self.max_active_calls = 0
            self._lock = threading.Lock()

        def get_general_ledger(self, year: int, month: int) -> dict[str, Any]:
            with self._lock:
                self._active_calls += 1
                self.max_active_calls = max(self.max_active_calls, self._active_calls)
            try:
                time.sleep(0.01)
                self.calls.append((year, month))
                return {"month": month, "year": year}
            finally:
                with self._lock:
                    self._active_calls -= 1

        def get_general_ledger_with_source(
            self,
            year: int,
            month: int,
        ) -> tuple[dict[str, Any], str]:
            return self.get_general_ledger(year, month), "api"

    monkeypatch.setattr(
        general_ledger_module,
        "_SERVICE_LIMITER",
        ServiceLimiter(config=LimiterConfig(qbo_max=2, gusto_max=1)),
    )

    provider = _SlowProvider()
    completed = fetch_general_ledger_monthly_slices(
        year=2025,
        provider=provider,
        start_month=1,
        end_month=6,
    )

    assert [slice_.month for slice_ in completed] == [1, 2, 3, 4, 5, 6]
    assert sorted(provider.calls) == [(2025, month) for month in range(1, 7)]
    assert provider.max_active_calls == 2


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


def _row_with_missing_txn_id(
    *,
    doc_num: str,
    memo: str,
    payee: str | None = None,
) -> GeneralLedgerRow:
    # Use model_construct to simulate upstream rows where txn_id is missing.
    return GeneralLedgerRow.model_construct(
        txn_id="",
        date=date(2025, 1, 15),
        transaction_type="JournalEntry",
        document_number=doc_num,
        account_name="Cash",
        account_type="Bank",
        payee=payee,
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


def test_merge_general_ledger_monthly_slices_dedupes_blank_txn_rows_with_whitespace_variations(
) -> None:
    jan = GeneralLedgerMonthlySlice(month=1, payload={"month": 1})
    feb = GeneralLedgerMonthlySlice(month=2, payload={"month": 2})

    normalized_by_month = {
        1: [
            _row_with_missing_txn_id(
                doc_num="DOC-1",
                memo="  note ",
                payee="  Acme  ",
            )
        ],
        2: [
            _row_with_missing_txn_id(
                doc_num="DOC-1",
                memo="note",
                payee="Acme",
            )
        ],
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


def test_merge_general_ledger_monthly_slices_orders_chronologically() -> None:
    slices = tuple(
        GeneralLedgerMonthlySlice(month=month, payload={"month": month})
        for month in range(12, 0, -1)
    )

    def _normalizer(payload: dict[str, Any]) -> list[GeneralLedgerRow]:
        month = payload["month"]
        return [_row(txn_id=f"txn-{month}", doc_num=f"M{month:02d}")]

    merged = merge_general_ledger_monthly_slices(slices, normalizer=_normalizer)

    assert [row.document_number for row in merged] == [f"M{month:02d}" for month in range(1, 13)]


def test_merge_general_ledger_monthly_slices_skips_empty_months() -> None:
    slices = (
        GeneralLedgerMonthlySlice(month=1, payload={"month": 1}),
        GeneralLedgerMonthlySlice(month=2, payload={"month": 2}),
        GeneralLedgerMonthlySlice(month=3, payload={"month": 3}),
    )

    def _normalizer(payload: dict[str, Any]) -> list[GeneralLedgerRow]:
        month = payload["month"]
        if month == 2:
            return []
        return [_row(txn_id=f"txn-{month}", doc_num=f"M{month}")]

    merged = merge_general_ledger_monthly_slices(slices, normalizer=_normalizer)

    assert [row.document_number for row in merged] == ["M1", "M3"]


def test_iter_csv_rows_preserves_unicode_and_empty_optional_fields() -> None:
    rows = (
        _row(
            txn_id="txn-1",
            doc_num="DOC-1",
            payee="Café ☕",
            memo=None,
        ),
    )

    serialized = list(_iter_csv_rows(rows))
    assert serialized[0]["payee"] == "Café ☕"
    assert serialized[0]["memo"] == ""


def test_general_ledger_streaming_csv_matches_batch(tmp_path: Path) -> None:
    rows = (
        _row(txn_id="txn-1", doc_num="DOC-1"),
        _row(txn_id="txn-2", doc_num="DOC-2"),
        _row(txn_id="txn-3", doc_num="DOC-3"),
    )
    fieldnames = [
        "txn_id",
        "date",
        "transaction_type",
        "document_number",
        "account_name",
        "account_type",
        "payee",
        "memo",
        "debit",
        "credit",
        "signed_amount",
    ]
    writer = CsvWriter()
    batch_path = tmp_path / "batch.csv"
    streaming_path = tmp_path / "streaming.csv"

    writer.write_rows(
        batch_path,
        fieldnames=fieldnames,
        rows=_iter_csv_rows(rows),
    )
    writer.write_rows_streaming(
        streaming_path,
        fieldnames=fieldnames,
        rows=_iter_csv_rows(rows),
        dedupe_id_field="txn_id",
    )

    assert batch_path.read_text(encoding="utf-8") == streaming_path.read_text(encoding="utf-8")


def test_general_ledger_streaming_csv_matches_golden_snapshot(tmp_path: Path) -> None:
    fixture_path = Path("tests/fixtures/qbo/general_ledger_2025_golden.csv")
    with fixture_path.open(newline="", encoding="utf-8") as handle:
        fixture_rows = list(csv.DictReader(handle))

    rows: list[GeneralLedgerRow] = []
    for item in fixture_rows:
        rows.append(
            GeneralLedgerRow(
                txn_id=item["txn_id"],
                date=date.fromisoformat(item["date"]),
                transaction_type=item["transaction_type"],
                document_number=item["document_number"],
                account_name=item["account_name"],
                account_type=item["account_type"],
                payee=item["payee"] or None,
                memo=item["memo"] or None,
                debit=Decimal(item["debit"]),
                credit=Decimal(item["credit"]),
            )
        )

    # Include a duplicate row in source data and verify dedupe keeps golden output stable.
    duplicated_rows = tuple(rows + [rows[0]])

    output_path = tmp_path / "General_Ledger_2025.csv"
    CsvWriter().write_rows_streaming(
        output_path,
        fieldnames=[
            "txn_id",
            "date",
            "transaction_type",
            "document_number",
            "account_name",
            "account_type",
            "payee",
            "memo",
            "debit",
            "credit",
            "signed_amount",
        ],
        rows=_iter_csv_rows(duplicated_rows),
        dedupe_id_field="txn_id",
    )

    assert output_path.name == "General_Ledger_2025.csv"
    assert output_path.read_text(encoding="utf-8") == fixture_path.read_text(encoding="utf-8")
