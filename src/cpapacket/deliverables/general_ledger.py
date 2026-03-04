"""General ledger deliverable orchestration helpers."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from hashlib import sha256
from typing import Any, Protocol

from cpapacket.deliverables.general_ledger_normalizer import normalize_general_ledger_report
from cpapacket.models.general_ledger import GeneralLedgerRow


class GeneralLedgerMonthProvider(Protocol):
    """Provider interface for fetching one month of general ledger data."""

    def get_general_ledger(self, year: int, month: int) -> dict[str, Any]:
        """Return QBO GeneralLedger payload for a specific month."""


@dataclass(frozen=True)
class GeneralLedgerMonthlySlice:
    """One fetched monthly ledger payload."""

    month: int
    payload: dict[str, Any]


class GeneralLedgerSliceError(RuntimeError):
    """Raised when monthly slicing fails for a specific month."""

    def __init__(
        self,
        *,
        year: int,
        failed_month: int,
        completed_slices: tuple[GeneralLedgerMonthlySlice, ...],
        cause: Exception,
    ) -> None:
        super().__init__(
            f"general ledger monthly slicing failed for {year}-{failed_month:02d}: {cause}"
        )
        self.year = year
        self.failed_month = failed_month
        self.completed_slices = completed_slices
        self.cause = cause


def merge_general_ledger_monthly_slices(
    slices: tuple[GeneralLedgerMonthlySlice, ...],
    *,
    normalizer: Callable[
        [dict[str, Any]], list[GeneralLedgerRow]
    ] = normalize_general_ledger_report,
) -> tuple[GeneralLedgerRow, ...]:
    """Merge monthly slices in month order and deduplicate rows.

    Deduplication key preference:
    1. ``txn_id`` when present.
    2. Composite hash of stable transaction fields when ``txn_id`` is blank.

    The first occurrence wins, so if the same transaction appears in adjacent
    months (date-window overlap), the earliest month slice is preserved.
    """
    merged: list[GeneralLedgerRow] = []
    seen_keys: set[str] = set()

    for slice_ in sorted(slices, key=lambda item: item.month):
        for row in normalizer(slice_.payload):
            key = _dedupe_key_for_row(row)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            merged.append(row)

    return tuple(merged)


def fetch_general_ledger_monthly_slices(
    *,
    year: int,
    provider: GeneralLedgerMonthProvider,
    start_month: int = 1,
    end_month: int = 12,
    progress_callback: Callable[[int], None] | None = None,
) -> tuple[GeneralLedgerMonthlySlice, ...]:
    """Fetch monthly general-ledger slices in order with resumable ranges.

    Pass ``start_month`` to resume after a prior partial failure (for example,
    retrying from the first failed month onward).
    """
    if start_month < 1 or start_month > 12:
        raise ValueError("start_month must be between 1 and 12")
    if end_month < 1 or end_month > 12:
        raise ValueError("end_month must be between 1 and 12")
    if start_month > end_month:
        raise ValueError("start_month must be <= end_month")

    completed: list[GeneralLedgerMonthlySlice] = []
    for month in range(start_month, end_month + 1):
        try:
            payload = provider.get_general_ledger(year, month)
        except Exception as exc:  # pragma: no cover - exercised in tests via raised error
            raise GeneralLedgerSliceError(
                year=year,
                failed_month=month,
                completed_slices=tuple(completed),
                cause=exc,
            ) from exc

        completed.append(GeneralLedgerMonthlySlice(month=month, payload=payload))
        if progress_callback is not None:
            progress_callback(month)

    return tuple(completed)


def _dedupe_key_for_row(row: GeneralLedgerRow) -> str:
    txn_id = row.txn_id.strip()
    if txn_id:
        return f"txn:{txn_id}"

    signature = "|".join(
        (
            row.date.isoformat(),
            row.transaction_type.strip(),
            row.document_number.strip(),
            row.account_name.strip(),
            format(row.debit, "f"),
            format(row.credit, "f"),
            (row.payee or "").strip(),
            (row.memo or "").strip(),
        )
    )
    return f"composite:{sha256(signature.encode('utf-8')).hexdigest()}"
