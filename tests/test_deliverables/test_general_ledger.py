from __future__ import annotations

from typing import Any

import pytest

from cpapacket.deliverables.general_ledger import (
    GeneralLedgerSliceError,
    fetch_general_ledger_monthly_slices,
)


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
