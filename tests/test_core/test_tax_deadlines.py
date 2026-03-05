from __future__ import annotations

from datetime import date

from cpapacket.core.tax_deadlines import (
    PAST_DUE_LABEL,
    UPCOMING_LABEL,
    classify_deadline_status,
)


def test_classify_deadline_status_returns_none_when_completed() -> None:
    status = classify_deadline_status(
        due_date=date(2026, 4, 15),
        completed=True,
        today=date(2026, 4, 16),
    )
    assert status is None


def test_classify_deadline_status_marks_past_due_when_unpaid_and_past_due() -> None:
    status = classify_deadline_status(
        due_date=date(2026, 4, 15),
        completed=False,
        today=date(2026, 4, 16),
    )
    assert status == PAST_DUE_LABEL


def test_classify_deadline_status_marks_upcoming_within_default_window() -> None:
    status = classify_deadline_status(
        due_date=date(2026, 5, 15),
        completed=False,
        today=date(2026, 4, 15),
    )
    assert status == UPCOMING_LABEL


def test_classify_deadline_status_marks_upcoming_on_exact_window_boundary() -> None:
    status = classify_deadline_status(
        due_date=date(2026, 5, 15),
        completed=False,
        today=date(2026, 4, 15),
        upcoming_window_days=30,
    )
    assert status == UPCOMING_LABEL


def test_classify_deadline_status_returns_none_outside_upcoming_window() -> None:
    status = classify_deadline_status(
        due_date=date(2026, 6, 1),
        completed=False,
        today=date(2026, 4, 15),
    )
    assert status is None

