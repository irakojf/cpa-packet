from __future__ import annotations

from datetime import date

from cpapacket.core.tax_deadlines import (
    PAST_DUE_LABEL,
    UPCOMING_LABEL,
    classify_deadline_status,
)
from cpapacket.packet.tax_tracker import generate_default_deadlines


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


def test_generate_default_deadlines_includes_expected_federal_dates() -> None:
    deadlines = generate_default_deadlines(year=2026)
    federal = [item for item in deadlines if item.jurisdiction == "Federal"]
    federal_due_dates = {item.due_date for item in federal}

    assert date(2026, 3, 15) in federal_due_dates
    assert date(2026, 4, 15) in federal_due_dates
    assert date(2026, 6, 16) in federal_due_dates
    assert date(2026, 9, 15) in federal_due_dates
    assert date(2027, 1, 15) in federal_due_dates


def test_generate_default_deadlines_includes_ny_quarterlies_and_de_franchise() -> None:
    deadlines = generate_default_deadlines(year=2026)
    ny_estimated = [
        item
        for item in deadlines
        if item.jurisdiction == "NY" and item.category == "estimated_tax"
    ]
    de_filing = [
        item
        for item in deadlines
        if item.jurisdiction == "DE" and item.name == "Franchise Tax"
    ]

    assert {item.due_date for item in ny_estimated} == {
        date(2026, 4, 15),
        date(2026, 6, 16),
        date(2026, 9, 15),
        date(2027, 1, 15),
    }
    assert len(de_filing) == 1
    assert de_filing[0].due_date == date(2026, 3, 1)
