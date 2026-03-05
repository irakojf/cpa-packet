"""Deadline-awareness helpers for estimated tax tracking."""

from __future__ import annotations

from datetime import date

PAST_DUE_LABEL = "PAST DUE"
UPCOMING_LABEL = "UPCOMING"


def classify_deadline_status(
    *,
    due_date: date,
    completed: bool,
    today: date,
    upcoming_window_days: int = 30,
) -> str | None:
    """Return an informational deadline status label, if any."""
    if completed:
        return None

    if today > due_date:
        return PAST_DUE_LABEL

    days_until_due = (due_date - today).days
    if days_until_due <= upcoming_window_days:
        return UPCOMING_LABEL

    return None
