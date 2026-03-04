"""Date helper utilities shared across CLI and deliverables."""

from __future__ import annotations

from calendar import monthrange
from datetime import date


def last_day_of_month(year: int, month: int) -> date:
    """Return the final day for a given year/month."""
    _validate_year(year)
    if month < 1 or month > 12:
        raise ValueError("month must be in 1..12")
    return date(year, month, monthrange(year, month)[1])


def fiscal_year_start(year: int) -> date:
    """Return the inclusive fiscal year start date (January 1)."""
    _validate_year(year)
    return date(year, 1, 1)


def fiscal_year_end(year: int) -> date:
    """Return the inclusive fiscal year end date (December 31)."""
    _validate_year(year)
    return date(year, 12, 31)


def iso_date(value: date) -> str:
    """Return an ISO-8601 formatted date string."""
    return value.isoformat()


def _validate_year(year: int) -> None:
    if year < 1:
        raise ValueError("year must be >= 1")

