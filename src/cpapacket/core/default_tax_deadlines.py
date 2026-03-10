"""Default estimated-tax and filing deadline generation."""

from __future__ import annotations

from datetime import date

from cpapacket.models.tax import TaxDeadline


def generate_default_tax_deadlines(*, year: int) -> list[TaxDeadline]:
    """Return default Federal, NY, and DE deadlines for a tax year."""
    if year < 2000 or year > 2100:
        raise ValueError("year must be in [2000, 2100]")

    deadlines = [
        TaxDeadline(
            jurisdiction="Federal",
            name="1120-S Filing",
            due_date=date(year, 3, 15),
            category="filing",
        ),
        TaxDeadline(
            jurisdiction="Federal",
            name="Individual Filing",
            due_date=date(year, 4, 15),
            category="filing",
        ),
        TaxDeadline(
            jurisdiction="Federal",
            name="Q1 Estimated Tax",
            due_date=date(year, 4, 15),
            category="estimated_tax",
        ),
        TaxDeadline(
            jurisdiction="Federal",
            name="Q2 Estimated Tax",
            due_date=date(year, 6, 16),
            category="estimated_tax",
        ),
        TaxDeadline(
            jurisdiction="Federal",
            name="Q3 Estimated Tax",
            due_date=date(year, 9, 15),
            category="estimated_tax",
        ),
        TaxDeadline(
            jurisdiction="Federal",
            name="Q4 Estimated Tax",
            due_date=date(year + 1, 1, 15),
            category="estimated_tax",
        ),
        TaxDeadline(
            jurisdiction="NY",
            name="Q1 Estimated Tax",
            due_date=date(year, 4, 15),
            category="estimated_tax",
        ),
        TaxDeadline(
            jurisdiction="NY",
            name="Q2 Estimated Tax",
            due_date=date(year, 6, 16),
            category="estimated_tax",
        ),
        TaxDeadline(
            jurisdiction="NY",
            name="Q3 Estimated Tax",
            due_date=date(year, 9, 15),
            category="estimated_tax",
        ),
        TaxDeadline(
            jurisdiction="NY",
            name="Q4 Estimated Tax",
            due_date=date(year + 1, 1, 15),
            category="estimated_tax",
        ),
        TaxDeadline(
            jurisdiction="DE",
            name="Franchise Tax",
            due_date=date(year, 3, 1),
            category="filing",
        ),
    ]

    return sorted(deadlines, key=lambda item: (item.due_date, item.jurisdiction, item.name))
