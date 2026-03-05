from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from cpapacket.models.tax import EstimatedTaxPayment, TaxDeadline


def test_estimated_tax_payment_happy_path_quantizes_amounts() -> None:
    payment = EstimatedTaxPayment(
        jurisdiction=" Federal ",
        due_date=date(2025, 4, 15),
        amount="120.126",
        status="not_paid",
        paid_date=None,
        last_updated=datetime(2025, 1, 10, 9, 30),
    )

    assert payment.jurisdiction == "Federal"
    assert payment.amount == Decimal("120.13")
    assert payment.status == "not_paid"


def test_tax_deadline_construction_defaults_completed_false() -> None:
    deadline = TaxDeadline(
        jurisdiction="NY",
        name=" Q1 Estimated Tax ",
        due_date=date(2025, 4, 15),
        category="estimated_tax",
    )

    assert deadline.name == "Q1 Estimated Tax"
    assert deadline.completed is False


def test_tax_models_reject_invalid_jurisdiction() -> None:
    with pytest.raises(ValidationError):
        EstimatedTaxPayment(
            jurisdiction="CA",
            due_date=date(2025, 4, 15),
            amount="25.00",
            status="not_paid",
            paid_date=None,
            last_updated=datetime(2025, 1, 1, 12, 0),
        )

    with pytest.raises(ValidationError):
        TaxDeadline(
            jurisdiction="CA",
            name="Invalid",
            due_date=date(2025, 4, 15),
            category="estimated_tax",
        )


def test_tax_deadline_rejects_invalid_category() -> None:
    with pytest.raises(ValidationError):
        TaxDeadline(
            jurisdiction="DE",
            name="Delaware Franchise Tax",
            due_date=date(2025, 3, 1),
            category="unknown",
        )


def test_estimated_tax_payment_rejects_negative_amounts() -> None:
    with pytest.raises(ValidationError):
        EstimatedTaxPayment(
            jurisdiction="DE",
            due_date=date(2025, 4, 15),
            amount="-1.00",
            status="not_paid",
            paid_date=None,
            last_updated=datetime(2025, 1, 1, 12, 0),
        )
