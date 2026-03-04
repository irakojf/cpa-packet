from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from cpapacket.models.tax_tracker import EstimatedTaxPayment, TaxDeadline


def test_estimated_tax_payment_valid_record_quantizes_amount() -> None:
    payment = EstimatedTaxPayment(
        jurisdiction="Federal",
        due_date=date(2025, 4, 15),
        amount="1234.567",
        status="not_paid",
    )

    assert payment.jurisdiction == "Federal"
    assert payment.due_date == date(2025, 4, 15)
    assert payment.amount == Decimal("1234.57")
    assert payment.status == "not_paid"
    assert payment.paid_date is None
    assert isinstance(payment.last_updated, datetime)


def test_estimated_tax_payment_paid_requires_paid_date() -> None:
    with pytest.raises(ValidationError, match="paid_date is required"):
        EstimatedTaxPayment(
            jurisdiction="NY",
            due_date=date(2025, 6, 15),
            amount="100.00",
            status="paid",
            paid_date=None,
        )


def test_estimated_tax_payment_not_paid_rejects_paid_date() -> None:
    with pytest.raises(ValidationError, match="paid_date must be null"):
        EstimatedTaxPayment(
            jurisdiction="DE",
            due_date=date(2025, 9, 15),
            amount="100.00",
            status="not_paid",
            paid_date=date(2025, 9, 1),
        )


def test_estimated_tax_payment_rejects_invalid_jurisdiction() -> None:
    with pytest.raises(ValidationError):
        EstimatedTaxPayment(
            jurisdiction="CA",  # type: ignore[arg-type]
            due_date=date(2025, 4, 15),
            amount="50.00",
            status="not_paid",
        )


def test_estimated_tax_payment_rejects_negative_or_nonfinite_amount() -> None:
    with pytest.raises(ValidationError, match="amount must be >= 0"):
        EstimatedTaxPayment(
            jurisdiction="Federal",
            due_date=date(2025, 4, 15),
            amount="-1.00",
            status="not_paid",
        )

    with pytest.raises(ValidationError, match="amount must be finite"):
        EstimatedTaxPayment(
            jurisdiction="Federal",
            due_date=date(2025, 4, 15),
            amount="NaN",
            status="not_paid",
        )


def test_estimated_tax_payment_respects_explicit_last_updated_and_is_frozen() -> None:
    payment = EstimatedTaxPayment(
        jurisdiction="Federal",
        due_date=date(2025, 1, 15),
        amount="10.00",
        status="paid",
        paid_date=date(2025, 1, 10),
        last_updated=datetime(2025, 1, 1, 0, 0, tzinfo=UTC),
    )

    assert payment.last_updated == datetime(2025, 1, 1, 0, 0, tzinfo=UTC)
    with pytest.raises(ValidationError):
        payment.amount = Decimal("11.00")


def test_tax_deadline_valid_model() -> None:
    deadline = TaxDeadline(
        jurisdiction=" Federal ",
        name=" Q1 Estimated Tax ",
        due_date=date(2025, 4, 15),
        category="estimated_tax",
    )

    assert deadline.jurisdiction == "Federal"
    assert deadline.name == "Q1 Estimated Tax"
    assert deadline.due_date == date(2025, 4, 15)
    assert deadline.category == "estimated_tax"
    assert deadline.completed is False


def test_tax_deadline_rejects_blank_text_and_invalid_category() -> None:
    with pytest.raises(ValidationError, match="must not be blank"):
        TaxDeadline(
            jurisdiction=" ",
            name="deadline",
            due_date=date(2025, 4, 15),
            category="filing",
        )

    with pytest.raises(ValidationError):
        TaxDeadline(
            jurisdiction="Federal",
            name="deadline",
            due_date=date(2025, 4, 15),
            category="other",  # type: ignore[arg-type]
        )


def test_tax_deadline_awareness_past_due_detection() -> None:
    deadline = TaxDeadline(
        jurisdiction="Federal",
        name="Q1 Estimated Tax",
        due_date=date(2025, 4, 14),
        category="estimated_tax",
        completed=False,
    )

    assert deadline.awareness_status(today=date(2025, 4, 15)) == "past_due"


def test_tax_deadline_awareness_upcoming_within_30_days() -> None:
    deadline = TaxDeadline(
        jurisdiction="Federal",
        name="Q2 Estimated Tax",
        due_date=date(2025, 6, 15),
        category="estimated_tax",
        completed=False,
    )

    assert deadline.awareness_status(today=date(2025, 5, 20)) == "upcoming"


def test_tax_deadline_awareness_completed_deadline_not_flagged() -> None:
    deadline = TaxDeadline(
        jurisdiction="Federal",
        name="Q1 Estimated Tax",
        due_date=date(2025, 4, 15),
        category="estimated_tax",
        completed=True,
    )

    assert deadline.awareness_status(today=date(2025, 4, 16)) == "none"


def test_tax_deadline_awareness_boundary_today_equals_due_date_is_upcoming() -> None:
    deadline = TaxDeadline(
        jurisdiction="Federal",
        name="Q1 Estimated Tax",
        due_date=date(2025, 4, 15),
        category="estimated_tax",
        completed=False,
    )

    assert deadline.awareness_status(today=date(2025, 4, 15)) == "upcoming"


def test_tax_deadline_awareness_future_deadline_not_flagged() -> None:
    deadline = TaxDeadline(
        jurisdiction="Federal",
        name="Q4 Estimated Tax",
        due_date=date(2025, 12, 15),
        category="estimated_tax",
        completed=False,
    )

    assert deadline.awareness_status(today=date(2025, 6, 1)) == "none"
