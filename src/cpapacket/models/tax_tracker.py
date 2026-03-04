"""Tax tracker domain models."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_TWO_PLACES = Decimal("0.01")

Jurisdiction = Literal["DE", "NY", "Federal"]
PaymentStatus = Literal["paid", "not_paid"]
DeadlineCategory = Literal["estimated_tax", "filing", "extension"]
DeadlineAwarenessStatus = Literal["past_due", "upcoming", "none"]


class EstimatedTaxPayment(BaseModel):
    """One estimated tax payment record and its lifecycle status."""

    model_config = ConfigDict(frozen=True)

    jurisdiction: Jurisdiction
    due_date: date
    amount: Decimal = Field(ge=Decimal("0.00"))
    status: PaymentStatus = "not_paid"
    paid_date: date | None = None
    last_updated: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("amount", mode="before")
    @classmethod
    def _coerce_amount(cls, value: object) -> Decimal:
        try:
            decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise ValueError("amount must be a valid decimal value") from exc
        if not decimal_value.is_finite():
            raise ValueError("amount must be finite")
        if decimal_value < Decimal("0"):
            raise ValueError("amount must be >= 0")
        return decimal_value.quantize(_TWO_PLACES, rounding=ROUND_HALF_UP)

    @model_validator(mode="after")
    def _validate_paid_status(self) -> EstimatedTaxPayment:
        if self.status == "paid" and self.paid_date is None:
            raise ValueError("paid_date is required when status is paid")
        if self.status == "not_paid" and self.paid_date is not None:
            raise ValueError("paid_date must be null when status is not_paid")
        return self


class TaxDeadline(BaseModel):
    """One tax-related deadline entry for dashboard/status workflows."""

    model_config = ConfigDict(frozen=True)

    jurisdiction: str
    name: str
    due_date: date
    category: DeadlineCategory
    completed: bool = False

    @field_validator("jurisdiction", "name")
    @classmethod
    def _validate_required_text(cls, value: str) -> str:
        trimmed = value.strip()
        if not trimmed:
            raise ValueError("must not be blank")
        return trimmed

    def awareness_status(
        self,
        *,
        today: date | None = None,
        upcoming_window_days: int = 30,
    ) -> DeadlineAwarenessStatus:
        """Classify deadline urgency for status dashboards."""
        if self.completed:
            return "none"
        if upcoming_window_days < 0:
            raise ValueError("upcoming_window_days must be >= 0")

        reference_day = today or date.today()
        if self.due_date < reference_day:
            return "past_due"
        if self.due_date <= reference_day + timedelta(days=upcoming_window_days):
            return "upcoming"
        return "none"
