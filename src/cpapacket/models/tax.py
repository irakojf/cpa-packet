"""Estimated tax tracker domain models."""

from __future__ import annotations

from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

_TWO_PLACES = Decimal("0.01")

Jurisdiction = Literal["DE", "NY", "Federal"]
PaymentStatus = Literal["paid", "not_paid"]
TaxCategory = Literal["estimated_tax", "filing", "extension"]


def _coerce_money(value: object) -> Decimal:
    try:
        decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("must be a valid decimal value") from exc

    if not decimal_value.is_finite():
        raise ValueError("must be finite")
    if decimal_value < Decimal("0"):
        raise ValueError("must be >= 0")
    return decimal_value.quantize(_TWO_PLACES, rounding=ROUND_HALF_UP)


class EstimatedTaxPayment(BaseModel):
    """Estimated tax payment entry for a single jurisdiction and due date."""

    model_config = ConfigDict(frozen=True)

    jurisdiction: Jurisdiction
    due_date: date
    amount: Decimal = Field(ge=Decimal("0.00"))
    status: PaymentStatus
    paid_date: date | None = None
    last_updated: datetime

    @field_validator("jurisdiction", "status", mode="before")
    @classmethod
    def _normalize_enum_fields(cls, value: object) -> object:
        if isinstance(value, str):
            return value.strip()
        return value

    @field_validator("amount", mode="before")
    @classmethod
    def _coerce_amount(cls, value: object) -> Decimal:
        return _coerce_money(value)


class TaxDeadline(BaseModel):
    """Static tax deadline entry for a jurisdiction."""

    model_config = ConfigDict(frozen=True)

    jurisdiction: Jurisdiction
    name: str
    due_date: date
    category: TaxCategory
    completed: bool = False

    @field_validator("jurisdiction", "category", mode="before")
    @classmethod
    def _normalize_enum_fields(cls, value: object) -> object:
        if isinstance(value, str):
            return value.strip()
        return value

    @field_validator("name")
    @classmethod
    def _require_name(cls, value: str) -> str:
        trimmed = value.strip()
        if not trimmed:
            raise ValueError("must not be blank")
        return trimmed
