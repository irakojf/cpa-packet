"""Shared normalized row model for financial report outputs."""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

_TWO_PLACES = Decimal("0.01")


class NormalizedRow(BaseModel):
    """Canonical flattened row used by P&L and Balance Sheet pipelines."""

    model_config = ConfigDict(frozen=True)

    section: str
    label: str
    amount: Decimal
    row_type: Literal["header", "account", "subtotal", "total"]
    level: int = Field(ge=0)
    path: str

    @field_validator("section", "label", "path")
    @classmethod
    def _must_not_be_blank(cls, value: str) -> str:
        trimmed = value.strip()
        if not trimmed:
            raise ValueError("must not be blank")
        return trimmed

    @field_validator("amount", mode="before")
    @classmethod
    def _coerce_and_quantize_amount(cls, value: object) -> Decimal:
        try:
            decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise ValueError("amount must be a valid decimal value") from exc

        if not decimal_value.is_finite():
            raise ValueError("amount must be finite")

        return decimal_value.quantize(_TWO_PLACES, rounding=ROUND_HALF_UP)
