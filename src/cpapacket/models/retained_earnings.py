"""Retained earnings rollforward domain model."""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator

_TWO_PLACES = Decimal("0.01")


class RetainedEarningsRollforward(BaseModel):
    """Canonical retained-earnings rollforward values for packet reporting."""

    model_config = ConfigDict(frozen=True)

    beginning_re: Decimal
    net_income: Decimal
    distributions: Decimal
    expected_ending_re: Decimal
    actual_ending_re: Decimal
    difference: Decimal
    status: Literal["Balanced", "Mismatch"]
    flags: list[str]

    @field_validator(
        "beginning_re",
        "net_income",
        "distributions",
        "expected_ending_re",
        "actual_ending_re",
        "difference",
        mode="before",
    )
    @classmethod
    def _coerce_money(cls, value: object) -> Decimal:
        try:
            decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError("must be a valid decimal value") from exc

        if not decimal_value.is_finite():
            raise ValueError("must be finite")
        return decimal_value.quantize(_TWO_PLACES, rounding=ROUND_HALF_UP)

    @field_validator("flags")
    @classmethod
    def _normalize_flags(cls, value: list[str]) -> list[str]:
        normalized: list[str] = []
        for item in value:
            token = item.strip()
            if not token:
                continue
            normalized.append(token)
        return normalized
