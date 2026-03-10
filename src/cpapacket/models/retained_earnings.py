"""Book-equity rollforward domain model."""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator

_TWO_PLACES = Decimal("0.01")


class RetainedEarningsRollforward(BaseModel):
    """Canonical book-equity rollforward values for packet reporting."""

    model_config = ConfigDict(frozen=True)

    beginning_book_equity_bucket: Decimal
    current_year_net_income: Decimal
    current_year_distributions_gl: Decimal
    current_year_distributions_bs_change: Decimal
    current_year_contributions_gl: Decimal
    other_direct_book_equity_postings: Decimal
    expected_ending_book_equity_bucket: Decimal
    actual_ending_book_equity_bucket: Decimal
    ending_book_equity_difference: Decimal
    status: Literal["Balanced", "Review"]
    flags: list[str]

    @field_validator(
        "beginning_book_equity_bucket",
        "current_year_net_income",
        "current_year_distributions_gl",
        "current_year_distributions_bs_change",
        "current_year_contributions_gl",
        "other_direct_book_equity_postings",
        "expected_ending_book_equity_bucket",
        "actual_ending_book_equity_bucket",
        "ending_book_equity_difference",
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
