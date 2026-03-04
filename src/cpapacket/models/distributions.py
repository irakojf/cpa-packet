"""Distribution and miscoding domain models."""

from __future__ import annotations

from datetime import date
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

_TWO_PLACES = Decimal("0.01")


class MiscodedDistributionCandidate(BaseModel):
    """Represents a potentially miscoded shareholder distribution transaction."""

    model_config = ConfigDict(frozen=True)

    txn_id: str
    date: date
    transaction_type: str
    payee: str | None = None
    memo: str | None = None
    account: str
    amount: Decimal = Field(ge=Decimal("0.00"))
    reason_codes: list[str] = Field(min_length=1)
    confidence: Literal["High", "Medium", "Low"]
    score: int = Field(ge=0)

    @field_validator("txn_id", "transaction_type", "account")
    @classmethod
    def _require_non_blank(cls, value: str) -> str:
        trimmed = value.strip()
        if not trimmed:
            raise ValueError("must not be blank")
        return trimmed

    @field_validator("payee", "memo")
    @classmethod
    def _normalize_optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        trimmed = value.strip()
        return trimmed or None

    @field_validator("reason_codes")
    @classmethod
    def _normalize_reason_codes(cls, value: list[str]) -> list[str]:
        cleaned: list[str] = []
        for item in value:
            normalized = item.strip()
            if not normalized:
                continue
            cleaned.append(normalized)
        if not cleaned:
            raise ValueError("reason_codes must include at least one non-blank code")
        return cleaned

    @field_validator("amount", mode="before")
    @classmethod
    def _coerce_amount(cls, value: object) -> Decimal:
        try:
            decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError("amount must be a valid decimal value") from exc

        if not decimal_value.is_finite():
            raise ValueError("amount must be finite")
        if decimal_value < Decimal("0"):
            raise ValueError("amount must be >= 0")
        return decimal_value.quantize(_TWO_PLACES, rounding=ROUND_HALF_UP)
