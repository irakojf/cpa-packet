"""Contractor summary domain model."""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal, InvalidOperation

from pydantic import BaseModel, ConfigDict, Field, field_validator

_TWO_PLACES = Decimal("0.01")


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


class ContractorRecord(BaseModel):
    """Aggregated contractor payment and 1099-review summary for one vendor."""

    model_config = ConfigDict(frozen=True)

    vendor_id: str
    display_name: str
    tax_id_on_file: bool
    total_paid: Decimal = Field(ge=Decimal("0.00"))
    contractor_account_total: Decimal = Field(ge=Decimal("0.00"))
    card_processor_total: Decimal = Field(ge=Decimal("0.00"))
    non_card_total: Decimal = Field(ge=Decimal("0.00"))
    requires_1099_review: bool
    flags: list[str] = Field(default_factory=list)
    source_accounts: list[str] = Field(default_factory=list)

    @field_validator("vendor_id", "display_name")
    @classmethod
    def _require_non_blank(cls, value: str) -> str:
        trimmed = value.strip()
        if not trimmed:
            raise ValueError("must not be blank")
        return trimmed

    @field_validator("flags", "source_accounts")
    @classmethod
    def _normalize_flags(cls, value: list[str]) -> list[str]:
        cleaned: list[str] = []
        for item in value:
            normalized = item.strip()
            if not normalized:
                continue
            cleaned.append(normalized)
        return cleaned

    @field_validator(
        "total_paid",
        "contractor_account_total",
        "card_processor_total",
        "non_card_total",
        mode="before",
    )
    @classmethod
    def _coerce_amounts(cls, value: object) -> Decimal:
        return _coerce_money(value)
