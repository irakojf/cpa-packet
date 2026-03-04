"""General ledger domain models."""

from __future__ import annotations

from datetime import date
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation

from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator

_TWO_PLACES = Decimal("0.01")


class GeneralLedgerRow(BaseModel):
    """Canonical general-ledger row model used by reconciliation workflows."""

    model_config = ConfigDict(frozen=True)

    txn_id: str
    date: date
    transaction_type: str
    document_number: str
    account_name: str
    account_type: str
    payee: str | None = None
    memo: str | None = None
    debit: Decimal = Field(ge=Decimal("0.00"))
    credit: Decimal = Field(ge=Decimal("0.00"))

    @field_validator(
        "txn_id",
        "transaction_type",
        "document_number",
        "account_name",
        "account_type",
    )
    @classmethod
    def _must_not_be_blank(cls, value: str) -> str:
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

    @field_validator("debit", "credit", mode="before")
    @classmethod
    def _coerce_money(cls, value: object) -> Decimal:
        try:
            decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise ValueError("must be a valid decimal value") from exc

        if not decimal_value.is_finite():
            raise ValueError("must be finite")
        if decimal_value < Decimal("0"):
            raise ValueError("must be >= 0")
        return decimal_value.quantize(_TWO_PLACES, rounding=ROUND_HALF_UP)

    @computed_field(return_type=Decimal)
    @property
    def signed_amount(self) -> Decimal:
        """Signed amount where debit is positive and credit is negative."""
        return (self.debit - self.credit).quantize(_TWO_PLACES, rounding=ROUND_HALF_UP)
