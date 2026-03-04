"""Formatting helpers for currency values and PDF indentation."""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP

_TWO_PLACES = Decimal("0.01")


def format_currency_csv(amount: Decimal) -> str:
    """Format a Decimal amount for CSV output (plain numeric, 2dp)."""
    normalized = _normalize_decimal(amount)
    return f"{normalized:.2f}"


def format_currency_pdf(amount: Decimal) -> str:
    """Format a Decimal amount for PDF output ($ + thousands separators)."""
    normalized = _normalize_decimal(amount)
    absolute = abs(normalized)
    if normalized < Decimal("0"):
        return f"-${absolute:,.2f}"
    return f"${absolute:,.2f}"


def indent_for_level(level: int, *, spaces_per_level: int = 2) -> str:
    """Return left-padding spaces for hierarchical PDF row indentation."""
    if level < 0:
        raise ValueError("level must be >= 0")
    if spaces_per_level < 1:
        raise ValueError("spaces_per_level must be >= 1")
    return " " * (level * spaces_per_level)


def _normalize_decimal(amount: Decimal) -> Decimal:
    if not isinstance(amount, Decimal):
        raise TypeError("amount must be decimal.Decimal")
    if not amount.is_finite():
        raise ValueError("amount must be finite")
    return amount.quantize(_TWO_PLACES, rounding=ROUND_HALF_UP)
