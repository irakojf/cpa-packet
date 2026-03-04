"""Helpers for contractor summary calculations."""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal

from cpapacket.utils.constants import CONTRACTOR_1099_THRESHOLD

_CENT = Decimal("0.01")
_ZERO = Decimal("0.00")


def should_flag_for_1099_review(*, non_card_total: Decimal) -> bool:
    """Return whether non-card payments meet the 1099 review threshold."""
    normalized_total = non_card_total.quantize(_CENT, rounding=ROUND_HALF_UP)
    if normalized_total <= _ZERO:
        return False
    return normalized_total >= CONTRACTOR_1099_THRESHOLD
