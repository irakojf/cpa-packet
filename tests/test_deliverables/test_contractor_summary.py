"""Tests for contractor summary helper functions."""

from __future__ import annotations

from decimal import Decimal

from cpapacket.deliverables.contractor_summary import should_flag_for_1099_review


def test_should_flag_for_1099_review_at_threshold() -> None:
    assert should_flag_for_1099_review(non_card_total=Decimal("600.00")) is True


def test_should_flag_for_1099_review_below_threshold() -> None:
    assert should_flag_for_1099_review(non_card_total=Decimal("599.99")) is False


def test_should_flag_for_1099_review_card_only_vendor_not_flagged() -> None:
    assert should_flag_for_1099_review(non_card_total=Decimal("0.00")) is False
