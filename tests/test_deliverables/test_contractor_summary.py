"""Tests for contractor summary helper functions."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from cpapacket.deliverables.contractor_summary import (
    detect_contractor_accounts,
    should_flag_for_1099_review,
)


class _FakeProviders:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload
        self.calls = 0

    def get_accounts(self) -> dict[str, Any]:
        self.calls += 1
        return self._payload


def test_should_flag_for_1099_review_at_threshold() -> None:
    assert should_flag_for_1099_review(non_card_total=Decimal("600.00")) is True


def test_should_flag_for_1099_review_below_threshold() -> None:
    assert should_flag_for_1099_review(non_card_total=Decimal("599.99")) is False


def test_should_flag_for_1099_review_card_only_vendor_not_flagged() -> None:
    assert should_flag_for_1099_review(non_card_total=Decimal("0.00")) is False


def test_detect_contractor_accounts_filters_expense_and_cogs() -> None:
    providers = _FakeProviders(
        {
            "QueryResponse": {
                "Account": [
                    {"Id": "1", "Name": "Contract Labor", "AccountType": "Expense"},
                    {"Id": "2", "Name": "Subcontractors", "AccountType": "Cost of Goods Sold"},
                    {"Id": "3", "Name": "Office Expense", "AccountType": "Expense"},
                    {"Id": "4", "Name": "Contractor Income", "AccountType": "Income"},
                ]
            }
        }
    )

    detected = detect_contractor_accounts(providers=providers)

    assert detected == [
        {"id": "1", "name": "Contract Labor", "account_type": "Expense"},
        {"id": "2", "name": "Subcontractors", "account_type": "Cost of Goods Sold"},
    ]
    assert providers.calls == 1


def test_detect_contractor_accounts_accepts_root_account_list_shape() -> None:
    providers = _FakeProviders(
        {
            "Account": [
                {"Id": "10", "Name": "CONTRACTORS", "AccountType": "Expense"},
                {"Id": "11", "Name": "Stripe Fees", "AccountType": "Expense"},
            ]
        }
    )

    detected = detect_contractor_accounts(providers=providers)

    assert detected == [{"id": "10", "name": "CONTRACTORS", "account_type": "Expense"}]
