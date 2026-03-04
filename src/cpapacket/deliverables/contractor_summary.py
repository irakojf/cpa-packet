"""Helpers for contractor summary calculations."""

from __future__ import annotations

from collections.abc import Mapping
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Protocol

from cpapacket.utils.constants import CONTRACTOR_1099_THRESHOLD

_CENT = Decimal("0.01")
_ZERO = Decimal("0.00")
_CONTRACTOR_KEYWORDS = ("contract", "contractor", "subcontract")
_CONTRACTOR_ACCOUNT_TYPES = {"expense", "costofgoodssold", "cogs"}


class _AccountProviders(Protocol):
    def get_accounts(self) -> dict[str, Any]:
        """Return QBO accounts query payload."""


def should_flag_for_1099_review(*, non_card_total: Decimal) -> bool:
    """Return whether non-card payments meet the 1099 review threshold."""
    normalized_total = non_card_total.quantize(_CENT, rounding=ROUND_HALF_UP)
    if normalized_total <= _ZERO:
        return False
    return normalized_total >= CONTRACTOR_1099_THRESHOLD


def detect_contractor_accounts(*, providers: _AccountProviders) -> list[dict[str, str]]:
    """Identify contractor-related Expense/COGS accounts from QBO account payload."""
    payload = providers.get_accounts()
    accounts = _extract_accounts(payload)
    detected: list[dict[str, str]] = []
    seen_ids: set[str] = set()

    for account in accounts:
        account_id = str(account.get("Id", "")).strip()
        account_name = str(account.get("Name", "")).strip()
        account_type = str(account.get("AccountType", "")).strip()
        if not account_id or not account_name:
            continue
        if not _is_contractor_account_type(account_type):
            continue
        if not _contains_contractor_keyword(account_name):
            continue
        if account_id in seen_ids:
            continue

        seen_ids.add(account_id)
        detected.append(
            {
                "id": account_id,
                "name": account_name,
                "account_type": account_type,
            }
        )

    return sorted(detected, key=lambda account: account["name"].lower())


def _extract_accounts(payload: dict[str, Any]) -> list[Mapping[str, Any]]:
    query_response = payload.get("QueryResponse")
    if isinstance(query_response, Mapping):
        raw_accounts = query_response.get("Account")
        if isinstance(raw_accounts, list):
            return [account for account in raw_accounts if isinstance(account, Mapping)]

    raw_accounts = payload.get("Account")
    if isinstance(raw_accounts, list):
        return [account for account in raw_accounts if isinstance(account, Mapping)]
    return []


def _is_contractor_account_type(account_type: str) -> bool:
    normalized = "".join(ch for ch in account_type.strip().lower() if ch.isalnum())
    return normalized in _CONTRACTOR_ACCOUNT_TYPES


def _contains_contractor_keyword(account_name: str) -> bool:
    normalized = account_name.strip().lower()
    return any(keyword in normalized for keyword in _CONTRACTOR_KEYWORDS)
