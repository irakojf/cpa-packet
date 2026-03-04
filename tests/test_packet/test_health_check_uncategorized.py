from __future__ import annotations

import json
from pathlib import Path

from cpapacket.packet.health_check import (
    DataHealthCheckContext,
    check_open_prior_year_items,
    check_suspense_accounts_balance,
    check_uncategorized_transactions,
    check_undeposited_funds_balance,
)


class _FakeProviders:
    def __init__(self) -> None:
        self.calls: list[tuple[int, int]] = []

    def get_general_ledger(self, year: int, month: int) -> dict[str, object]:
        self.calls.append((year, month))
        if month == 2:
            return {
                "Rows": {
                    "Row": [
                        {
                            "type": "Section",
                            "Header": {
                                "ColData": [
                                    {"value": "Uncategorized Income"},
                                    {"value": "125.50"},
                                ]
                            },
                            "Rows": {"Row": []},
                        },
                        {
                            "ColData": [
                                {"value": "Uncategorized Expense"},
                                {"value": "(10.00)"},
                            ]
                        },
                    ]
                }
            }
        return {"Rows": {"Row": []}}


def test_check_uncategorized_transactions_warns_with_count_and_total() -> None:
    providers = _FakeProviders()
    context = DataHealthCheckContext(year=2025, providers=providers, gusto_connected=False)

    issue = check_uncategorized_transactions(context)

    assert issue is not None
    assert issue.code == "uncategorized_transactions"
    assert issue.metadata["count"] == "2"
    assert issue.metadata["dollar_total"] == "135.50"
    assert providers.calls == [(2025, month) for month in range(1, 13)]


class _CleanProviders:
    def get_general_ledger(self, year: int, month: int) -> dict[str, object]:
        del year, month
        return {"Rows": {"Row": []}}


def test_check_uncategorized_transactions_returns_none_when_clean() -> None:
    context = DataHealthCheckContext(year=2025, providers=_CleanProviders(), gusto_connected=False)

    issue = check_uncategorized_transactions(context)

    assert issue is None


class _BalanceProviders:
    def __init__(self, balance_value: str) -> None:
        self._balance_value = balance_value
        self.calls: list[tuple[int, str]] = []

    def get_balance_sheet(self, year: int, as_of: str) -> dict[str, object]:
        self.calls.append((year, as_of))
        return {
            "Rows": {
                "Row": [
                    {
                        "type": "Section",
                        "Header": {
                            "ColData": [
                                {"value": "Undeposited Funds"},
                                {"value": self._balance_value},
                            ]
                        },
                        "Rows": {"Row": []},
                    }
                ]
            }
        }


def test_check_undeposited_funds_balance_warns_when_non_zero() -> None:
    providers = _BalanceProviders("42.12")
    context = DataHealthCheckContext(year=2025, providers=providers, gusto_connected=False)

    issue = check_undeposited_funds_balance(context)

    assert issue is not None
    assert issue.code == "undeposited_funds_balance"
    assert issue.metadata["as_of"] == "2025-12-31"
    assert issue.metadata["balance"] == "42.12"
    assert providers.calls == [(2025, "2025-12-31")]


def test_check_undeposited_funds_balance_respects_tolerance() -> None:
    providers = _BalanceProviders("0.01")
    context = DataHealthCheckContext(year=2025, providers=providers, gusto_connected=False)

    issue = check_undeposited_funds_balance(context)

    assert issue is None


class _SuspenseProviders:
    def __init__(self, amount: str) -> None:
        self._amount = amount

    def get_balance_sheet(self, year: int, as_of: str) -> dict[str, object]:
        del year, as_of
        return {
            "Rows": {
                "Row": [
                    {
                        "type": "Section",
                        "Header": {
                            "ColData": [
                                {"value": "Ask My Accountant"},
                                {"value": self._amount},
                            ]
                        },
                        "Rows": {"Row": []},
                    }
                ]
            }
        }


def test_check_suspense_accounts_balance_warns_when_non_zero() -> None:
    context = DataHealthCheckContext(
        year=2025,
        providers=_SuspenseProviders("100.00"),
        gusto_connected=False,
    )

    issue = check_suspense_accounts_balance(context)

    assert issue is not None
    assert issue.code == "suspense_balance"
    assert issue.metadata["balance"] == "100.00"


def test_check_suspense_accounts_balance_returns_none_when_within_tolerance() -> None:
    context = DataHealthCheckContext(
        year=2025,
        providers=_SuspenseProviders("0.01"),
        gusto_connected=False,
    )

    issue = check_suspense_accounts_balance(context)

    assert issue is None


class _PriorYearProviders:
    def __init__(self) -> None:
        self.calls: list[tuple[int, int]] = []

    def get_general_ledger(self, year: int, month: int) -> dict[str, object]:
        self.calls.append((year, month))
        if month == 1:
            return {
                "Rows": {
                    "Row": [
                        {
                            "TxnId": "inv-1",
                            "TxnDate": "2024-12-30",
                            "AccountType": "AccountsReceivable",
                            "Amount": "100.00",
                        },
                        {
                            "TxnId": "inv-1",
                            "TxnDate": "2024-12-30",
                            "AccountType": "AccountsReceivable",
                            "Amount": "100.00",
                        },
                        {
                            "TxnId": "bill-1",
                            "TxnDate": "2024-11-15",
                            "AccountType": "AccountsPayable",
                            "Amount": "(25.00)",
                        },
                        {
                            "TxnId": "inv-2",
                            "TxnDate": "2025-01-02",
                            "AccountType": "AccountsReceivable",
                            "Amount": "200.00",
                        },
                    ]
                }
            }
        return {"Rows": {"Row": []}}


def test_check_open_prior_year_items_counts_unique_prior_ar_ap_entries() -> None:
    providers = _PriorYearProviders()
    context = DataHealthCheckContext(year=2025, providers=providers, gusto_connected=False)

    issue = check_open_prior_year_items(context)

    assert issue is not None
    assert issue.code == "open_prior_year_items"
    assert issue.metadata["as_of"] == "2025-01-01"
    assert issue.metadata["count"] == "2"
    assert issue.metadata["dollar_total"] == "125.00"
    assert providers.calls == [(2025, month) for month in range(1, 13)]


class _NoPriorYearProviders:
    def get_general_ledger(self, year: int, month: int) -> dict[str, object]:
        del year, month
        return {"Rows": {"Row": []}}


def test_check_open_prior_year_items_returns_none_when_none_found() -> None:
    context = DataHealthCheckContext(
        year=2025,
        providers=_NoPriorYearProviders(),
        gusto_connected=False,
    )

    issue = check_open_prior_year_items(context)

    assert issue is None


def test_health_check_fixture_contains_expected_warning_signals() -> None:
    payload = json.loads(
        Path("tests/fixtures/qbo/uncategorized_transactions.json").read_text(encoding="utf-8")
    )

    class _FixtureProviders:
        def __init__(self, fixture: dict[str, object]) -> None:
            self._fixture = fixture

        def get_general_ledger(self, year: int, month: int) -> dict[str, object]:
            del year
            gl_by_month = self._fixture["general_ledger_by_month"]
            assert isinstance(gl_by_month, dict)
            data = gl_by_month[str(month)]
            assert isinstance(data, dict)
            return data

        def get_balance_sheet(self, year: int, as_of: str) -> dict[str, object]:
            del year, as_of
            data = self._fixture["balance_sheet_year_end"]
            assert isinstance(data, dict)
            return data

    context = DataHealthCheckContext(
        year=2025,
        providers=_FixtureProviders(payload),
        gusto_connected=False,
    )

    uncategorized = check_uncategorized_transactions(context)
    assert uncategorized is not None
    assert uncategorized.code == "uncategorized_transactions"
    assert uncategorized.metadata["count"] == "2"
    assert uncategorized.metadata["dollar_total"] == "135.50"

    undeposited = check_undeposited_funds_balance(context)
    assert undeposited is not None
    assert undeposited.code == "undeposited_funds_balance"
    assert undeposited.metadata["balance"] == "42.12"

    suspense = check_suspense_accounts_balance(context)
    assert suspense is not None
    assert suspense.code == "suspense_balance"
    assert suspense.metadata["balance"] == "100.00"
