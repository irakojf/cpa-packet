from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

from cpapacket.deliverables.payroll_summary import normalize_payroll_runs
from cpapacket.reconciliation.payroll_recon import (
    collect_payroll_recon_edge_warnings,
    compute_gusto_reconciliation_total,
    compute_qbo_payroll_total,
    detect_qbo_payroll_accounts,
    fetch_gusto_reconciliation_total,
    fetch_qbo_payroll_total,
    reconcile_payroll_totals,
)


class _Provider:
    def __init__(self, runs: list[dict[str, object]]) -> None:
        self._runs = runs
        self.calls: list[int] = []

    def get_payroll_runs(self, year: int) -> list[dict[str, object]]:
        self.calls.append(year)
        return self._runs


class _QboProvider:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload
        self.calls = 0

    def get_accounts(self) -> dict[str, object]:
        self.calls += 1
        return self._payload


def _load_payroll_runs_fixture() -> list[dict[str, object]]:
    payload = json.loads(Path("tests/fixtures/gusto/payroll_2025.json").read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    runs = payload.get("payrolls")
    assert isinstance(runs, list)
    return [run for run in runs if isinstance(run, dict)]


def test_reconcile_payroll_totals_within_tolerance_is_reconciled() -> None:
    result = reconcile_payroll_totals(
        gusto_total=Decimal("1000.00"),
        qbo_total=Decimal("1000.01"),
    )

    assert result.variance == Decimal("0.01")
    assert result.status == "RECONCILED"


def test_reconcile_payroll_totals_over_tolerance_is_mismatch() -> None:
    result = reconcile_payroll_totals(
        gusto_total=Decimal("1000.00"),
        qbo_total=Decimal("1000.02"),
    )

    assert result.variance == Decimal("0.02")
    assert result.status == "MISMATCH"


def test_reconcile_payroll_totals_exact_negative_boundary_is_reconciled() -> None:
    result = reconcile_payroll_totals(
        gusto_total=Decimal("500.01"),
        qbo_total=Decimal("500.00"),
    )

    assert result.variance == Decimal("-0.01")
    assert result.status == "RECONCILED"


def test_reconcile_payroll_totals_quantizes_inputs() -> None:
    result = reconcile_payroll_totals(
        gusto_total=Decimal("1000"),
        qbo_total=Decimal("1000.005"),
    )

    assert result.gusto_total == Decimal("1000.00")
    assert result.qbo_total == Decimal("1000.01")
    assert result.variance == Decimal("0.01")


def test_reconcile_payroll_totals_rejects_negative_tolerance() -> None:
    with pytest.raises(ValueError, match="tolerance must be >= 0"):
        reconcile_payroll_totals(
            gusto_total=Decimal("100.00"),
            qbo_total=Decimal("100.00"),
            tolerance=Decimal("-0.01"),
        )


def test_reconcile_payroll_totals_handles_zero_totals() -> None:
    result = reconcile_payroll_totals(
        gusto_total=Decimal("0.00"),
        qbo_total=Decimal("0.00"),
    )

    assert result.variance == Decimal("0.00")
    assert result.status == "RECONCILED"


def test_compute_gusto_reconciliation_total_matches_payroll_summary_formula() -> None:
    runs = normalize_payroll_runs(_load_payroll_runs_fixture())

    total = compute_gusto_reconciliation_total(runs)

    assert total == Decimal("42476.00")


def test_fetch_gusto_reconciliation_total_uses_provider_runs() -> None:
    provider = _Provider(_load_payroll_runs_fixture())

    total = fetch_gusto_reconciliation_total(providers=provider, year=2025)

    assert provider.calls == [2025]
    assert total == Decimal("42476.00")


def test_compute_qbo_payroll_total_filters_by_type_and_keywords() -> None:
    payload: dict[str, object] = {
        "QueryResponse": {
            "Account": [
                {
                    "Name": "Payroll Expense",
                    "AccountType": "Expense",
                    "CurrentBalance": "15000.00",
                },
                {
                    "Name": "Wages Expense",
                    "AccountType": "Expense",
                    "Balance": "8200.00",
                },
                {
                    "Name": "Employer Tax Expense",
                    "AccountType": "Cost of Goods Sold",
                    "CurrentBalance": "2500.00",
                },
                {
                    "Name": "401(k) Employer Match",
                    "AccountType": "Expense",
                    "CurrentBalance": "1300.00",
                },
                {
                    "Name": "Office Supplies",
                    "AccountType": "Expense",
                    "CurrentBalance": "999.00",
                },
                {
                    "Name": "Payroll Liability",
                    "AccountType": "Liability",
                    "CurrentBalance": "5000.00",
                },
            ]
        }
    }

    total = compute_qbo_payroll_total(payload)

    assert total == Decimal("27000.00")


@pytest.mark.parametrize(
    ("name", "balance"),
    [
        ("Payroll Expense", "10.00"),
        ("Wages Expense", "20.00"),
        ("Salary Expense", "30.00"),
        ("Employer Tax Expense", "40.00"),
        ("401(k) Employer Match", "50.00"),
    ],
)
def test_compute_qbo_payroll_total_matches_each_keyword(name: str, balance: str) -> None:
    payload: dict[str, object] = {
        "QueryResponse": {
            "Account": [
                {
                    "Name": name,
                    "AccountType": "Expense",
                    "CurrentBalance": balance,
                }
            ]
        }
    }

    total = compute_qbo_payroll_total(payload)

    assert total == Decimal(balance)


def test_compute_qbo_payroll_total_handles_invalid_or_missing_balances() -> None:
    payload: dict[str, object] = {
        "QueryResponse": {
            "Account": [
                {"Name": "Salary", "AccountType": "Expense", "CurrentBalance": "abc"},
                {"Name": "Wages", "AccountType": "Expense"},
                {"Name": "Payroll", "AccountType": "Expense", "CurrentBalance": ""},
            ]
        }
    }

    total = compute_qbo_payroll_total(payload)

    assert total == Decimal("0.00")


def test_fetch_qbo_payroll_total_calls_provider_once() -> None:
    provider = _QboProvider(
        {
            "QueryResponse": {
                "Account": [
                    {
                        "Name": "Payroll Expense",
                        "AccountType": "Expense",
                        "CurrentBalance": "123.45",
                    }
                ]
            }
        }
    )

    total = fetch_qbo_payroll_total(providers=provider)

    assert provider.calls == 1
    assert total == Decimal("123.45")


def test_detect_qbo_payroll_accounts_reports_match_count_and_total() -> None:
    payload: dict[str, object] = {
        "QueryResponse": {
            "Account": [
                {"Name": "Payroll Expense", "AccountType": "Expense", "CurrentBalance": "100.00"},
                {"Name": "Office Supplies", "AccountType": "Expense", "CurrentBalance": "25.00"},
                {"Name": "Wages Expense", "AccountType": "Expense", "CurrentBalance": "50.00"},
            ]
        }
    }

    detected = detect_qbo_payroll_accounts(payload)

    assert detected.matched_account_count == 2
    assert detected.total == Decimal("150.00")


def test_collect_payroll_recon_edge_warnings_manual_journal_inflation() -> None:
    reconciliation = reconcile_payroll_totals(
        gusto_total=Decimal("1000.00"),
        qbo_total=Decimal("1200.00"),
    )

    warnings = collect_payroll_recon_edge_warnings(
        reconciliation=reconciliation,
        matched_qbo_accounts=3,
    )

    assert (
        "QBO payroll total exceeds Gusto beyond tolerance; check for manual payroll journals."
        in warnings
    )


def test_collect_payroll_recon_edge_warnings_sync_lag_minor_variance() -> None:
    reconciliation = reconcile_payroll_totals(
        gusto_total=Decimal("1000.00"),
        qbo_total=Decimal("1000.01"),
    )

    warnings = collect_payroll_recon_edge_warnings(
        reconciliation=reconciliation,
        matched_qbo_accounts=2,
    )

    assert "Minor payroll variance is within tolerance; possible Gusto-to-QBO sync lag." in warnings


def test_collect_payroll_recon_edge_warnings_no_matching_qbo_accounts() -> None:
    reconciliation = reconcile_payroll_totals(
        gusto_total=Decimal("1500.00"),
        qbo_total=Decimal("0.00"),
    )

    warnings = collect_payroll_recon_edge_warnings(
        reconciliation=reconciliation,
        matched_qbo_accounts=0,
    )

    assert (
        "No matching QBO payroll accounts were found; variance likely reflects full Gusto total."
        in warnings
    )
