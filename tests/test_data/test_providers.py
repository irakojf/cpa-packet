from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from cpapacket.data.providers import DataProviders
from cpapacket.data.store import SessionDataStore


class _FakeResponse:
    def __init__(self, payload: Any) -> None:
        self._payload = payload

    def json(self) -> Any:
        return self._payload


class _FakeQboClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def request(
        self,
        method: str,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> _FakeResponse:
        self.calls.append(
            {
                "method": method,
                "endpoint": endpoint,
                "params": params,
                "json_body": json_body,
            }
        )
        return _FakeResponse({"endpoint": endpoint, "params": params or {}})


class _FakeGustoClient:
    def __init__(self, payload: Any | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self.payload = payload if payload is not None else [{"id": "run-1"}]

    def request(
        self,
        method: str,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        required: bool = True,
    ) -> _FakeResponse | None:
        self.calls.append(
            {
                "method": method,
                "endpoint": endpoint,
                "params": params,
                "json_body": json_body,
                "required": required,
            }
        )
        if self.payload is None:
            return None
        return _FakeResponse(self.payload)


def test_get_pnl_uses_store_cache_for_repeated_calls() -> None:
    store = SessionDataStore()
    qbo = _FakeQboClient()
    providers = DataProviders(store=store, qbo_client=qbo)

    first = providers.get_pnl(2025, "Accrual")
    second = providers.get_pnl(2025, "Accrual")

    assert first == second
    assert len(qbo.calls) == 1
    assert qbo.calls[0]["endpoint"] == "/reports/ProfitAndLoss"


def test_get_general_ledger_uses_month_date_range() -> None:
    store = SessionDataStore()
    qbo = _FakeQboClient()
    providers = DataProviders(store=store, qbo_client=qbo)

    payload = providers.get_general_ledger(2025, 2)

    assert payload["params"]["start_date"] == "2025-02-01"
    assert payload["params"]["end_date"] == "2025-02-28"


def test_get_balance_sheet_accepts_date_object() -> None:
    store = SessionDataStore()
    qbo = _FakeQboClient()
    providers = DataProviders(store=store, qbo_client=qbo)

    payload = providers.get_balance_sheet(2025, date(2025, 12, 31))

    assert payload["endpoint"] == "/reports/BalanceSheet"
    assert payload["params"]["as_of_date"] == "2025-12-31"


def test_get_payroll_runs_returns_empty_when_gusto_absent() -> None:
    store = SessionDataStore()
    qbo = _FakeQboClient()
    providers = DataProviders(store=store, qbo_client=qbo, gusto_client=None)

    assert providers.get_payroll_runs(2025) == []


def test_get_payroll_runs_uses_optional_gusto_request_and_cache() -> None:
    store = SessionDataStore()
    qbo = _FakeQboClient()
    gusto = _FakeGustoClient(payload=[{"id": "r1"}, {"id": "r2"}])
    providers = DataProviders(store=store, qbo_client=qbo, gusto_client=gusto)

    first = providers.get_payroll_runs(2025)
    second = providers.get_payroll_runs(2025)

    assert first == [{"id": "r1"}, {"id": "r2"}]
    assert second == first
    assert len(gusto.calls) == 1
    assert gusto.calls[0]["required"] is False


def test_get_general_ledger_rejects_invalid_month() -> None:
    store = SessionDataStore()
    qbo = _FakeQboClient()
    providers = DataProviders(store=store, qbo_client=qbo)

    with pytest.raises(ValueError, match="month must be between 1 and 12"):
        providers.get_general_ledger(2025, 13)
