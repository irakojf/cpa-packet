"""High-level typed data accessors backed by SessionDataStore."""

from __future__ import annotations

import calendar
from datetime import date
from typing import Any, Protocol

from cpapacket.data.keys import build_cache_key
from cpapacket.data.store import SessionDataStore


class _JsonResponse(Protocol):
    def json(self) -> Any:
        ...


class _QboClient(Protocol):
    def request(
        self,
        method: str,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> _JsonResponse:
        ...


class _GustoClient(Protocol):
    def request(
        self,
        method: str,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        required: bool = True,
    ) -> _JsonResponse | None:
        ...


class DataProviders:
    """Provider-layer gateway for all API fetches and cache-key generation."""

    def __init__(
        self,
        *,
        store: SessionDataStore,
        qbo_client: _QboClient,
        gusto_client: _GustoClient | None = None,
        cache_version: str = "1",
    ) -> None:
        self._store = store
        self._qbo = qbo_client
        self._gusto = gusto_client
        self._cache_version = cache_version

    def get_pnl(self, year: int, method: str) -> dict[str, Any]:
        params = {
            "start_date": f"{year}-01-01",
            "end_date": f"{year}-12-31",
            "accounting_method": method,
        }
        return self._cached_qbo_json(endpoint="/reports/ProfitAndLoss", params=params, schema="qbo.pnl.v1")

    def get_balance_sheet(self, year: int, as_of: date | str) -> dict[str, Any]:
        as_of_date = as_of if isinstance(as_of, str) else as_of.isoformat()
        params = {"as_of_date": as_of_date}
        return self._cached_qbo_json(endpoint="/reports/BalanceSheet", params=params, schema="qbo.balance_sheet.v1")

    def get_general_ledger(self, year: int, month: int) -> dict[str, Any]:
        start_date, end_date = _month_date_range(year=year, month=month)
        params = {
            "start_date": start_date,
            "end_date": end_date,
            "accounting_method": "Accrual",
        }
        return self._cached_qbo_json(endpoint="/reports/GeneralLedger", params=params, schema="qbo.general_ledger.v1")

    def get_payroll_runs(self, year: int) -> list[dict[str, Any]]:
        if self._gusto is None:
            return []

        params = {
            "start_date": f"{year}-01-01",
            "end_date": f"{year}-12-31",
        }

        cache_key = build_cache_key(
            source="gusto",
            endpoint="/payrolls",
            params=params,
            schema="gusto.payroll_runs.v1",
            cache_version=self._cache_version,
        )

        def fetcher() -> list[dict[str, Any]]:
            response = self._gusto.request("GET", "/payrolls", params=params, required=False)
            if response is None:
                return []
            payload = response.json()
            if isinstance(payload, list):
                return [item for item in payload if isinstance(item, dict)]
            return []

        payload, _source = self._store.get_or_fetch(cache_key, fetcher)
        return payload

    def get_accounts(self) -> dict[str, Any]:
        query = "select * from Account"
        params = {"query": query}
        return self._cached_qbo_json(endpoint="/query", params=params, schema="qbo.accounts.v1")

    def get_company_info(self) -> dict[str, Any]:
        return self._cached_qbo_json(endpoint="/companyinfo", params={}, schema="qbo.company_info.v1")

    def _cached_qbo_json(
        self,
        *,
        endpoint: str,
        params: dict[str, Any],
        schema: str,
    ) -> dict[str, Any]:
        cache_key = build_cache_key(
            source="qbo",
            endpoint=endpoint,
            params=params,
            schema=schema,
            cache_version=self._cache_version,
        )

        def fetcher() -> dict[str, Any]:
            response = self._qbo.request("GET", endpoint, params=params)
            payload = response.json()
            if isinstance(payload, dict):
                return payload
            raise TypeError("QBO response payload must be an object")

        payload, _source = self._store.get_or_fetch(cache_key, fetcher)
        return payload


def _month_date_range(*, year: int, month: int) -> tuple[str, str]:
    if month < 1 or month > 12:
        raise ValueError("month must be between 1 and 12")
    _, last_day = calendar.monthrange(year, month)
    return f"{year}-{month:02d}-01", f"{year}-{month:02d}-{last_day:02d}"
