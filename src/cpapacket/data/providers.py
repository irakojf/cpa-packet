"""High-level typed data accessors backed by SessionDataStore."""

from __future__ import annotations

import calendar
from datetime import date
from typing import Any, Literal, Protocol

from cpapacket.data.keys import build_cache_key
from cpapacket.data.store import SessionDataStore


class _JsonResponse(Protocol):
    def json(self) -> Any: ...


class _QboClient(Protocol):
    def request(
        self,
        method: str,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> _JsonResponse: ...


class _GustoClient(Protocol):
    def request(
        self,
        method: str,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        required: bool = True,
    ) -> _JsonResponse | None: ...


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
        method_token = _normalize_accounting_method(method)
        params = {
            "start_date": f"{year}-01-01",
            "end_date": f"{year}-12-31",
            "accounting_method": method_token,
        }
        cache_key = build_cache_key(
            source="qbo",
            endpoint="/reports/ProfitAndLoss",
            params=params,
            schema="qbo.pnl.v1",
            cache_version=self._cache_version,
        )

        tried_variants: list[dict[str, Any]] = []

        def fetcher() -> dict[str, Any]:
            last_error: Exception | None = None
            for candidate_params in _pnl_param_variants(
                start_date=params["start_date"],
                end_date=params["end_date"],
                accounting_method=method_token,
            ):
                tried_variants.append(dict(candidate_params))
                try:
                    response = self._qbo.request(
                        "GET", "/reports/ProfitAndLoss", params=candidate_params
                    )
                except Exception as exc:
                    if _status_code(exc) == 400:
                        last_error = exc
                        continue
                    raise

                payload = response.json()
                if isinstance(payload, dict):
                    return payload
                raise TypeError("QBO response payload must be an object")

            if last_error is not None:
                variants = ", ".join(_format_variant(variant) for variant in tried_variants)
                raise RuntimeError(
                    "QBO ProfitAndLoss request returned HTTP 400 for all supported parameter "
                    f"variants ({variants}). Verify realm/report permissions and accounting "
                    "method support, then rerun."
                ) from last_error
            raise RuntimeError("QBO ProfitAndLoss request failed before receiving a response.")

        payload, _source = self._store.get_or_fetch(cache_key, fetcher)
        return payload

    def get_balance_sheet(self, year: int, as_of: date | str) -> dict[str, Any]:
        as_of_date = as_of if isinstance(as_of, str) else as_of.isoformat()
        params = {
            "start_date": f"{year}-01-01",
            "end_date": as_of_date,
        }
        return self._cached_qbo_json(
            endpoint="/reports/BalanceSheet", params=params, schema="qbo.balance_sheet.v1"
        )

    def get_general_ledger(self, year: int, month: int) -> dict[str, Any]:
        start_date, end_date = _month_date_range(year=year, month=month)
        params = {
            "start_date": start_date,
            "end_date": end_date,
            "accounting_method": "Accrual",
        }
        return self._cached_qbo_json(
            endpoint="/reports/GeneralLedger",
            params=params,
            schema="qbo.general_ledger.v1",
        )

    def get_general_ledger_with_source(
        self,
        year: int,
        month: int,
    ) -> tuple[dict[str, Any], Literal["cache", "api"]]:
        start_date, end_date = _month_date_range(year=year, month=month)
        params = {
            "start_date": start_date,
            "end_date": end_date,
            "accounting_method": "Accrual",
        }
        payload, source = self._cached_qbo_json_with_source(
            endpoint="/reports/GeneralLedger",
            params=params,
            schema="qbo.general_ledger.v1",
        )
        return payload, source

    def get_payroll_runs(self, year: int) -> list[dict[str, Any]]:
        gusto_client = self._gusto
        if gusto_client is None:
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
            response = gusto_client.request("GET", "/payrolls", params=params, required=False)
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
        realm_id = self._qbo._config.realm_id
        if realm_id is None or not realm_id.strip():
            raise RuntimeError("QBO realm_id is required for company info requests.")
        return self._cached_qbo_json(
            endpoint=f"/companyinfo/{realm_id.strip()}", params={}, schema="qbo.company_info.v1"
        )

    def _cached_qbo_json(
        self,
        *,
        endpoint: str,
        params: dict[str, Any],
        schema: str,
    ) -> dict[str, Any]:
        payload, _source = self._cached_qbo_json_with_source(
            endpoint=endpoint,
            params=params,
            schema=schema,
        )
        return payload

    def _cached_qbo_json_with_source(
        self,
        *,
        endpoint: str,
        params: dict[str, Any],
        schema: str,
    ) -> tuple[dict[str, Any], Literal["cache", "api"]]:
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

        payload, source = self._store.get_or_fetch(cache_key, fetcher)
        normalized_source: Literal["cache", "api"] = "cache" if source == "cache" else "api"
        return payload, normalized_source


def _month_date_range(*, year: int, month: int) -> tuple[str, str]:
    if month < 1 or month > 12:
        raise ValueError("month must be between 1 and 12")
    _, last_day = calendar.monthrange(year, month)
    return f"{year}-{month:02d}-01", f"{year}-{month:02d}-{last_day:02d}"


def _normalize_accounting_method(method: str) -> str:
    normalized = method.strip().lower()
    if normalized == "accrual":
        return "Accrual"
    if normalized == "cash":
        return "Cash"
    return method.strip()


def _pnl_param_variants(
    *,
    start_date: str,
    end_date: str,
    accounting_method: str,
) -> tuple[dict[str, Any], ...]:
    base = {
        "start_date": start_date,
        "end_date": end_date,
    }
    normalized_method = accounting_method.strip()
    lower_method = normalized_method.lower()

    variants: list[dict[str, Any]] = [{**base, "accounting_method": normalized_method}]
    if lower_method and lower_method != normalized_method:
        variants.append({**base, "accounting_method": lower_method})
    variants.append(base)
    return tuple(variants)


def _status_code(error: Exception) -> int | None:
    response = getattr(error, "response", None)
    if response is None:
        return None
    status = getattr(response, "status_code", None)
    if isinstance(status, int):
        return status
    return None


def _format_variant(params: dict[str, Any]) -> str:
    method = params.get("accounting_method")
    if isinstance(method, str) and method.strip():
        return f"accounting_method={method.strip()}"
    return "accounting_method=<omitted>"
