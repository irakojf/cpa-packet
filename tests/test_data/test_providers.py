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


class _FakeErrorResponse:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code


class _FakeHttpStatusError(RuntimeError):
    def __init__(self, status_code: int) -> None:
        super().__init__(f"HTTP {status_code}")
        self.response = _FakeErrorResponse(status_code)


class _FakeQboClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self._config = type("Config", (), {"realm_id": "test-realm"})()

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


@pytest.mark.parametrize(
    ("raw_method", "expected"),
    [("accrual", "Accrual"), ("cash", "Cash"), ("Accrual", "Accrual")],
)
def test_get_pnl_normalizes_accounting_method_for_qbo(raw_method: str, expected: str) -> None:
    store = SessionDataStore()
    qbo = _FakeQboClient()
    providers = DataProviders(store=store, qbo_client=qbo)

    providers.get_pnl(2025, raw_method)

    assert qbo.calls[0]["params"]["accounting_method"] == expected


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
    assert payload["params"]["start_date"] == "2025-01-01"
    assert payload["params"]["end_date"] == "2025-12-31"


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


def test_get_accounts_and_company_info_use_expected_endpoints() -> None:
    store = SessionDataStore()
    qbo = _FakeQboClient()
    providers = DataProviders(store=store, qbo_client=qbo)

    accounts = providers.get_accounts()
    company = providers.get_company_info()

    assert accounts["endpoint"] == "/query"
    assert accounts["params"]["query"] == "select * from Account"
    assert company["endpoint"] == "/companyinfo/test-realm"
    assert company["params"] == {}


def test_provider_layer_generates_cache_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    store = SessionDataStore()
    qbo = _FakeQboClient()
    providers = DataProviders(store=store, qbo_client=qbo)
    calls: list[dict[str, Any]] = []

    def fake_build_cache_key(
        *,
        source: str,
        endpoint: str,
        params: dict[str, Any],
        schema: str,
        cache_version: str,
    ) -> str:
        calls.append(
            {
                "source": source,
                "endpoint": endpoint,
                "params": params,
                "schema": schema,
                "cache_version": cache_version,
            }
        )
        return f"fake:{source}:{endpoint}:{schema}"

    monkeypatch.setattr("cpapacket.data.providers.build_cache_key", fake_build_cache_key)
    providers.get_pnl(2025, "Accrual")
    providers.get_company_info()

    assert calls[0]["source"] == "qbo"
    assert calls[0]["endpoint"] == "/reports/ProfitAndLoss"
    assert calls[0]["schema"] == "qbo.pnl.v1"
    assert calls[1]["endpoint"] == "/companyinfo/test-realm"
    assert calls[1]["schema"] == "qbo.company_info.v1"


def test_provider_propagates_qbo_payload_type_error() -> None:
    class _InvalidQboClient(_FakeQboClient):
        def request(
            self,
            method: str,
            endpoint: str,
            *,
            params: dict[str, Any] | None = None,
            json_body: dict[str, Any] | None = None,
        ) -> _FakeResponse:
            del method, endpoint, params, json_body
            return _FakeResponse(["not-a-dict"])

    store = SessionDataStore()
    qbo = _InvalidQboClient()
    providers = DataProviders(store=store, qbo_client=qbo)

    with pytest.raises(TypeError, match="payload must be an object"):
        providers.get_pnl(2025, "Accrual")


def test_get_pnl_integration_cache_hit_avoids_duplicate_http_calls() -> None:
    httpx = pytest.importorskip("httpx")
    respx = pytest.importorskip("respx")

    class _HttpQboClient:
        def __init__(self, client: Any) -> None:
            self._client = client

        def request(
            self,
            method: str,
            endpoint: str,
            *,
            params: dict[str, Any] | None = None,
            json_body: dict[str, Any] | None = None,
        ) -> object:
            return self._client.request(
                method,
                f"https://api.example.test{endpoint}",
                params=params,
                json=json_body,
            )

    store = SessionDataStore()
    with httpx.Client() as client:
        qbo = _HttpQboClient(client)
        providers = DataProviders(store=store, qbo_client=qbo)

        with respx.mock(assert_all_called=True) as router:
            route = router.get("https://api.example.test/reports/ProfitAndLoss").mock(
                return_value=httpx.Response(200, json={"Rows": {"Row": []}}),
            )

            first = providers.get_pnl(2025, "Accrual")
            second = providers.get_pnl(2025, "Accrual")

    assert first == {"Rows": {"Row": []}}
    assert second == first
    assert route.call_count == 1


def test_get_pnl_retries_400_with_lowercase_then_succeeds() -> None:
    class _VariantQboClient(_FakeQboClient):
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
            accounting_method = (params or {}).get("accounting_method")
            if accounting_method == "Accrual":
                raise _FakeHttpStatusError(400)
            return _FakeResponse({"Rows": {"Row": []}, "accounting_method": accounting_method})

    store = SessionDataStore()
    qbo = _VariantQboClient()
    providers = DataProviders(store=store, qbo_client=qbo)

    payload = providers.get_pnl(2025, "Accrual")

    assert payload["Rows"] == {"Row": []}
    assert payload["accounting_method"] == "accrual"
    assert [call["params"].get("accounting_method") for call in qbo.calls] == [
        "Accrual",
        "accrual",
    ]


def test_get_pnl_raises_actionable_error_when_all_400_variants_fail() -> None:
    class _Always400QboClient(_FakeQboClient):
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
            raise _FakeHttpStatusError(400)

    store = SessionDataStore()
    qbo = _Always400QboClient()
    providers = DataProviders(store=store, qbo_client=qbo)

    with pytest.raises(RuntimeError, match="all supported parameter variants"):
        providers.get_pnl(2025, "Accrual")

    assert [call["params"].get("accounting_method") for call in qbo.calls] == [
        "Accrual",
        "accrual",
        None,
    ]
