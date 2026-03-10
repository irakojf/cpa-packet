from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest

from cpapacket.clients.auth import OAuthToken
from cpapacket.clients.qbo import QboOAuthClient, QboOAuthConfig
from cpapacket.core.retry import RetryPolicy


class InMemoryTokenStore:
    def __init__(self, token: OAuthToken | None = None) -> None:
        self.token = token
        self.saved: list[OAuthToken] = []

    def load_token(self) -> OAuthToken | None:
        return self.token

    def save_token(self, token: OAuthToken) -> None:
        self.saved.append(token)
        self.token = token

    def refresh_lock(self):  # noqa: ANN201
        class _NoopContext:
            def __enter__(self) -> None:
                return None

            def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
                return None

        return _NoopContext()


class FakeResponse:
    def __init__(
        self,
        status_code: int,
        payload: dict[str, Any],
        headers: Mapping[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self._payload = payload
        self.headers = dict(headers or {})

    def json(self) -> dict[str, Any]:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise httpx.HTTPStatusError(
                f"HTTP {self.status_code}",
                request=httpx.Request("GET", "https://example.com"),
                response=httpx.Response(self.status_code),
            )


class FakeHttpClient:
    def __init__(self) -> None:
        self.post_calls: list[dict[str, Any]] = []
        self.request_calls: list[dict[str, Any]] = []
        self.post_queue: list[FakeResponse] = []
        self.request_queue: list[FakeResponse] = []

    def post(
        self, url: str, *, data: dict[str, str], auth: tuple[str, str], headers: dict[str, str]
    ) -> FakeResponse:
        self.post_calls.append({"url": url, "data": data, "auth": auth, "headers": headers})
        return self.post_queue.pop(0)

    def request(
        self,
        *,
        method: str,
        url: str,
        params: dict[str, Any] | None,
        json: dict[str, Any] | None,
        headers: dict[str, str],
    ) -> FakeResponse:
        self.request_calls.append(
            {"method": method, "url": url, "params": params, "json": json, "headers": headers}
        )
        return self.request_queue.pop(0)


def _token(expiry_delta_seconds: int) -> OAuthToken:
    return OAuthToken(
        access_token="access",
        refresh_token="refresh",
        expires_at=datetime.now(UTC) + timedelta(seconds=expiry_delta_seconds),
    )


def test_authorization_url_returns_verifier() -> None:
    client = QboOAuthClient(
        QboOAuthConfig(
            client_id="cid",
            client_secret="secret",
            redirect_uri="http://localhost:8765/callback",
        ),
        token_store=InMemoryTokenStore(),
        http_client=FakeHttpClient(),
    )

    url, verifier = client.authorization_url(state="state-1")

    assert "code_challenge=" in url
    assert "state=state-1" in url
    assert len(verifier) >= 43


def test_qbo_config_uses_env_api_base_url_override(monkeypatch: Any) -> None:
    monkeypatch.setenv(
        "CPAPACKET_QBO_API_BASE_URL",
        "https://sandbox-quickbooks.api.intuit.com/v3/company",
    )

    config = QboOAuthConfig(
        client_id="cid",
        client_secret="secret",
        redirect_uri="http://localhost:8765/callback",
    )

    assert config.api_base_url == "https://sandbox-quickbooks.api.intuit.com/v3/company"


def test_exchange_code_for_token_saves_token() -> None:
    http_client = FakeHttpClient()
    http_client.post_queue.append(
        FakeResponse(
            200,
            {
                "access_token": "new-access",
                "refresh_token": "new-refresh",
                "expires_in": 3600,
                "token_type": "Bearer",
                "scope": "com.intuit.quickbooks.accounting",
            },
        )
    )
    store = InMemoryTokenStore()
    client = QboOAuthClient(
        QboOAuthConfig(
            client_id="cid",
            client_secret="secret",
            redirect_uri="http://localhost/callback",
        ),
        token_store=store,
        http_client=http_client,
    )

    token = client.exchange_code_for_token(code="abc123", code_verifier="verifier-1")

    assert token.access_token == "new-access"
    assert token.refresh_token == "new-refresh"
    assert len(store.saved) == 1
    assert http_client.post_calls[0]["data"]["grant_type"] == "authorization_code"


def test_request_retries_once_after_401() -> None:
    # Use a non-expired token so get_valid_token() returns it without refreshing.
    # The server rejects it with 401, triggering the refresh-and-retry path.
    valid_looking = _token(300)
    store = InMemoryTokenStore(token=valid_looking)

    http_client = FakeHttpClient()
    http_client.post_queue.append(
        FakeResponse(
            200,
            {
                "access_token": "fresh-access",
                "refresh_token": "fresh-refresh",
                "expires_in": 3600,
                "token_type": "Bearer",
            },
        )
    )
    http_client.request_queue.append(FakeResponse(401, {}))
    http_client.request_queue.append(FakeResponse(200, {"ok": True}))

    client = QboOAuthClient(
        QboOAuthConfig(
            client_id="cid",
            client_secret="secret",
            redirect_uri="http://localhost/callback",
            realm_id="12345",
        ),
        token_store=store,
        http_client=http_client,
    )

    response = client.request("GET", "/query", params={"q": "select 1"})

    assert response.status_code == 200
    assert len(http_client.request_calls) == 2
    # First request uses original token, gets 401
    assert http_client.request_calls[0]["headers"]["Authorization"] == "Bearer access"
    # After refresh, second request uses fresh token
    assert http_client.request_calls[1]["headers"]["Authorization"] == "Bearer fresh-access"


def test_request_requires_realm_id() -> None:
    store = InMemoryTokenStore(token=_token(3600))
    client = QboOAuthClient(
        QboOAuthConfig(
            client_id="cid",
            client_secret="secret",
            redirect_uri="http://localhost/callback",
            realm_id=None,
        ),
        token_store=store,
        http_client=FakeHttpClient(),
    )

    with pytest.raises(RuntimeError, match="realm_id is required"):
        client.request("GET", "/companyinfo")


def test_get_company_info_uses_companyinfo_endpoint() -> None:
    store = InMemoryTokenStore(token=_token(3600))
    http_client = FakeHttpClient()
    http_client.request_queue.append(
        FakeResponse(200, {"CompanyInfo": {"CompanyName": "Example Co"}})
    )
    client = QboOAuthClient(
        QboOAuthConfig(
            client_id="cid",
            client_secret="secret",
            redirect_uri="http://localhost/callback",
            realm_id="12345",
        ),
        token_store=store,
        http_client=http_client,
    )

    payload = client.get_company_info()

    assert payload["CompanyInfo"]["CompanyName"] == "Example Co"
    assert len(http_client.request_calls) == 1
    assert http_client.request_calls[0]["url"].endswith("/12345/companyinfo/12345")


def test_get_company_info_requires_realm_id() -> None:
    client = QboOAuthClient(
        QboOAuthConfig(
            client_id="cid",
            client_secret="secret",
            redirect_uri="http://localhost/callback",
            realm_id=None,
        ),
        token_store=InMemoryTokenStore(token=_token(3600)),
        http_client=FakeHttpClient(),
    )

    with pytest.raises(RuntimeError, match="realm_id is required"):
        client.get_company_info()


def test_request_retries_429_with_retry_after_then_succeeds() -> None:
    store = InMemoryTokenStore(token=_token(3600))
    http_client = FakeHttpClient()
    http_client.request_queue.append(
        FakeResponse(429, {"fault": "rate limit"}, headers={"Retry-After": "2"})
    )
    http_client.request_queue.append(FakeResponse(200, {"ok": True}))
    slept: list[float] = []
    client = QboOAuthClient(
        QboOAuthConfig(
            client_id="cid",
            client_secret="secret",
            redirect_uri="http://localhost/callback",
            realm_id="12345",
        ),
        token_store=store,
        http_client=http_client,
        retry_policy=RetryPolicy(max_429=1, max_5xx=0, base_delay_seconds=0.5, jitter_ratio=0.0),
        sleep_fn=lambda seconds: slept.append(seconds),
        rand_fn=lambda: 0.5,
    )

    response = client.request("GET", "/query", params={"q": "select 1"})

    assert response.status_code == 200
    assert len(http_client.request_calls) == 2
    assert slept == [2.0]


def test_request_raises_http_error_when_429_retry_budget_exhausted() -> None:
    store = InMemoryTokenStore(token=_token(3600))
    http_client = FakeHttpClient()
    http_client.request_queue.append(FakeResponse(429, {"fault": "rate limit"}))
    http_client.request_queue.append(FakeResponse(429, {"fault": "rate limit"}))
    slept: list[float] = []
    client = QboOAuthClient(
        QboOAuthConfig(
            client_id="cid",
            client_secret="secret",
            redirect_uri="http://localhost/callback",
            realm_id="12345",
        ),
        token_store=store,
        http_client=http_client,
        retry_policy=RetryPolicy(max_429=1, max_5xx=0, base_delay_seconds=1.0, jitter_ratio=0.0),
        sleep_fn=lambda seconds: slept.append(seconds),
        rand_fn=lambda: 0.5,
    )

    with pytest.raises(httpx.HTTPStatusError):
        client.request("GET", "/query", params={"q": "select 1"})

    assert len(http_client.request_calls) == 2
    assert slept == [1.0]
