from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest

from cpapacket.clients.auth import OAuthToken
from cpapacket.clients.qbo import QboOAuthClient, QboOAuthConfig


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
    def __init__(self, status_code: int, payload: dict[str, Any]) -> None:
        self.status_code = status_code
        self._payload = payload

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

    def post(self, url: str, *, data: dict[str, str], auth: tuple[str, str], headers: dict[str, str]) -> FakeResponse:
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
    expired = _token(-10)
    fresh = _token(3600)
    fresh = fresh.model_copy(update={"access_token": "fresh-access", "refresh_token": "fresh-refresh"})
    store = InMemoryTokenStore(token=expired)

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
    assert http_client.request_calls[0]["headers"]["Authorization"] == "Bearer fresh-access"
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
