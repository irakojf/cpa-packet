from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest

from cpapacket.clients.auth import OAuthToken
from cpapacket.clients.gusto import GustoOAuthClient, GustoOAuthConfig


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

    def post(self, url: str, *, data: dict[str, str], headers: dict[str, str]) -> FakeResponse:
        self.post_calls.append({"url": url, "data": data, "headers": headers})
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


def test_is_configured_false_without_token() -> None:
    client = GustoOAuthClient(
        GustoOAuthConfig(
            client_id="cid",
            client_secret="secret",
            redirect_uri="http://localhost/callback",
        ),
        token_store=InMemoryTokenStore(token=None),
        http_client=FakeHttpClient(),
    )
    assert not client.is_configured()


def test_optional_request_returns_none_when_missing_token() -> None:
    client = GustoOAuthClient(
        GustoOAuthConfig(
            client_id="cid",
            client_secret="secret",
            redirect_uri="http://localhost/callback",
        ),
        token_store=InMemoryTokenStore(token=None),
        http_client=FakeHttpClient(),
    )

    response = client.request("GET", "/companies", required=False)
    assert response is None


def test_required_request_raises_when_missing_token() -> None:
    client = GustoOAuthClient(
        GustoOAuthConfig(
            client_id="cid",
            client_secret="secret",
            redirect_uri="http://localhost/callback",
        ),
        token_store=InMemoryTokenStore(token=None),
        http_client=FakeHttpClient(),
    )
    with pytest.raises(RuntimeError, match="auth gusto login"):
        client.request("GET", "/companies", required=True)


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
            },
        )
    )
    store = InMemoryTokenStore()

    client = GustoOAuthClient(
        GustoOAuthConfig(
            client_id="cid",
            client_secret="secret",
            redirect_uri="http://localhost/callback",
        ),
        token_store=store,
        http_client=http_client,
    )

    token = client.exchange_code_for_token(code="code-1", code_verifier="verifier-1")

    assert token.access_token == "new-access"
    assert token.refresh_token == "new-refresh"
    assert len(store.saved) == 1
    assert http_client.post_calls[0]["data"]["grant_type"] == "authorization_code"


def test_request_retries_once_after_401() -> None:
    # Use a non-expired token so get_valid_token() returns it without refreshing.
    # The server will reject it with 401, triggering the refresh-and-retry path.
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
            },
        )
    )
    http_client.request_queue.append(FakeResponse(401, {}))
    http_client.request_queue.append(FakeResponse(200, {"ok": True}))

    client = GustoOAuthClient(
        GustoOAuthConfig(
            client_id="cid",
            client_secret="secret",
            redirect_uri="http://localhost/callback",
        ),
        token_store=store,
        http_client=http_client,
    )

    response = client.request("GET", "/companies")
    assert response is not None
    assert response.status_code == 200
    assert len(http_client.request_calls) == 2
    assert http_client.request_calls[1]["headers"]["Authorization"] == "Bearer fresh-access"
