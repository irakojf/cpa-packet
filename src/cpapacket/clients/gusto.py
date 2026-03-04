"""Gusto OAuth client built on shared auth infrastructure."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

from cpapacket.clients.auth import (
    OAuthProviderConfig,
    OAuthToken,
    OAuthTokenStore,
    build_authorization_url,
    generate_pkce_pair,
)

GUSTO_AUTHORIZE_URL = "https://app.gusto.com/oauth/authorize"
GUSTO_TOKEN_URL = "https://api.gusto.com/oauth/token"
GUSTO_API_BASE_URL = "https://api.gusto.com/v1"
GUSTO_SCOPES = ("payrolls:read", "employees:read", "companies:read")


@dataclass(frozen=True, slots=True)
class GustoOAuthConfig:
    client_id: str
    client_secret: str
    redirect_uri: str
    authorize_url: str = GUSTO_AUTHORIZE_URL
    token_url: str = GUSTO_TOKEN_URL
    api_base_url: str = GUSTO_API_BASE_URL


class GustoOAuthClient:
    """Gusto API client with optional-friendly OAuth handling."""

    def __init__(
        self,
        config: GustoOAuthConfig,
        *,
        token_store: OAuthTokenStore | None = None,
        http_client: httpx.Client | None = None,
        timeout: float = 30.0,
    ) -> None:
        self._config = config
        self._token_store = token_store if token_store is not None else OAuthTokenStore("gusto")
        self._http = http_client if http_client is not None else httpx.Client(timeout=timeout)

        self._provider_config = OAuthProviderConfig(
            provider_name="gusto",
            client_id=config.client_id,
            authorize_url=config.authorize_url,
            token_url=config.token_url,
            redirect_uri=config.redirect_uri,
            scopes=GUSTO_SCOPES,
        )

    def authorization_url(self, *, state: str) -> tuple[str, str]:
        """Build login URL and return matching PKCE verifier."""
        pkce = generate_pkce_pair()
        url = build_authorization_url(config=self._provider_config, state=state, pkce=pkce)
        return url, pkce.verifier

    def is_configured(self) -> bool:
        """Return True if Gusto token exists; used for graceful optional behavior."""
        return self._token_store.load_token() is not None

    def exchange_code_for_token(self, *, code: str, code_verifier: str) -> OAuthToken:
        token = self._post_token(
            {
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": self._config.redirect_uri,
                "code_verifier": code_verifier,
                "client_id": self._config.client_id,
                "client_secret": self._config.client_secret,
            }
        )
        self._token_store.save_token(token)
        return token

    def refresh_access_token(self, refresh_token: str) -> OAuthToken:
        token = self._post_token(
            {
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": self._config.client_id,
                "client_secret": self._config.client_secret,
            }
        )
        self._token_store.save_token(token)
        return token

    def get_valid_token(self, *, required: bool = True) -> OAuthToken | None:
        """Load token and refresh if expired; optional mode returns None when missing."""
        token = self._token_store.load_token()
        if token is None:
            if required:
                raise RuntimeError("Gusto token not found. Run `cpapacket auth gusto login`.")
            return None
        if not token.is_expired():
            return token

        with self._token_store.refresh_lock():
            current = self._token_store.load_token()
            if current is None:
                if required:
                    raise RuntimeError("Gusto token not found. Run `cpapacket auth gusto login`.")
                return None
            if not current.is_expired():
                return current
            return self.refresh_access_token(current.refresh_token)

    def request(
        self,
        method: str,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        required: bool = True,
    ) -> httpx.Response | None:
        """Perform authenticated request. Returns None when optional auth is absent."""
        token = self.get_valid_token(required=required)
        if token is None:
            return None

        url = self._build_api_url(endpoint)
        response = self._http.request(
            method=method,
            url=url,
            params=params,
            json=json_body,
            headers=self._auth_headers(token.access_token),
        )
        if response.status_code != 401:
            response.raise_for_status()
            return response

        refreshed = self.refresh_access_token(token.refresh_token)
        retry = self._http.request(
            method=method,
            url=url,
            params=params,
            json=json_body,
            headers=self._auth_headers(refreshed.access_token),
        )
        retry.raise_for_status()
        return retry

    def _post_token(self, form_data: dict[str, str]) -> OAuthToken:
        response = self._http.post(
            self._config.token_url,
            data=form_data,
            headers={"Accept": "application/json"},
        )
        response.raise_for_status()
        payload = response.json()

        expires_in = payload.get("expires_in")
        access_token = payload.get("access_token")
        refresh_token = payload.get("refresh_token")
        token_type = payload.get("token_type", "Bearer")
        scope = payload.get("scope")

        if not isinstance(expires_in, int):
            raise RuntimeError("Gusto token response missing integer expires_in")
        if not isinstance(access_token, str) or not access_token:
            raise RuntimeError("Gusto token response missing access_token")
        if not isinstance(refresh_token, str) or not refresh_token:
            raise RuntimeError("Gusto token response missing refresh_token")

        return OAuthToken.from_token_response(
            access_token=access_token,
            refresh_token=refresh_token,
            expires_in_seconds=expires_in,
            token_type=token_type if isinstance(token_type, str) and token_type else "Bearer",
            scope=scope if isinstance(scope, str) else None,
        )

    def _build_api_url(self, endpoint: str) -> str:
        base = self._config.api_base_url.rstrip("/")
        suffix = endpoint if endpoint.startswith("/") else f"/{endpoint}"
        return f"{base}{suffix}"

    @staticmethod
    def _auth_headers(access_token: str) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}",
        }
