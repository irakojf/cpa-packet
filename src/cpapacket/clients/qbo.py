"""QuickBooks Online OAuth client built on shared auth infrastructure."""

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

QBO_AUTHORIZE_URL = "https://appcenter.intuit.com/connect/oauth2"
QBO_TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
QBO_API_BASE_URL = "https://quickbooks.api.intuit.com/v3/company"
QBO_SCOPES = ("com.intuit.quickbooks.accounting",)


@dataclass(frozen=True, slots=True)
class QboOAuthConfig:
    client_id: str
    client_secret: str
    redirect_uri: str
    realm_id: str | None = None
    authorize_url: str = QBO_AUTHORIZE_URL
    token_url: str = QBO_TOKEN_URL
    api_base_url: str = QBO_API_BASE_URL


class QboOAuthClient:
    """QBO API client with OAuth code exchange, refresh, and bearer requests."""

    def __init__(
        self,
        config: QboOAuthConfig,
        *,
        token_store: OAuthTokenStore | None = None,
        http_client: httpx.Client | None = None,
        timeout: float = 30.0,
    ) -> None:
        self._config = config
        self._token_store = token_store if token_store is not None else OAuthTokenStore("qbo")
        self._http = http_client if http_client is not None else httpx.Client(timeout=timeout)

        self._provider_config = OAuthProviderConfig(
            provider_name="qbo",
            client_id=config.client_id,
            authorize_url=config.authorize_url,
            token_url=config.token_url,
            redirect_uri=config.redirect_uri,
            scopes=QBO_SCOPES,
        )

    def authorization_url(self, *, state: str) -> tuple[str, str]:
        """Build login URL and return the matching code_verifier."""
        pkce = generate_pkce_pair()
        url = build_authorization_url(config=self._provider_config, state=state, pkce=pkce)
        return url, pkce.verifier

    def exchange_code_for_token(self, *, code: str, code_verifier: str) -> OAuthToken:
        """Exchange authorization code for an OAuth token and persist it."""
        token = self._post_token(
            {
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": self._config.redirect_uri,
                "code_verifier": code_verifier,
            }
        )
        self._token_store.save_token(token)
        return token

    def refresh_access_token(self, refresh_token: str) -> OAuthToken:
        """Refresh and persist OAuth token."""
        token = self._post_token(
            {
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
            }
        )
        self._token_store.save_token(token)
        return token

    def get_valid_token(self) -> OAuthToken:
        """Load token and refresh it when expired."""
        token = self._token_store.load_token()
        if token is None:
            raise RuntimeError("QBO token not found. Run `cpapacket auth qbo login`.")
        if not token.is_expired():
            return token

        with self._token_store.refresh_lock():
            current = self._token_store.load_token()
            if current is None:
                raise RuntimeError("QBO token not found. Run `cpapacket auth qbo login`.")
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
    ) -> httpx.Response:
        """Perform an authorized QBO request with one refresh-on-401 retry."""
        token = self.get_valid_token()
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

    def get_company_info(self) -> dict[str, Any]:
        """Fetch QBO company info payload for the configured realm."""
        realm_id = self._config.realm_id
        if realm_id is None or not realm_id.strip():
            raise RuntimeError("QBO realm_id is required for company info requests.")

        response = self.request("GET", f"/companyinfo/{realm_id.strip()}")
        payload = response.json()
        if not isinstance(payload, dict):
            raise TypeError("QBO company info payload must be an object")
        return payload

    def _post_token(self, form_data: dict[str, str]) -> OAuthToken:
        response = self._http.post(
            self._config.token_url,
            data=form_data,
            auth=(self._config.client_id, self._config.client_secret),
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
            raise RuntimeError("QBO token response missing integer expires_in")
        if not isinstance(access_token, str) or not access_token:
            raise RuntimeError("QBO token response missing access_token")
        if not isinstance(refresh_token, str) or not refresh_token:
            raise RuntimeError("QBO token response missing refresh_token")

        return OAuthToken.from_token_response(
            access_token=access_token,
            refresh_token=refresh_token,
            expires_in_seconds=expires_in,
            token_type=token_type if isinstance(token_type, str) and token_type else "Bearer",
            scope=scope if isinstance(scope, str) else None,
        )

    def _build_api_url(self, endpoint: str) -> str:
        if self._config.realm_id is None or not self._config.realm_id.strip():
            raise RuntimeError("QBO realm_id is required for API requests.")
        base = self._config.api_base_url.rstrip("/")
        suffix = endpoint if endpoint.startswith("/") else f"/{endpoint}"
        return f"{base}/{self._config.realm_id}{suffix}"

    @staticmethod
    def _auth_headers(access_token: str) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}",
        }
