from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from cpapacket.clients.auth import (
    OAuthProviderConfig,
    OAuthToken,
    OAuthTokenStore,
    PkcePair,
    build_authorization_url,
    generate_pkce_pair,
)


def test_generate_pkce_pair_shapes() -> None:
    pair = generate_pkce_pair()
    assert isinstance(pair, PkcePair)
    assert len(pair.verifier) >= 43
    assert len(pair.challenge) >= 43
    assert "=" not in pair.verifier
    assert "=" not in pair.challenge


def test_build_authorization_url_includes_required_params() -> None:
    config = OAuthProviderConfig(
        provider_name="qbo",
        client_id="client-123",
        authorize_url="https://example.com/oauth/authorize",
        token_url="https://example.com/oauth/token",
        redirect_uri="http://localhost:8765/callback",
        scopes=("com.intuit.quickbooks.accounting", "openid"),
    )
    pkce = PkcePair(verifier="verifier", challenge="challenge")

    url = build_authorization_url(config=config, state="state-abc", pkce=pkce)
    parsed = urlparse(url)
    query = parse_qs(parsed.query)

    assert parsed.scheme == "https"
    assert parsed.netloc == "example.com"
    assert query["response_type"] == ["code"]
    assert query["client_id"] == ["client-123"]
    assert query["redirect_uri"] == ["http://localhost:8765/callback"]
    assert query["state"] == ["state-abc"]
    assert query["code_challenge"] == ["challenge"]
    assert query["code_challenge_method"] == ["S256"]
    assert query["scope"] == ["com.intuit.quickbooks.accounting openid"]


def test_oauth_token_expiry_with_leeway() -> None:
    soon = datetime.now(UTC) + timedelta(seconds=30)
    later = datetime.now(UTC) + timedelta(minutes=10)

    token_soon = OAuthToken(
        access_token="access",
        refresh_token="refresh",
        expires_at=soon,
    )
    token_later = OAuthToken(
        access_token="access",
        refresh_token="refresh",
        expires_at=later,
    )

    assert token_soon.is_expired(leeway_seconds=60)
    assert not token_later.is_expired(leeway_seconds=60)


def test_oauth_token_store_fallback_round_trip(tmp_path: Path, monkeypatch) -> None:
    # Force fallback path even when keyring is installed.
    monkeypatch.setattr("cpapacket.clients.auth._KEYRING_AVAILABLE", False)

    store = OAuthTokenStore("qbo", config_root=tmp_path)
    token = OAuthToken.from_token_response(
        access_token="access-1",
        refresh_token="refresh-1",
        expires_in_seconds=3600,
    )

    store.save_token(token)
    loaded = store.load_token()

    assert loaded is not None
    assert loaded.access_token == "access-1"
    assert loaded.refresh_token == "refresh-1"
    assert loaded.token_type == "Bearer"
    assert loaded.expires_at > datetime.now(UTC)


def test_oauth_token_store_clear_writes_revoked_marker(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("cpapacket.clients.auth._KEYRING_AVAILABLE", False)

    store = OAuthTokenStore("gusto", config_root=tmp_path)
    token = OAuthToken.from_token_response(
        access_token="access-2",
        refresh_token="refresh-2",
        expires_in_seconds=3600,
    )
    store.save_token(token)
    assert store.load_token() is not None

    store.clear_token()
    assert store.load_token() is None


def test_oauth_token_store_save_mirrors_fallback_when_keyring_write_succeeds(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr("cpapacket.clients.auth._KEYRING_AVAILABLE", False)

    store = OAuthTokenStore("qbo", config_root=tmp_path)
    store.clear_token()
    assert store.load_token() is None

    monkeypatch.setattr(store, "_save_to_keyring", lambda payload: True)
    monkeypatch.setattr(store, "_load_from_keyring", lambda: None)

    token = OAuthToken.from_token_response(
        access_token="access-3",
        refresh_token="refresh-3",
        expires_in_seconds=3600,
    )
    store.save_token(token)
    loaded = store.load_token()

    assert loaded is not None
    assert loaded.access_token == "access-3"
    assert loaded.refresh_token == "refresh-3"


def test_refresh_lock_yields_without_error(tmp_path: Path) -> None:
    store = OAuthTokenStore("qbo", config_root=tmp_path)

    with store.refresh_lock():
        marker = tmp_path / "lock_marker.txt"
        marker.write_text("locked", encoding="utf-8")

    assert marker.read_text(encoding="utf-8") == "locked"
