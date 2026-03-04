"""Shared OAuth primitives and token persistence for provider clients."""

from __future__ import annotations

import base64
import getpass
import hashlib
import hmac
import json
import platform
import secrets
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import Any, Final, TextIO, cast
from urllib.parse import urlencode

from platformdirs import user_config_path
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from cpapacket.core.filesystem import atomic_write

keyring: Any
KeyringError: type[Exception]
try:
    import keyring
    from keyring.errors import KeyringError
    _KEYRING_AVAILABLE = True
except Exception:  # pragma: no cover - import error path varies by runtime
    keyring = None
    KeyringError = RuntimeError
    _KEYRING_AVAILABLE = False

fcntl: Any
try:
    import fcntl

    _HAS_FCNTL = True
except Exception:  # pragma: no cover - non-POSIX fallback
    fcntl = None
    _HAS_FCNTL = False

_APP_NAME: Final[str] = "cpapacket"
_SERVICE_NAME: Final[str] = "cpapacket.oauth"
_STORED_USERNAME: Final[str] = "default"
_ENCRYPTION_VERSION: Final[int] = 1
_THREAD_LOCKS: dict[str, Lock] = {}
_THREAD_LOCKS_GUARD = Lock()


class OAuthToken(BaseModel):
    """OAuth token payload with explicit expiry semantics."""

    model_config = ConfigDict(frozen=True)

    access_token: str = Field(min_length=1)
    refresh_token: str = Field(min_length=1)
    token_type: str = Field(default="Bearer", min_length=1)
    expires_at: datetime
    scope: str | None = None

    def is_expired(self, *, leeway_seconds: int = 60) -> bool:
        """Return True when token is expired or within the leeway window."""
        now = datetime.now(UTC)
        normalized = self.expires_at.astimezone(UTC)
        return normalized <= now + timedelta(seconds=leeway_seconds)

    @classmethod
    def from_token_response(
        cls,
        *,
        access_token: str,
        refresh_token: str,
        expires_in_seconds: int,
        token_type: str = "Bearer",
        scope: str | None = None,
        issued_at: datetime | None = None,
    ) -> OAuthToken:
        """Build a token model from an OAuth token endpoint response."""
        base = issued_at.astimezone(UTC) if issued_at is not None else datetime.now(UTC)
        if expires_in_seconds <= 0:
            raise ValueError("expires_in_seconds must be > 0")
        return cls(
            access_token=access_token,
            refresh_token=refresh_token,
            token_type=token_type,
            expires_at=base + timedelta(seconds=expires_in_seconds),
            scope=scope,
        )


@dataclass(frozen=True, slots=True)
class OAuthProviderConfig:
    """Provider-specific OAuth configuration used by shared auth helpers."""

    provider_name: str
    client_id: str
    authorize_url: str
    token_url: str
    redirect_uri: str
    scopes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PkcePair:
    """PKCE verifier/challenge pair for Authorization Code flow."""

    verifier: str
    challenge: str


def generate_pkce_pair() -> PkcePair:
    """Generate a PKCE code verifier and corresponding S256 code challenge."""
    verifier = _urlsafe_token(64)
    challenge = _base64url_encode(hashlib.sha256(verifier.encode("ascii")).digest())
    return PkcePair(verifier=verifier, challenge=challenge)


def build_authorization_url(
    *,
    config: OAuthProviderConfig,
    state: str,
    pkce: PkcePair,
    extra_params: dict[str, str] | None = None,
) -> str:
    """Build Authorization Code + PKCE URL for interactive login."""
    params: dict[str, str] = {
        "response_type": "code",
        "client_id": config.client_id,
        "redirect_uri": config.redirect_uri,
        "scope": " ".join(config.scopes),
        "state": state,
        "code_challenge": pkce.challenge,
        "code_challenge_method": "S256",
    }
    if extra_params:
        params.update(extra_params)
    return f"{config.authorize_url}?{urlencode(params)}"


class OAuthTokenStore:
    """Keyring-first token persistence with encrypted JSON fallback."""

    def __init__(self, provider_name: str, *, config_root: Path | None = None) -> None:
        if not provider_name.strip():
            raise ValueError("provider_name must not be blank")

        sanitized = provider_name.strip().lower().replace(" ", "_")
        root = config_root if config_root is not None else user_config_path(_APP_NAME, _APP_NAME)
        self._provider_name = sanitized
        self._service_name = f"{_SERVICE_NAME}.{sanitized}"
        self._config_root = Path(root)
        self._fallback_path = self._config_root / "auth" / f"{sanitized}_token.enc.json"
        self._lock_path = self._config_root / "auth" / f"{sanitized}.refresh.lock"

    def load_token(self) -> OAuthToken | None:
        """Load provider token from keyring, then encrypted fallback."""
        payload = self._load_from_keyring()
        if payload is not None:
            return self._parse_token(payload)

        payload = self._load_from_encrypted_fallback()
        if payload is None:
            return None
        return self._parse_token(payload)

    def save_token(self, token: OAuthToken) -> None:
        """Persist token to keyring when possible, otherwise encrypted fallback."""
        payload = token.model_dump_json()

        if self._save_to_keyring(payload):
            return
        self._save_to_encrypted_fallback(payload)

    def clear_token(self) -> None:
        """Clear keyring token and mark fallback token as revoked."""
        if _KEYRING_AVAILABLE and keyring is not None:
            with suppress(Exception):
                keyring.delete_password(self._service_name, _STORED_USERNAME)

        revoked_payload = json.dumps({"revoked": True, "provider": self._provider_name})
        self._save_to_encrypted_fallback(revoked_payload)

    @contextmanager
    def refresh_lock(self) -> Any:
        """Serialize token refreshes across threads/processes for a provider."""
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        thread_lock = _provider_thread_lock(self._provider_name)
        with thread_lock, open(self._lock_path, "a+", encoding="utf-8") as handle:
            if _HAS_FCNTL and fcntl is not None:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                if _HAS_FCNTL and fcntl is not None:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def _save_to_keyring(self, payload: str) -> bool:
        if not _KEYRING_AVAILABLE or keyring is None:
            return False

        try:
            keyring.set_password(self._service_name, _STORED_USERNAME, payload)
            return True
        except (KeyringError, RuntimeError):
            return False

    def _load_from_keyring(self) -> str | None:
        if not _KEYRING_AVAILABLE or keyring is None:
            return None

        try:
            return cast(str | None, keyring.get_password(self._service_name, _STORED_USERNAME))
        except (KeyringError, RuntimeError):
            return None

    def _save_to_encrypted_fallback(self, payload: str) -> None:
        envelope = _encrypt_payload(payload, self._provider_name)
        with atomic_write(self._fallback_path) as handle:
            cast(TextIO, handle).write(json.dumps(envelope, sort_keys=True))

    def _load_from_encrypted_fallback(self) -> str | None:
        if not self._fallback_path.exists():
            return None
        ciphertext = self._fallback_path.read_text(encoding="utf-8")
        return _decrypt_payload(ciphertext, self._provider_name)

    def _parse_token(self, payload: str) -> OAuthToken | None:
        try:
            parsed = json.loads(payload)
            if isinstance(parsed, dict) and parsed.get("revoked"):
                return None
        except json.JSONDecodeError:
            return None

        try:
            return OAuthToken.model_validate_json(payload)
        except ValidationError:
            return None


def _provider_thread_lock(provider_name: str) -> Lock:
    with _THREAD_LOCKS_GUARD:
        lock = _THREAD_LOCKS.get(provider_name)
        if lock is None:
            lock = Lock()
            _THREAD_LOCKS[provider_name] = lock
        return lock


def _urlsafe_token(length: int) -> str:
    token = base64.urlsafe_b64encode(secrets.token_bytes(length)).decode("ascii")
    return token.rstrip("=")


def _derive_key(provider_name: str, nonce: bytes) -> bytes:
    seed = "|".join(
        [
            _APP_NAME,
            provider_name,
            getpass.getuser(),
            platform.node(),
            str(Path.home()),
        ]
    ).encode("utf-8")
    return hashlib.pbkdf2_hmac("sha256", seed, nonce, 120_000, dklen=32)


def _base64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _base64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def _keystream(key: bytes, nonce: bytes, size: int) -> bytes:
    output = bytearray()
    counter = 0
    while len(output) < size:
        block = hashlib.sha256(key + nonce + counter.to_bytes(8, "big")).digest()
        output.extend(block)
        counter += 1
    return bytes(output[:size])


def _encrypt_payload(payload: str, provider_name: str) -> dict[str, str | int]:
    nonce = secrets.token_bytes(16)
    key = _derive_key(provider_name, nonce)
    plaintext = payload.encode("utf-8")
    stream = _keystream(key, nonce, len(plaintext))
    ciphertext = bytes(p ^ k for p, k in zip(plaintext, stream, strict=True))
    mac = hmac.new(key, nonce + ciphertext, hashlib.sha256).digest()
    return {
        "v": _ENCRYPTION_VERSION,
        "nonce": _base64url_encode(nonce),
        "ciphertext": _base64url_encode(ciphertext),
        "mac": _base64url_encode(mac),
    }


def _decrypt_payload(payload: str, provider_name: str) -> str | None:
    try:
        envelope = json.loads(payload)
        if not isinstance(envelope, dict):
            return None
        if envelope.get("v") != _ENCRYPTION_VERSION:
            return None

        nonce_value = envelope.get("nonce")
        ciphertext_value = envelope.get("ciphertext")
        mac_value = envelope.get("mac")
        if (
            not isinstance(nonce_value, str)
            or not isinstance(ciphertext_value, str)
            or not isinstance(mac_value, str)
        ):
            return None

        nonce = _base64url_decode(nonce_value)
        ciphertext = _base64url_decode(ciphertext_value)
        mac = _base64url_decode(mac_value)
    except (json.JSONDecodeError, ValueError, TypeError):
        return None

    key = _derive_key(provider_name, nonce)
    expected_mac = hmac.new(key, nonce + ciphertext, hashlib.sha256).digest()
    if not hmac.compare_digest(mac, expected_mac):
        return None

    stream = _keystream(key, nonce, len(ciphertext))
    plaintext = bytes(c ^ k for c, k in zip(ciphertext, stream, strict=True))
    try:
        return plaintext.decode("utf-8")
    except UnicodeDecodeError:
        return None
