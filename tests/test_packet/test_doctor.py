from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from cpapacket.clients.auth import OAuthToken
from cpapacket.packet.doctor import run_python_environment_check, run_qbo_token_check


def test_run_python_environment_check_passes_when_version_and_packages_present(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr("cpapacket.packet.doctor.find_spec", lambda _name: object())

    result = run_python_environment_check(
        min_version=(3, 11),
        required_modules=("click", "pydantic"),
        version_info=(3, 11, 9),
    )

    assert result.status == "pass"
    assert result.guidance is None
    assert result.summary == "Python environment check passed."


def test_run_python_environment_check_fails_for_old_python_version() -> None:
    result = run_python_environment_check(
        min_version=(3, 11),
        required_modules=(),
        version_info=(3, 10, 14),
    )

    assert result.status == "fail"
    assert "below required version 3.11" in result.summary
    assert result.guidance == "Install Python 3.11+ and rerun `cpapacket doctor`."


def test_run_python_environment_check_fails_when_required_packages_missing(
    monkeypatch: Any,
) -> None:
    missing = {"reportlab", "keyring"}

    def fake_find_spec(name: str) -> object | None:
        if name in missing:
            return None
        return object()

    monkeypatch.setattr("cpapacket.packet.doctor.find_spec", fake_find_spec)

    result = run_python_environment_check(
        min_version=(3, 11),
        required_modules=("click", "reportlab", "keyring"),
        version_info=(3, 11, 1),
    )

    assert result.status == "fail"
    assert result.summary == "Required Python packages are missing."
    assert "missing=keyring,reportlab" in result.details
    assert result.guidance == "Install project dependencies and rerun `cpapacket doctor`."


def _token_with_expiry(seconds_from_now: int) -> OAuthToken:
    return OAuthToken(
        access_token="access",
        refresh_token="refresh",
        token_type="Bearer",
        expires_at=datetime.now(UTC) + timedelta(seconds=seconds_from_now),
    )


def test_run_qbo_token_check_fails_when_token_missing() -> None:
    token = _token_with_expiry(3600)
    result = run_qbo_token_check(
        load_token=lambda: None,
        refresh_probe=lambda _refresh_token: token,
    )

    assert result.status == "fail"
    assert result.summary == "QBO token not found."
    assert result.guidance == "Run `cpapacket auth qbo login` and rerun `cpapacket doctor`."


def test_run_qbo_token_check_passes_when_refresh_probe_succeeds() -> None:
    token = _token_with_expiry(3600)

    def refresh_probe(refresh_token: str) -> OAuthToken:
        assert refresh_token == "refresh"
        return token

    result = run_qbo_token_check(load_token=lambda: token, refresh_probe=refresh_probe)

    assert result.status == "pass"
    assert result.summary == "QBO token check passed."
    assert "expired=false" in result.details
    assert "refresh_probe=ok" in result.details


def test_run_qbo_token_check_fails_when_refresh_probe_errors() -> None:
    token = _token_with_expiry(-120)

    def refresh_probe(_refresh_token: str) -> OAuthToken:
        raise RuntimeError("invalid_grant")

    result = run_qbo_token_check(load_token=lambda: token, refresh_probe=refresh_probe)

    assert result.status == "fail"
    assert result.summary == "QBO token refresh probe failed."
    assert any(item == "expired=true" for item in result.details)
    assert any(item == "refresh_error=invalid_grant" for item in result.details)
    assert result.guidance == "Re-authenticate with `cpapacket auth qbo login` and retry doctor."
