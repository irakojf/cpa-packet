"""Doctor command checks shared across CLI wrappers."""

from __future__ import annotations

import sys
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from importlib.util import find_spec
from typing import Literal, Protocol

from cpapacket.clients.auth import OAuthToken

CheckStatus = Literal["pass", "fail"]

DEFAULT_REQUIRED_MODULES: tuple[str, ...] = (
    "click",
    "pydantic",
    "httpx",
    "reportlab",
    "rich",
    "keyring",
    "platformdirs",
)


@dataclass(frozen=True, slots=True)
class DoctorCheckResult:
    """Structured output for one doctor check result."""

    check_name: str
    status: CheckStatus
    summary: str
    details: list[str] = field(default_factory=list)
    guidance: str | None = None


class QboTokenLoader(Protocol):
    def __call__(self) -> OAuthToken | None:
        """Load the currently stored QBO token."""


class QboTokenRefreshProbe(Protocol):
    def __call__(self, refresh_token: str) -> OAuthToken:
        """Validate refresh token flow and return refreshed token."""


class QboCompanyInfoProbe(Protocol):
    def __call__(self) -> dict[str, object]:
        """Fetch lightweight company info from QBO."""


class GustoCompanyIdentityProbe(Protocol):
    def __call__(self) -> dict[str, object]:
        """Fetch lightweight company identity from Gusto."""


def run_qbo_connectivity_check(
    *,
    company_info_probe: QboCompanyInfoProbe,
) -> DoctorCheckResult:
    """Validate QBO API connectivity using a lightweight company-info call."""
    try:
        payload = company_info_probe()
    except Exception as exc:
        guidance = "Verify network access and QBO credentials, then rerun `cpapacket doctor`."
        error_text = str(exc).lower()
        if "403" in error_text and "quickbooks.api.intuit.com" in error_text:
            guidance = (
                "Verify CPAPACKET_QBO_REALM_ID matches the authorized company and app environment. "
                "For sandbox companies, set "
                "`CPAPACKET_QBO_API_BASE_URL=https://sandbox-quickbooks.api.intuit.com/"
                "v3/company`, "
                "re-authenticate with `cpapacket auth qbo login`, then rerun `cpapacket doctor`."
            )
        return DoctorCheckResult(
            check_name="qbo_connectivity",
            status="fail",
            summary="QBO connectivity check failed.",
            details=[f"probe_error={exc}"],
            guidance=guidance,
        )

    company_name = _extract_company_name(payload)
    details = []
    if company_name:
        details.append(f"company={company_name}")

    return DoctorCheckResult(
        check_name="qbo_connectivity",
        status="pass",
        summary="QBO connectivity check passed.",
        details=details,
    )


def run_python_environment_check(
    *,
    min_version: tuple[int, int] = (3, 11),
    required_modules: Sequence[str] = DEFAULT_REQUIRED_MODULES,
    version_info: tuple[int, int, int] | None = None,
) -> DoctorCheckResult:
    """Validate Python runtime version and required dependency availability."""
    major, minor, micro = version_info or (
        sys.version_info.major,
        sys.version_info.minor,
        sys.version_info.micro,
    )
    runtime_label = f"{major}.{minor}.{micro}"

    if (major, minor) < min_version:
        required_label = f"{min_version[0]}.{min_version[1]}"
        return DoctorCheckResult(
            check_name="python_environment",
            status="fail",
            summary=f"Python {runtime_label} is below required version {required_label}.",
            details=[f"runtime={runtime_label}", f"required>={required_label}"],
            guidance="Install Python 3.11+ and rerun `cpapacket doctor`.",
        )

    missing_modules = [module for module in required_modules if find_spec(module) is None]
    if missing_modules:
        return DoctorCheckResult(
            check_name="python_environment",
            status="fail",
            summary="Required Python packages are missing.",
            details=[f"runtime={runtime_label}", f"missing={','.join(sorted(missing_modules))}"],
            guidance="Install project dependencies and rerun `cpapacket doctor`.",
        )

    return DoctorCheckResult(
        check_name="python_environment",
        status="pass",
        summary="Python environment check passed.",
        details=[
            f"runtime={runtime_label}",
            f"required_modules={len(required_modules)}",
        ],
    )


def run_qbo_token_check(
    *,
    load_token: QboTokenLoader,
    refresh_probe: QboTokenRefreshProbe,
    expiry_leeway_seconds: int = 60,
) -> DoctorCheckResult:
    """Validate QBO token presence, expiry, and refresh flow."""
    token = load_token()
    if token is None:
        return DoctorCheckResult(
            check_name="qbo_token",
            status="fail",
            summary="QBO token not found.",
            details=[],
            guidance="Run `cpapacket auth qbo login` and rerun `cpapacket doctor`.",
        )

    now = datetime.now(UTC)
    details = [f"expires_at={token.expires_at.astimezone(UTC).isoformat()}"]
    if token.is_expired(leeway_seconds=expiry_leeway_seconds):
        details.append("expired=true")
    else:
        details.append("expired=false")

    try:
        refresh_probe(token.refresh_token)
    except Exception as exc:
        return DoctorCheckResult(
            check_name="qbo_token",
            status="fail",
            summary="QBO token refresh probe failed.",
            details=[*details, f"refresh_error={exc}"],
            guidance="Re-authenticate with `cpapacket auth qbo login` and retry doctor.",
        )

    age_seconds = int((token.expires_at.astimezone(UTC) - now).total_seconds())
    return DoctorCheckResult(
        check_name="qbo_token",
        status="pass",
        summary="QBO token check passed.",
        details=[*details, f"seconds_to_expiry={age_seconds}", "refresh_probe=ok"],
    )


def run_gusto_token_check(
    *,
    load_token: QboTokenLoader,
    refresh_probe: QboTokenRefreshProbe,
    expiry_leeway_seconds: int = 60,
) -> DoctorCheckResult:
    """Validate optional Gusto token state without hard-failing when absent."""
    token = load_token()
    if token is None:
        return DoctorCheckResult(
            check_name="gusto_token",
            status="pass",
            summary="Gusto token not configured (optional).",
            details=["configured=false"],
            guidance="Run `cpapacket auth gusto login` to enable Gusto-backed checks.",
        )

    now = datetime.now(UTC)
    details = [f"expires_at={token.expires_at.astimezone(UTC).isoformat()}"]
    if token.is_expired(leeway_seconds=expiry_leeway_seconds):
        details.append("expired=true")
    else:
        details.append("expired=false")

    try:
        refresh_probe(token.refresh_token)
    except Exception as exc:
        return DoctorCheckResult(
            check_name="gusto_token",
            status="fail",
            summary="Gusto token refresh probe failed.",
            details=[*details, f"refresh_error={exc}"],
            guidance="Run `cpapacket auth gusto login` to refresh credentials and retry doctor.",
        )

    age_seconds = int((token.expires_at.astimezone(UTC) - now).total_seconds())
    return DoctorCheckResult(
        check_name="gusto_token",
        status="pass",
        summary="Gusto token check passed.",
        details=[*details, f"seconds_to_expiry={age_seconds}", "refresh_probe=ok"],
    )


def run_gusto_connectivity_check(
    *,
    load_token: QboTokenLoader,
    company_identity_probe: GustoCompanyIdentityProbe,
) -> DoctorCheckResult:
    """Validate optional Gusto API connectivity via lightweight identity fetch."""
    token = load_token()
    if token is None:
        return DoctorCheckResult(
            check_name="gusto_connectivity",
            status="pass",
            summary="Gusto connectivity check skipped (token not configured).",
            details=["configured=false"],
            guidance="Run `cpapacket auth gusto login` to enable Gusto-backed checks.",
        )

    details = [f"expires_at={token.expires_at.astimezone(UTC).isoformat()}"]
    try:
        identity_payload = company_identity_probe()
    except Exception as exc:
        return DoctorCheckResult(
            check_name="gusto_connectivity",
            status="fail",
            summary="Gusto connectivity check failed.",
            details=[*details, f"probe_error={exc}"],
            guidance="Verify network/API access and rerun `cpapacket doctor`.",
        )

    company_name = _extract_company_name(identity_payload)
    if company_name:
        details.append(f"company={company_name}")
    return DoctorCheckResult(
        check_name="gusto_connectivity",
        status="pass",
        summary="Gusto connectivity check passed.",
        details=[*details, "identity_probe=ok"],
    )


def _extract_company_name(payload: dict[str, object]) -> str | None:
    company_info = payload.get("CompanyInfo")
    if isinstance(company_info, dict):
        nested = company_info.get("CompanyName")
        if isinstance(nested, str) and nested.strip():
            return nested.strip()

    top_level = payload.get("CompanyName")
    if isinstance(top_level, str) and top_level.strip():
        return top_level.strip()

    return None
