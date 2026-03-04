"""Doctor command checks shared across CLI wrappers."""

from __future__ import annotations

import sys
from collections.abc import Sequence
from dataclasses import dataclass, field
from importlib.util import find_spec
from typing import Literal

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
