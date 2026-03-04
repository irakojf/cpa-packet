from __future__ import annotations

from typing import Any

from cpapacket.packet.doctor import run_python_environment_check


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
