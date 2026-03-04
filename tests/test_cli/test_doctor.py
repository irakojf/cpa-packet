from __future__ import annotations

from click.testing import CliRunner
from pytest import MonkeyPatch

from cpapacket.cli import doctor as doctor_cli
from cpapacket.cli.main import cli
from cpapacket.packet.doctor import DoctorCheckResult


def test_doctor_command_reports_pass_checks(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr(
        doctor_cli,
        "_run_doctor_checks",
        lambda: doctor_cli.DoctorCommandSummary(
            results=[
                DoctorCheckResult(
                    check_name="python_environment",
                    status="pass",
                    summary="Python environment check passed.",
                    details=["runtime=3.11.9"],
                ),
                DoctorCheckResult(
                    check_name="qbo_token",
                    status="pass",
                    summary="QBO token check passed.",
                    details=["refresh_probe=ok"],
                ),
            ],
            failure_count=0,
        ),
    )

    runner = CliRunner()
    result = runner.invoke(cli, ["doctor"])

    assert result.exit_code == 0
    assert "python_environment" in result.output
    assert "qbo_token" in result.output
    assert "✓" in result.output


def test_doctor_command_exits_nonzero_when_any_check_fails(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr(
        doctor_cli,
        "_run_doctor_checks",
        lambda: doctor_cli.DoctorCommandSummary(
            results=[
                DoctorCheckResult(
                    check_name="python_environment",
                    status="pass",
                    summary="Python environment check passed.",
                ),
                DoctorCheckResult(
                    check_name="qbo_connectivity",
                    status="fail",
                    summary="QBO connectivity check failed.",
                    details=["probe_error=timeout"],
                    guidance="Verify network/API access and rerun `cpapacket doctor`.",
                ),
            ],
            failure_count=1,
        ),
    )

    runner = CliRunner()
    result = runner.invoke(cli, ["doctor"])

    assert result.exit_code == 1
    assert "qbo_connectivity" in result.output
    assert "X" in result.output
    assert "Guidance:" in result.output
