from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from cpapacket.packet.health_check import (
    DataHealthCheckContext,
    DataHealthIssue,
    DataHealthReport,
    check_payroll_sync_status,
    render_data_health_report,
    run_data_health_precheck,
    should_continue_after_report,
    write_data_health_report,
)


def test_run_data_health_precheck_collects_issues_and_check_names() -> None:
    context = DataHealthCheckContext(year=2025, providers=object(), gusto_connected=False)

    def clean(_context: DataHealthCheckContext) -> None:
        return None

    def warning(_context: DataHealthCheckContext) -> DataHealthIssue:
        return DataHealthIssue(
            code="uncategorized",
            title="Uncategorized Transactions",
            message="Found uncategorized postings.",
        )

    report = run_data_health_precheck(context=context, checks=[clean, warning])

    assert report.year == 2025
    assert report.check_names == ["clean", "warning"]
    assert len(report.issues) == 1
    assert report.issues[0].code == "uncategorized"


def test_run_data_health_precheck_converts_check_exceptions_to_warnings() -> None:
    context = DataHealthCheckContext(year=2025, providers=object(), gusto_connected=False)

    def exploding(_context: DataHealthCheckContext) -> None:
        raise RuntimeError("boom")

    report = run_data_health_precheck(context=context, checks=[exploding])

    assert report.has_issues is True
    assert report.issues[0].code == "exploding_error"
    assert report.issues[0].metadata["error"] == "boom"


def test_write_data_health_report_writes_meta_public_file_atomically(tmp_path: Path) -> None:
    report = DataHealthReport(
        year=2025,
        generated_at=datetime(2026, 1, 5, 12, 0, tzinfo=UTC).isoformat(),
        issues=[
            DataHealthIssue(
                code="suspense_balance",
                title="Suspense Account Balance",
                message="Suspense account has non-zero balance.",
                metadata={"amount": "500.00"},
            )
        ],
        check_names=["check_suspense"],
    )

    output_path = write_data_health_report(output_root=tmp_path, report=report)
    expected_path = tmp_path / "_meta" / "public" / "data_health_check.txt"

    assert output_path == expected_path
    assert output_path.exists()
    assert not list((tmp_path / "_meta" / "public").glob("*.tmp"))

    text = output_path.read_text(encoding="utf-8")
    assert "cpapacket data health check" in text
    assert "status: warnings" in text
    assert "[suspense_balance] Suspense Account Balance" in text
    assert "amount: 500.00" in text


def test_render_data_health_report_clean_status() -> None:
    report = DataHealthReport(
        year=2025,
        generated_at="2026-01-05T12:00:00+00:00",
        issues=[],
        check_names=["a", "b"],
    )

    text = render_data_health_report(report)
    assert "status: clean" in text
    assert "No data quality warnings detected." in text


def test_should_continue_after_report_behavior() -> None:
    clean_report = DataHealthReport(
        year=2025,
        generated_at="2026-01-05T12:00:00+00:00",
        issues=[],
        check_names=[],
    )
    warning_report = DataHealthReport(
        year=2025,
        generated_at="2026-01-05T12:00:00+00:00",
        issues=[DataHealthIssue(code="x", title="X", message="warn")],
        check_names=["x"],
    )

    assert should_continue_after_report(report=clean_report, non_interactive=False) is True
    assert should_continue_after_report(report=warning_report, non_interactive=True) is True
    assert should_continue_after_report(report=warning_report, non_interactive=False) is False
    assert (
        should_continue_after_report(
            report=warning_report,
            non_interactive=False,
            confirm=lambda _prompt: True,
        )
        is True
    )


class _PayrollProviders:
    def __init__(self, runs: list[dict[str, object]]) -> None:
        self._runs = runs

    def get_payroll_runs(self, year: int) -> list[dict[str, object]]:
        del year
        return self._runs


def test_check_payroll_sync_status_warns_when_unsynced() -> None:
    providers = _PayrollProviders(
        runs=[
            {"uuid": "run-1", "check_date": "2025-01-20", "qbo_sync_status": "pending"},
            {"uuid": "run-2", "check_date": "2025-02-20", "qbo_sync_status": "failed"},
        ]
    )
    context = DataHealthCheckContext(year=2025, providers=providers, gusto_connected=True)

    issue = check_payroll_sync_status(context)

    assert issue is not None
    assert issue.code == "payroll_sync_status"
    assert issue.metadata["latest_payroll_uuid"] == "run-2"


def test_check_payroll_sync_status_returns_none_when_synced() -> None:
    providers = _PayrollProviders(
        runs=[
            {"uuid": "run-1", "check_date": "2025-02-20", "qbo_sync_completed": True},
        ]
    )
    context = DataHealthCheckContext(year=2025, providers=providers, gusto_connected=True)

    issue = check_payroll_sync_status(context)

    assert issue is None


def test_check_payroll_sync_status_skips_when_gusto_not_connected() -> None:
    providers = _PayrollProviders(
        runs=[
            {"uuid": "run-1", "check_date": "2025-02-20", "qbo_sync_status": "failed"},
        ]
    )
    context = DataHealthCheckContext(year=2025, providers=providers, gusto_connected=False)

    issue = check_payroll_sync_status(context)

    assert issue is None
