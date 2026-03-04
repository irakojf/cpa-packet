from __future__ import annotations

from pathlib import Path

from cpapacket.privacy.checker import (
    scan_fixtures_for_patterns,
    scan_repo_for_sensitive_paths,
)


def test_scan_repo_for_sensitive_paths_detects_env_and_meta(tmp_path: Path) -> None:
    (tmp_path / ".env.local").write_text("secret")
    meta_dir = tmp_path / "_meta"
    meta_dir.mkdir()
    (meta_dir / "data.txt").write_text("cached")
    (tmp_path / "report.pdf").write_text("pdf")
    (tmp_path / "reports").mkdir()
    (tmp_path / "reports" / "ledger.csv").write_text("csv")

    violations = scan_repo_for_sensitive_paths(tmp_path)
    reasons = {v.reason for v in violations}
    paths = {str(v.path) for v in violations}

    assert any("`.env*`" in reason for reason in reasons)
    assert any("`_meta/`" in reason for reason in reasons)
    assert any("PDF" in reason.upper() for reason in reasons)
    assert any("CSV" in reason.upper() for reason in reasons)
    assert ".env.local" in paths
    assert any(entry.startswith("_meta") for entry in paths)
    assert "report.pdf" in paths
    assert "reports/ledger.csv" in paths


def test_scan_repo_ignores_excluded_directories(tmp_path: Path) -> None:
    exclude = tmp_path / ".git"
    exclude.mkdir()
    (exclude / ".env").write_text("hidden")

    violations = scan_repo_for_sensitive_paths(tmp_path)
    assert all(".git" not in str(v.path) for v in violations)


def test_scan_fixtures_for_patterns_detects_ssn_and_email(tmp_path: Path) -> None:
    fixtures = tmp_path / "tests" / "fixtures"
    fixtures.mkdir(parents=True, exist_ok=True)
    target = fixtures / "sensitive.json"
    target.write_text('{"ssn": "123-45-6789", "email": "user@example.com"}', encoding="utf-8")

    violations = scan_fixtures_for_patterns(tmp_path)
    assert len(violations) == 2
    patterns = {v.pattern for v in violations}
    assert {"SSN", "EMAIL"} == patterns
    assert any(v.path == Path("tests/fixtures/sensitive.json") for v in violations)


def test_scan_fixtures_for_patterns_detects_routing_and_account_numbers(
    tmp_path: Path,
) -> None:
    fixtures = tmp_path / "tests" / "fixtures"
    fixtures.mkdir(parents=True, exist_ok=True)
    target = fixtures / "bank.json"
    target.write_text(
        '{"routing_number": "011000015", "account_number": "000123456789"}',
        encoding="utf-8",
    )

    violations = scan_fixtures_for_patterns(tmp_path)
    patterns = {v.pattern for v in violations}

    assert "ROUTING_NUMBER" in patterns
    assert "ACCOUNT_NUMBER" in patterns
