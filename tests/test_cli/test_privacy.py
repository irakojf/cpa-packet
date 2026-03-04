from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from cpapacket.cli.main import cli


def test_privacy_check_passes_on_clean_tree(tmp_path: Path) -> None:
    runner = CliRunner()

    with runner.isolated_filesystem(temp_dir=str(tmp_path)):
        result = runner.invoke(cli, ["privacy", "check"])

    assert result.exit_code == 0
    assert "Privacy check passed" in result.output


def test_privacy_check_reports_sensitive_artifacts(tmp_path: Path) -> None:
    runner = CliRunner()

    with runner.isolated_filesystem(temp_dir=str(tmp_path)):
        repo_root = Path.cwd()
        (repo_root / ".env.local").write_text("SECRET", encoding="utf-8")
        meta_dir = repo_root / "_meta"
        meta_dir.mkdir()
        (meta_dir / "data.bin").write_text("cache", encoding="utf-8")
        fixtures = repo_root / "tests" / "fixtures"
        fixtures.mkdir(parents=True, exist_ok=True)
        (fixtures / "pii.json").write_text(
            '{"ssn": "123-45-6789"}',
            encoding="utf-8",
        )

        result = runner.invoke(cli, ["privacy", "check"])

    assert result.exit_code != 0
    assert "Privacy guardrails detected violations" in result.output
    assert "_meta" in result.output
    assert ".env" in result.output
    assert "SSN" in result.output
    assert "tests/fixtures/pii.json" in result.output
