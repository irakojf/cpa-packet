from __future__ import annotations

import subprocess

import pytest
from scripts import precommit_pii_scan


def test_find_violations_detects_expected_patterns() -> None:
    content = """
    {
      "ssn": "123-45-6789",
      "ein": "12-3456789",
      "email": "user@example.com",
      "note": "safe value"
    }
    """

    violations = precommit_pii_scan.find_violations("tests/fixtures/a.json", content)
    assert [v.pattern for v in violations] == ["SSN", "EIN", "EMAIL"]


def test_staged_test_json_paths_filters_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run_git(_: list[str]) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=["git"],
            returncode=0,
            stdout="\n".join(
                [
                    "tests/fixtures/safe.json",
                    "tests/fixtures/secret.txt",
                    "src/cpapacket/data/example.json",
                    "tests/test_data/sample.json",
                ]
            ),
            stderr="",
        )

    monkeypatch.setattr(precommit_pii_scan, "_run_git", fake_run_git)
    assert precommit_pii_scan.staged_test_json_paths() == [
        "tests/fixtures/safe.json",
        "tests/test_data/sample.json",
    ]


def test_staged_test_json_paths_raises_on_git_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run_git(_: list[str]) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=["git"],
            returncode=1,
            stdout="",
            stderr="fatal: not a git repository",
        )

    monkeypatch.setattr(precommit_pii_scan, "_run_git", fake_run_git)
    with pytest.raises(RuntimeError, match="not a git repository"):
        precommit_pii_scan.staged_test_json_paths()
