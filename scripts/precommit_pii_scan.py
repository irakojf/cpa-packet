#!/usr/bin/env python3
"""Pre-commit scanner for staged tests JSON files containing PII-like patterns."""

from __future__ import annotations

import re
import subprocess
import sys
from dataclasses import dataclass


@dataclass(frozen=True)
class PatternSpec:
    name: str
    regex: re.Pattern[str]


PATTERNS: tuple[PatternSpec, ...] = (
    PatternSpec("SSN", re.compile(r"\b\d{3}-\d{2}-\d{4}\b")),
    PatternSpec("EIN", re.compile(r"\b\d{2}-\d{7}\b")),
    PatternSpec("ITIN", re.compile(r"\b9\d{2}-\d{2}-\d{4}\b")),
    PatternSpec("PHONE", re.compile(r"\b(?:\d{3}-\d{3}-\d{4}|\(\d{3}\)\s?\d{3}-\d{4})\b")),
    PatternSpec("EMAIL", re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")),
)


@dataclass(frozen=True)
class Violation:
    path: str
    line_no: int
    pattern: str
    excerpt: str


def _run_git(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["git", *args], check=False, capture_output=True, text=True)


def staged_test_json_paths() -> list[str]:
    result = _run_git(["diff", "--cached", "--name-only", "--diff-filter=AM"])
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "failed to enumerate staged files")

    output = result.stdout.strip()
    if not output:
        return []

    candidates = output.splitlines()
    return [p for p in candidates if p.startswith("tests/") and p.endswith(".json")]


def staged_file_content(path: str) -> str:
    # :path reads from the index so unstaged local changes are ignored.
    result = _run_git(["show", f":{path}"])
    if result.returncode != 0:
        raise RuntimeError(f"failed to read staged content for {path}: {result.stderr.strip()}")
    return result.stdout


def find_violations(path: str, content: str) -> list[Violation]:
    violations: list[Violation] = []
    for line_no, line in enumerate(content.splitlines(), start=1):
        for spec in PATTERNS:
            match = spec.regex.search(line)
            if match is None:
                continue
            violations.append(
                Violation(
                    path=path,
                    line_no=line_no,
                    pattern=spec.name,
                    excerpt=match.group(0),
                )
            )
    return violations


def scan_staged_test_json() -> list[Violation]:
    violations: list[Violation] = []
    for path in staged_test_json_paths():
        content = staged_file_content(path)
        violations.extend(find_violations(path, content))
    return violations


def main() -> int:
    try:
        violations = scan_staged_test_json()
    except RuntimeError as exc:
        print(f"[pii-scan] ERROR: {exc}", file=sys.stderr)
        return 1

    if not violations:
        return 0

    print("[pii-scan] Potential PII detected in staged tests JSON files:", file=sys.stderr)
    for issue in violations:
        print(
            f"  - {issue.path}:{issue.line_no} [{issue.pattern}] {issue.excerpt}",
            file=sys.stderr,
        )

    print(
        "[pii-scan] Commit blocked. Remove/sanitize data or move to non-PII fixtures.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
