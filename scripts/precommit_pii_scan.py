#!/usr/bin/env python3
"""Pre-commit scanner for staged tests JSON files containing PII-like patterns."""

from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass

from cpapacket.privacy.patterns import PATTERNS


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
