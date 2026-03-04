"""Repository scanners for privacy guardrails."""

from __future__ import annotations

import os
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from cpapacket.privacy.patterns import PATTERNS, PatternSpec

EXCLUDED_DIRS: tuple[str, ...] = (
    ".git",
    ".venv",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    "__pycache__",
    ".beads",
    "node_modules",
    ".cache",
)


@dataclass(frozen=True)
class PathViolation:
    """A disallowed path found in the repository."""

    path: Path
    reason: str


@dataclass(frozen=True)
class PatternViolation:
    """A line that matched a sensitive regex pattern."""

    path: Path
    line_no: int
    pattern: str
    excerpt: str


def scan_repo_for_sensitive_paths(
    root: Path,
    *,
    excluded_dirs: Sequence[str] | None = None,
) -> list[PathViolation]:
    """Walk the tree under ``root`` and report disallowed files or directories."""

    resolved_root = root.resolve()
    excludes = set(excluded_dirs or EXCLUDED_DIRS)
    violations: list[PathViolation] = []

    for dirpath, dirnames, filenames in os.walk(resolved_root):
        rel_dir = Path(dirpath).relative_to(resolved_root)
        dirnames[:] = [d for d in dirnames if d not in excludes]

        for directory in list(dirnames):
            violation_reason = _sensitive_directory_reason(directory)
            if violation_reason:
                violations.append(
                    PathViolation(path=rel_dir / directory, reason=violation_reason)
                )
                dirnames.remove(directory)

        for filename in filenames:
            rel_path = rel_dir / filename
            if any(part in excludes for part in rel_path.parts):
                continue
            reason = _sensitive_file_reason(filename, rel_path)
            if reason is not None:
                violations.append(PathViolation(path=rel_path, reason=reason))

    return violations


def scan_fixtures_for_patterns(
    root: Path,
    *,
    patterns: Sequence[PatternSpec] = PATTERNS,
) -> list[PatternViolation]:
    """Search ``tests/fixtures`` for sensitive patterns (SSN, EIN, etc.)."""

    fixtures_dir = (root / "tests" / "fixtures").resolve()
    if not fixtures_dir.exists():
        return []

    violations: list[PatternViolation] = []
    for json_path in fixtures_dir.rglob("*.json"):
        try:
            text = json_path.read_text(encoding="utf-8")
        except OSError:
            continue

        for line_no, line in enumerate(text.splitlines(), start=1):
            for spec in patterns:
                match = spec.regex.search(line)
                if match is None:
                    continue
                violations.append(
                    PatternViolation(
                        path=json_path.relative_to(root),
                        line_no=line_no,
                        pattern=spec.name,
                        excerpt=match.group(0),
                    )
                )
    return violations


def _sensitive_directory_reason(directory: str) -> str | None:
    if directory == "_meta":
        return "privacy guardrails forbid `_meta/` directories"
    if directory.endswith("_CPA_Packet"):
        return "detecting `*_CPA_Packet/` folder"
    return None


def _sensitive_file_reason(filename: str, rel_path: Path) -> str | None:
    lower_name = filename.lower()
    if filename.startswith(".env"):
        return "contains `.env*`"
    if lower_name.endswith("_cpa_packet.zip"):
        return "contains `*_CPA_Packet.zip`"
    if rel_path.parts and "_meta" in rel_path.parts:
        return "path lives under `_meta/`"
    if lower_name.endswith((".pdf", ".csv")):
        return "contains generated PDF/CSV artifact"
    return None
