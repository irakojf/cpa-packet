"""Privacy utilities shared between CLI commands and scripts."""

from __future__ import annotations

from .checker import (
    PathViolation,
    PatternViolation,
    scan_fixtures_for_patterns,
    scan_repo_for_sensitive_paths,
)
from .patterns import PATTERNS, PatternSpec

__all__ = [
    "PatternSpec",
    "PatternViolation",
    "PATTERNS",
    "PathViolation",
    "scan_fixtures_for_patterns",
    "scan_repo_for_sensitive_paths",
]
