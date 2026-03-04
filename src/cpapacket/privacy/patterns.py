"""Reusable regex specifications for privacy pattern detection."""

from __future__ import annotations

import re
from dataclasses import dataclass
from re import Pattern


@dataclass(frozen=True)
class PatternSpec:
    """Describes a single regex pattern that identifies sensitive content."""

    name: str
    regex: Pattern[str]


PATTERNS: tuple[PatternSpec, ...] = (
    PatternSpec("SSN", re.compile(r"\b\d{3}-\d{2}-\d{4}\b")),
    PatternSpec("EIN", re.compile(r"\b\d{2}-\d{7}\b")),
    PatternSpec("ITIN", re.compile(r"\b9\d{2}-\d{2}-\d{4}\b")),
    PatternSpec(
        "ROUTING_NUMBER",
        re.compile(r'(?i)"(?:routing|routing_number|aba)"\s*:\s*"?(?:\d{9})"?'),
    ),
    PatternSpec(
        "ACCOUNT_NUMBER",
        re.compile(
            r'(?i)"(?:account|account_number|acct|bank_account)"\s*:\s*"?(?:\d{6,17})"?'
        ),
    ),
    PatternSpec(
        "EMAIL",
        re.compile(
            r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b",
        ),
    ),
    PatternSpec(
        "PHONE",
        re.compile(
            r"\b(?:\d{3}-\d{3}-\d{4}|\(\d{3}\)\s?\d{3}-\d{4})\b",
        ),
    ),
)
