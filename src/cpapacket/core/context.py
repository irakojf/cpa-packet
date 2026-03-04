"""Run-wide immutable execution context."""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_PACKET_YEAR_PATTERN = re.compile(r".*_(\d{4})_CPA_Packet$")


class RunContext(BaseModel):
    """Immutable context constructed once at CLI entry."""

    model_config = ConfigDict(frozen=True)

    year: int = Field(ge=1)
    run_id: str = Field(default_factory=lambda: str(uuid4()))
    year_source: Literal["explicit", "inferred", "default"]
    out_dir: Path
    method: Literal["accrual", "cash"] = "accrual"
    non_interactive: bool
    on_conflict: Literal["prompt", "overwrite", "copy", "abort"]
    incremental: bool = False
    force: bool = False
    no_cache: bool = False
    no_raw: bool = False
    redact: bool = False
    include_debug: bool = False
    verbose: bool = False
    quiet: bool = False
    plain: bool = False
    skip: list[str] = Field(default_factory=list)
    owner_keywords: list[str] = Field(default_factory=list)
    gusto_available: bool = True

    @field_validator("skip", "owner_keywords", mode="before")
    @classmethod
    def _normalize_string_list(cls, value: object) -> list[str]:
        if value is None:
            return []

        if isinstance(value, str):
            candidates = value.split(",")
        elif isinstance(value, list):
            candidates = value
        else:
            raise ValueError("must be a list of strings or a comma-separated string")

        normalized: list[str] = []
        for item in candidates:
            cleaned = str(item).strip()
            if cleaned:
                normalized.append(cleaned)
        return normalized

    @model_validator(mode="after")
    def _validate_flag_combinations(self) -> RunContext:
        if self.verbose and self.quiet:
            raise ValueError("verbose and quiet flags are mutually exclusive")
        if self.non_interactive and self.on_conflict == "prompt":
            raise ValueError("non_interactive mode cannot use on_conflict='prompt'")
        return self


def resolve_year_and_source(
    *,
    explicit_year: int | None,
    out_dir: Path | str | None,
    today: date | None = None,
) -> tuple[int, Literal["explicit", "inferred", "default"]]:
    """Resolve tax year using CLI explicit value, output dir hint, then month rule."""
    if explicit_year is not None:
        if explicit_year < 1:
            raise ValueError("explicit_year must be >= 1")
        return explicit_year, "explicit"

    inferred = _infer_year_from_out_dir(out_dir)
    if inferred is not None:
        return inferred, "inferred"

    reference_date = today or date.today()
    if 1 <= reference_date.month <= 9:
        return reference_date.year - 1, "default"
    return reference_date.year, "default"


def _infer_year_from_out_dir(out_dir: Path | str | None) -> int | None:
    if out_dir is None:
        return None
    out_name = Path(out_dir).name
    match = _PACKET_YEAR_PATTERN.match(out_name)
    if match is None:
        return None
    return int(match.group(1))
