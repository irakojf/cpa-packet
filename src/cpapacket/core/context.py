"""Run-wide immutable execution context."""

from __future__ import annotations

from pathlib import Path
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


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
