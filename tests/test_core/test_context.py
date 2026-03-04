from __future__ import annotations

from pathlib import Path
from uuid import UUID

import pytest
from pydantic import ValidationError

from cpapacket.core.context import RunContext


def _context_kwargs() -> dict[str, object]:
    return {
        "year": 2025,
        "year_source": "explicit",
        "out_dir": Path("/tmp/output"),
        "non_interactive": False,
        "on_conflict": "prompt",
    }


def test_run_context_defaults_and_uuid() -> None:
    ctx = RunContext(**_context_kwargs())
    UUID(ctx.run_id)
    assert ctx.method == "accrual"
    assert ctx.incremental is False
    assert ctx.force is False
    assert ctx.skip == []
    assert ctx.owner_keywords == []
    assert ctx.gusto_available is True


def test_run_context_normalizes_string_lists() -> None:
    ctx = RunContext(
        **_context_kwargs(),
        skip="pnl, balance_sheet, ,general_ledger",
        owner_keywords=[" Alex ", "", "Smith"],
    )
    assert ctx.skip == ["pnl", "balance_sheet", "general_ledger"]
    assert ctx.owner_keywords == ["Alex", "Smith"]


def test_run_context_rejects_verbose_and_quiet_together() -> None:
    with pytest.raises(ValidationError, match="mutually exclusive"):
        RunContext(**_context_kwargs(), verbose=True, quiet=True)


def test_run_context_rejects_prompt_in_non_interactive_mode() -> None:
    kwargs = _context_kwargs()
    kwargs["non_interactive"] = True
    with pytest.raises(
        ValidationError, match="non_interactive mode cannot use on_conflict='prompt'"
    ):
        RunContext(**kwargs)


def test_run_context_is_frozen() -> None:
    ctx = RunContext(**_context_kwargs())
    with pytest.raises(ValidationError):
        ctx.year = 2024
