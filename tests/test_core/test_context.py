from __future__ import annotations

from datetime import date
from pathlib import Path
from uuid import UUID

import pytest
from pydantic import ValidationError

from cpapacket.core.context import RunContext, resolve_year_and_source


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


def test_resolve_year_prefers_explicit_year() -> None:
    year, source = resolve_year_and_source(
        explicit_year=2022,
        out_dir=Path("/tmp/Acme_2025_CPA_Packet"),
        today=date(2026, 1, 15),
    )
    assert (year, source) == (2022, "explicit")


def test_resolve_year_infers_from_packet_directory_name() -> None:
    year, source = resolve_year_and_source(
        explicit_year=None,
        out_dir=Path("/tmp/Example_Co_2024_CPA_Packet"),
        today=date(2026, 1, 15),
    )
    assert (year, source) == (2024, "inferred")


def test_resolve_year_uses_default_rule_for_jan_to_sep() -> None:
    year, source = resolve_year_and_source(
        explicit_year=None,
        out_dir=Path("/tmp/not-a-packet-name"),
        today=date(2026, 9, 1),
    )
    assert (year, source) == (2025, "default")


def test_resolve_year_uses_default_rule_for_oct_to_dec() -> None:
    year, source = resolve_year_and_source(
        explicit_year=None,
        out_dir=Path("/tmp/not-a-packet-name"),
        today=date(2026, 10, 1),
    )
    assert (year, source) == (2026, "default")


def test_resolve_year_ignores_malformed_packet_directory_names() -> None:
    year, source = resolve_year_and_source(
        explicit_year=None,
        out_dir=Path("/tmp/Company_20X5_CPA_Packet"),
        today=date(2026, 1, 10),
    )
    assert (year, source) == (2025, "default")
