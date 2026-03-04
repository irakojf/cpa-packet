from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from cpapacket.cli.main import build_run_context, cli


def test_build_run_context_defaults_on_conflict_by_interactive_mode() -> None:
    interactive_ctx = build_run_context(
        year=2025,
        out_dir=Path("/tmp"),
        method="accrual",
        non_interactive=False,
        on_conflict=None,
        incremental=False,
        force=False,
        no_cache=False,
        no_raw=False,
        redact=False,
        include_debug=False,
        verbose=False,
        quiet=False,
        plain=False,
        owner_keywords_raw=None,
    )
    non_interactive_ctx = build_run_context(
        year=2025,
        out_dir=Path("/tmp"),
        method="accrual",
        non_interactive=True,
        on_conflict=None,
        incremental=False,
        force=False,
        no_cache=False,
        no_raw=False,
        redact=False,
        include_debug=False,
        verbose=False,
        quiet=False,
        plain=False,
        owner_keywords_raw=None,
    )

    assert interactive_ctx.on_conflict == "prompt"
    assert non_interactive_ctx.on_conflict == "abort"


def test_build_run_context_uses_year_resolution_and_owner_keywords() -> None:
    ctx = build_run_context(
        year=None,
        out_dir=Path("/tmp/Acme_2024_CPA_Packet"),
        method="cash",
        non_interactive=True,
        on_conflict="copy",
        incremental=True,
        force=True,
        no_cache=True,
        no_raw=True,
        redact=True,
        include_debug=True,
        verbose=True,
        quiet=False,
        plain=True,
        owner_keywords_raw="Alex, Smith ,",
    )

    assert ctx.year == 2024
    assert ctx.year_source == "inferred"
    assert ctx.method == "cash"
    assert ctx.owner_keywords == ["Alex", "Smith"]
    assert ctx.incremental is True
    assert ctx.force is True


def test_cli_version_flag_exits_successfully() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert result.output.strip() != ""


def test_context_debug_outputs_resolved_context_json() -> None:
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--year",
            "2025",
            "--non-interactive",
            "--owner-keywords",
            "Alex,Smith",
            "context-debug",
        ],
    )
    assert result.exit_code == 0
    assert '"year": 2025' in result.output
    assert '"year_source": "explicit"' in result.output
    assert '"owner_keywords"' in result.output
