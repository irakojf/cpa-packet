"""Top-level click entrypoint for cpapacket."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, Literal, cast

import click

from cpapacket.core.context import RunContext, resolve_year_and_source

MethodOption = Literal["accrual", "cash"]
ConflictOption = Literal["prompt", "overwrite", "copy", "abort"]


def _package_version() -> str:
    try:
        return version("cpapacket")
    except PackageNotFoundError:
        return "0.0.0"


def _normalize_csv_values(raw_value: str | None) -> list[str]:
    if not raw_value:
        return []
    return [value.strip() for value in raw_value.split(",") if value.strip()]


def build_run_context(
    *,
    year: int | None,
    out_dir: Path,
    method: MethodOption,
    non_interactive: bool,
    on_conflict: ConflictOption | None,
    incremental: bool,
    force: bool,
    no_cache: bool,
    no_raw: bool,
    redact: bool,
    include_debug: bool,
    verbose: bool,
    quiet: bool,
    plain: bool,
    owner_keywords_raw: str | None,
) -> RunContext:
    """Construct a validated RunContext from global CLI options."""
    resolved_year, year_source = resolve_year_and_source(
        explicit_year=year,
        out_dir=out_dir,
    )
    resolved_on_conflict: ConflictOption = on_conflict or (
        "abort" if non_interactive else "prompt"
    )

    return RunContext(
        year=resolved_year,
        year_source=year_source,
        out_dir=out_dir.resolve(),
        method=method,
        non_interactive=non_interactive,
        on_conflict=resolved_on_conflict,
        incremental=incremental,
        force=force,
        no_cache=no_cache,
        no_raw=no_raw,
        redact=redact,
        include_debug=include_debug,
        verbose=verbose,
        quiet=quiet,
        plain=plain,
        owner_keywords=_normalize_csv_values(owner_keywords_raw),
    )


@click.group(context_settings={"help_option_names": ["-h", "--help"]}, invoke_without_command=True)
@click.option(
    "--version",
    "show_version",
    is_flag=True,
    is_eager=True,
    help="Print version and exit.",
)
@click.option("--year", type=int, default=None, help="Tax year override.")
@click.option(
    "--method",
    type=click.Choice(["accrual", "cash"], case_sensitive=False),
    default="accrual",
    show_default=True,
    help="Accounting method for P&L.",
)
@click.option("--non-interactive", is_flag=True, help="Disable prompts and use safe defaults.")
@click.option(
    "--on-conflict",
    type=click.Choice(["overwrite", "copy", "abort", "prompt"], case_sensitive=False),
    default=None,
    help="Conflict behavior when output files already exist.",
)
@click.option("--incremental", is_flag=True, help="Skip up-to-date deliverables via metadata.")
@click.option("--force", is_flag=True, help="Force regeneration and bypass caches.")
@click.option("--no-cache", is_flag=True, help="Disable cache writes.")
@click.option("--no-raw", is_flag=True, help="Skip raw JSON artifact output.")
@click.option("--redact", is_flag=True, help="Redact sensitive fields in raw JSON output.")
@click.option("--include-debug", is_flag=True, help="Include debug log in output zip.")
@click.option("--owner-keywords", type=str, default=None, help="Comma-separated owner keywords.")
@click.option("--verbose", "-v", is_flag=True, help="Set console logging to DEBUG.")
@click.option("--quiet", "-q", is_flag=True, help="Set console logging to WARNING.")
@click.option("--plain", is_flag=True, help="Disable rich formatting.")
@click.pass_context
def cli(
    ctx: click.Context,
    show_version: bool,
    year: int | None,
    method: str,
    non_interactive: bool,
    on_conflict: str | None,
    incremental: bool,
    force: bool,
    no_cache: bool,
    no_raw: bool,
    redact: bool,
    include_debug: bool,
    owner_keywords: str | None,
    verbose: bool,
    quiet: bool,
    plain: bool,
) -> None:
    """cpapacket command group."""
    if show_version:
        click.echo(_package_version())
        ctx.exit(0)

    run_context = build_run_context(
        year=year,
        out_dir=Path.cwd(),
        method=cast(MethodOption, method.lower()),
        non_interactive=non_interactive,
        on_conflict=cast(ConflictOption | None, on_conflict.lower() if on_conflict else None),
        incremental=incremental,
        force=force,
        no_cache=no_cache,
        no_raw=no_raw,
        redact=redact,
        include_debug=include_debug,
        verbose=verbose,
        quiet=quiet,
        plain=plain,
        owner_keywords_raw=owner_keywords,
    )

    ctx.ensure_object(dict)
    ctx.obj["run_context"] = run_context


@cli.command("context-debug")
@click.pass_context
def context_debug(ctx: click.Context) -> None:
    """Print resolved RunContext as JSON for debugging."""
    run_context = ctx.obj["run_context"]
    click.echo(run_context.model_dump_json(indent=2))


def main(argv: list[str] | None = None) -> Any:
    return cli.main(args=argv, standalone_mode=False)
