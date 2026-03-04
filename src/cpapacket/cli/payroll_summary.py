"""Payroll summary CLI command wrapper."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, cast

import click

from cpapacket.clients.gusto import GustoOAuthClient, GustoOAuthConfig
from cpapacket.core.context import RunContext
from cpapacket.data.providers import DataProviders
from cpapacket.data.store import SessionDataStore
from cpapacket.deliverables.payroll_summary import PayrollSummaryDeliverable


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise click.ClickException(f"Missing required environment variable: {name}")
    return value


def _build_gusto_client() -> GustoOAuthClient:
    return GustoOAuthClient(
        GustoOAuthConfig(
            client_id=_required_env("CPAPACKET_GUSTO_CLIENT_ID"),
            client_secret=_required_env("CPAPACKET_GUSTO_CLIENT_SECRET"),
            redirect_uri=_required_env("CPAPACKET_GUSTO_REDIRECT_URI"),
        )
    )


class _UnusedQboClient:
    """Stub QBO client used by payroll-only CLI flows."""

    def request(
        self,
        method: str,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> Any:
        del method, endpoint, params, json_body
        raise RuntimeError("QBO client should not be used by payroll-summary command")


def register_payroll_summary_command(cli_group: click.Group) -> None:
    """Add `cpapacket payroll-summary` command to the provided click group."""

    @cli_group.command("payroll-summary")
    @click.option("--year", type=int, default=None, help="Tax year override for this run.")
    @click.option(
        "--out",
        "out_dir",
        type=click.Path(path_type=Path, file_okay=False, dir_okay=True),
        default=None,
        help="Base output directory for generated artifacts.",
    )
    @click.option(
        "--incremental/--no-incremental",
        default=None,
        help="Override incremental mode for this command.",
    )
    @click.option(
        "--force/--no-force",
        default=None,
        help="Override force mode for this command.",
    )
    @click.pass_context
    def payroll_summary_command(
        ctx: click.Context,
        year: int | None,
        out_dir: Path | None,
        incremental: bool | None,
        force: bool | None,
    ) -> None:
        """Generate payroll summary deliverable outputs."""
        run_context = cast(RunContext | None, ctx.obj.get("run_context") if ctx.obj else None)
        if run_context is None:
            raise click.ClickException("Run context is not initialized.")
        if not run_context.gusto_available:
            raise click.ClickException(
                "Gusto is not connected. Run `cpapacket auth gusto login` and try again."
            )

        updates: dict[str, object] = {}
        if year is not None:
            updates["year"] = year
            updates["year_source"] = "explicit"
        if out_dir is not None:
            updates["out_dir"] = out_dir.resolve()
        if incremental is not None:
            updates["incremental"] = incremental
        if force is not None:
            updates["force"] = force
        if updates:
            run_context = run_context.model_copy(update=updates)

        store = SessionDataStore(cache_dir=run_context.out_dir / "_meta" / "private" / "cache")
        providers = DataProviders(
            store=store,
            qbo_client=_UnusedQboClient(),
            gusto_client=_build_gusto_client(),
        )
        result = PayrollSummaryDeliverable().generate(run_context, providers, prompts={})
        if not result.success:
            raise click.ClickException(result.error or "Payroll summary generation failed.")

        click.echo("Payroll summary deliverable complete.")
        for artifact in result.artifacts:
            click.echo(artifact)
        for warning in result.warnings:
            click.echo(f"WARNING: {warning}")
