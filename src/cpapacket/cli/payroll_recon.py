"""Payroll reconciliation CLI command wrapper."""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, cast

import click

from cpapacket.clients.gusto import GustoOAuthClient, GustoOAuthConfig
from cpapacket.clients.qbo import QboOAuthClient, QboOAuthConfig
from cpapacket.core.context import RunContext
from cpapacket.data.providers import DataProviders
from cpapacket.data.store import SessionDataStore

if TYPE_CHECKING:
    from cpapacket.deliverables.payroll_recon import PayrollReconDeliverable


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise click.ClickException(f"Missing required environment variable: {name}")
    return value


def _build_qbo_client() -> QboOAuthClient:
    return QboOAuthClient(
        QboOAuthConfig(
            client_id=_required_env("CPAPACKET_QBO_CLIENT_ID"),
            client_secret=_required_env("CPAPACKET_QBO_CLIENT_SECRET"),
            redirect_uri=_required_env("CPAPACKET_QBO_REDIRECT_URI"),
            realm_id=os.getenv("CPAPACKET_QBO_REALM_ID"),
        )
    )


def _build_gusto_client() -> GustoOAuthClient:
    return GustoOAuthClient(
        GustoOAuthConfig(
            client_id=_required_env("CPAPACKET_GUSTO_CLIENT_ID"),
            client_secret=_required_env("CPAPACKET_GUSTO_CLIENT_SECRET"),
            redirect_uri=_required_env("CPAPACKET_GUSTO_REDIRECT_URI"),
        )
    )


def _build_deliverable() -> PayrollReconDeliverable:
    from cpapacket.deliverables.payroll_recon import PayrollReconDeliverable

    return PayrollReconDeliverable()


def register_payroll_recon_command(cli_group: click.Group) -> None:
    """Add `cpapacket payroll-recon` command to the provided click group."""

    @cli_group.command("payroll-recon")
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
    def payroll_recon_command(
        ctx: click.Context,
        year: int | None,
        out_dir: Path | None,
        incremental: bool | None,
        force: bool | None,
    ) -> None:
        """Generate payroll reconciliation deliverable outputs."""
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
            qbo_client=_build_qbo_client(),
            gusto_client=_build_gusto_client(),
        )
        result = _build_deliverable().generate(run_context, providers, prompts={})
        if not result.success:
            raise click.ClickException(result.error or "Payroll reconciliation generation failed.")

        click.echo("Payroll reconciliation deliverable complete.")
        for artifact in result.artifacts:
            click.echo(artifact)
        for warning in result.warnings:
            click.echo(f"WARNING: {warning}")
