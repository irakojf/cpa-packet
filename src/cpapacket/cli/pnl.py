"""P&L CLI command wrapper."""

from __future__ import annotations

import os
from typing import cast

import click

from cpapacket.clients.qbo import QboOAuthClient, QboOAuthConfig
from cpapacket.core.context import RunContext
from cpapacket.data.providers import DataProviders
from cpapacket.data.store import SessionDataStore
from cpapacket.deliverables.pnl import PnlDeliverable


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


def register_pnl_command(cli_group: click.Group) -> None:
    """Register `cpapacket pnl` command on the provided click group."""

    @cli_group.command("pnl")
    @click.pass_context
    def pnl_command(ctx: click.Context) -> None:
        """Generate Profit & Loss deliverable outputs."""
        run_context = cast(RunContext | None, ctx.obj.get("run_context") if ctx.obj else None)
        if run_context is None:
            raise click.ClickException("Run context is not initialized.")

        store = SessionDataStore(cache_dir=run_context.out_dir / "_meta" / "private" / "cache")
        providers = DataProviders(store=store, qbo_client=_build_qbo_client())
        result = PnlDeliverable().generate(run_context, providers, prompts={})

        if not result.success:
            raise click.ClickException(result.error or "P&L generation failed.")

        click.echo("P&L deliverable complete.")
        for artifact in result.artifacts:
            click.echo(artifact)
        for warning in result.warnings:
            click.echo(f"WARNING: {warning}")
