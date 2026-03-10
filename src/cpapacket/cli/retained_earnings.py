"""Retained earnings CLI command wrapper."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Protocol, cast

import click

from cpapacket.clients.qbo import QboOAuthClient, QboOAuthConfig
from cpapacket.core.context import RunContext
from cpapacket.data.providers import DataProviders
from cpapacket.data.store import SessionDataStore
from cpapacket.deliverables.base import DeliverableResult


class _RetainedEarningsDeliverableLike(Protocol):
    def generate(
        self,
        ctx: RunContext,
        providers: DataProviders,
        prompts: dict[str, object],
    ) -> DeliverableResult: ...


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


def _build_deliverable() -> _RetainedEarningsDeliverableLike:
    # Lazy import keeps command registration lightweight and avoids hard dependency
    # during partial feature development in concurrent swarm lanes.
    from cpapacket.deliverables.retained_earnings import RetainedEarningsDeliverable

    return RetainedEarningsDeliverable()


def register_retained_earnings_command(cli_group: click.Group) -> None:
    """Add `cpapacket retained-earnings` command to the provided click group."""

    @cli_group.command("retained-earnings")
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
    def retained_earnings_command(
        ctx: click.Context,
        year: int | None,
        out_dir: Path | None,
        incremental: bool | None,
        force: bool | None,
    ) -> None:
        """Generate retained earnings rollforward outputs."""
        run_context = cast(RunContext | None, ctx.obj.get("run_context") if ctx.obj else None)
        if run_context is None:
            raise click.ClickException("Run context is not initialized.")

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
        providers = DataProviders(store=store, qbo_client=_build_qbo_client())
        result = _build_deliverable().generate(run_context, providers, prompts={})

        if not result.success:
            raise click.ClickException(result.error or "Retained earnings generation failed.")

        click.echo("Retained earnings deliverable complete.")
        for artifact in result.artifacts:
            click.echo(artifact)
        for warning in result.warnings:
            click.echo(f"WARNING: {warning}")
