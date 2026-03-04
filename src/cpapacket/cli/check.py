"""Health check CLI command wrapper."""

from __future__ import annotations

import os
from typing import cast

import click

from cpapacket.clients.qbo import QboOAuthClient, QboOAuthConfig
from cpapacket.core.context import RunContext
from cpapacket.data.providers import DataProviders
from cpapacket.data.store import SessionDataStore
from cpapacket.packet.health_check import (
    DataHealthCheck,
    DataHealthCheckContext,
    check_open_prior_year_items,
    check_suspense_accounts_balance,
    check_uncategorized_transactions,
    check_undeposited_funds_balance,
    prompt_message,
    run_data_health_precheck,
    should_continue_after_report,
    write_data_health_report,
)


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


def _default_checks() -> tuple[DataHealthCheck, ...]:
    return (
        check_uncategorized_transactions,
        check_undeposited_funds_balance,
        check_suspense_accounts_balance,
        check_open_prior_year_items,
    )


def register_check_command(cli_group: click.Group) -> None:
    """Register `cpapacket check` command on the provided click group."""

    @cli_group.command("check")
    @click.pass_context
    def check_command(ctx: click.Context) -> None:
        """Run non-blocking accounting data health checks."""
        run_context = cast(RunContext | None, ctx.obj.get("run_context") if ctx.obj else None)
        if run_context is None:
            raise click.ClickException("Run context is not initialized.")

        store = SessionDataStore(cache_dir=run_context.out_dir / "_meta" / "private" / "cache")
        providers = DataProviders(store=store, qbo_client=_build_qbo_client())

        report = run_data_health_precheck(
            context=DataHealthCheckContext(
                year=run_context.year,
                providers=providers,
                gusto_connected=run_context.gusto_available,
            ),
            checks=_default_checks(),
        )
        report_path = write_data_health_report(output_root=run_context.out_dir, report=report)

        click.echo("Data health check complete.")
        click.echo(f"Report: {report_path}")

        if report.has_issues:
            click.echo(f"Warnings found: {len(report.issues)}")
            for issue in report.issues:
                click.echo(f"WARNING [{issue.code}] {issue.message}")

        proceed = should_continue_after_report(
            report=report,
            non_interactive=run_context.non_interactive,
            confirm=lambda message: click.confirm(message, default=False),
        )
        if not proceed:
            raise click.ClickException(f"Aborted by user: {prompt_message()}")
