"""Health check CLI command wrapper."""

from __future__ import annotations

import os
from typing import cast

import click
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

from cpapacket.clients.qbo import QboOAuthClient, QboOAuthConfig
from cpapacket.core.context import RunContext
from cpapacket.data.providers import DataProviders
from cpapacket.data.store import SessionDataStore
from cpapacket.packet.health_check import (
    DataHealthCheck,
    DataHealthCheckContext,
    check_open_prior_year_items,
    check_payroll_sync_status,
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
        check_payroll_sync_status,
    )


def _use_rich_panels(run_context: RunContext) -> bool:
    return not run_context.plain and not bool(os.getenv("NO_COLOR"))


def _emit_panel(message: str, *, title: str, style: str) -> None:
    console = Console(stderr=True, markup=False)
    console.print(
        Panel.fit(
            Text(message),
            title=title,
            border_style=style,
        )
    )


def _emit_warning(run_context: RunContext, message: str) -> None:
    if _use_rich_panels(run_context):
        _emit_panel(message, title="Warning", style="yellow")
        return
    click.echo(f"WARNING: {message}", err=True)


def _emit_error(run_context: RunContext, message: str) -> None:
    if _use_rich_panels(run_context):
        _emit_panel(message, title="Error", style="red")
        return
    click.echo(f"ERROR: {message}", err=True)


def _emit_success(run_context: RunContext, message: str) -> None:
    if _use_rich_panels(run_context):
        _emit_panel(message, title="Success", style="green")
        return
    click.echo(f"SUCCESS: {message}")


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

        _emit_success(run_context, "Data health check complete.")
        click.echo(f"Report: {report_path}")

        if report.has_issues:
            _emit_warning(run_context, f"Warnings found: {len(report.issues)}")
            for issue in report.issues:
                _emit_warning(run_context, f"[{issue.code}] {issue.message}")

        proceed = should_continue_after_report(
            report=report,
            non_interactive=run_context.non_interactive,
            confirm=lambda message: click.confirm(message, default=False),
        )
        if not proceed:
            abort_message = f"Aborted by user: {prompt_message()}"
            _emit_error(run_context, abort_message)
            raise click.exceptions.Exit(1)
