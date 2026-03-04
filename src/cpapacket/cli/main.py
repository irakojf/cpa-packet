"""Top-level click entrypoint for cpapacket."""

from __future__ import annotations

import os
import secrets
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, Literal, cast

import click

from cpapacket.cli.doctor import register_doctor_command
from cpapacket.cli.general_ledger import register_general_ledger_command
from cpapacket.cli.pnl import register_pnl_command
from cpapacket.cli.privacy import register_privacy_command
from cpapacket.clients.auth import OAuthTokenStore
from cpapacket.clients.gusto import GustoOAuthClient, GustoOAuthConfig
from cpapacket.clients.qbo import QboOAuthClient, QboOAuthConfig
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


def _detect_gusto_availability() -> bool:
    """Return True when a Gusto OAuth token is available for this environment."""
    try:
        return OAuthTokenStore("gusto").load_token() is not None
    except Exception:
        # Gusto auth is optional; detection failures should never block execution.
        return False


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise click.ClickException(f"Missing required environment variable: {name}")
    return value


def _build_qbo_client(*, realm_id: str | None = None) -> QboOAuthClient:
    return QboOAuthClient(
        QboOAuthConfig(
            client_id=_required_env("CPAPACKET_QBO_CLIENT_ID"),
            client_secret=_required_env("CPAPACKET_QBO_CLIENT_SECRET"),
            redirect_uri=_required_env("CPAPACKET_QBO_REDIRECT_URI"),
            realm_id=realm_id or os.getenv("CPAPACKET_QBO_REALM_ID"),
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
        gusto_available=_detect_gusto_availability(),
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


register_pnl_command(cli)
register_general_ledger_command(cli)
register_doctor_command(cli)
register_privacy_command(cli)
register_doctor_command(cli)


@cli.command("context-debug")
@click.pass_context
def context_debug(ctx: click.Context) -> None:
    """Print resolved RunContext as JSON for debugging."""
    run_context = ctx.obj["run_context"]
    click.echo(run_context.model_dump_json(indent=2))


@cli.group("auth")
def auth_group() -> None:
    """Authentication commands for external providers."""


@auth_group.group("qbo")
def auth_qbo_group() -> None:
    """QuickBooks Online OAuth commands."""


@auth_qbo_group.command("login")
@click.option("--state", type=str, default=None, help="OAuth state override.")
@click.option("--code", type=str, default=None, help="Authorization code from OAuth callback.")
@click.option("--code-verifier", type=str, default=None, help="PKCE code verifier.")
@click.option("--realm-id", type=str, default=None, help="QBO realm/company ID.")
def auth_qbo_login(
    state: str | None,
    code: str | None,
    code_verifier: str | None,
    realm_id: str | None,
) -> None:
    """Start QBO login or exchange an auth code for a token."""
    client = _build_qbo_client(realm_id=realm_id)
    if code:
        if not code_verifier:
            raise click.ClickException("--code-verifier is required when --code is provided.")
        token = client.exchange_code_for_token(code=code, code_verifier=code_verifier)
        click.echo(
            f"QBO token saved. Expires at {token.expires_at.isoformat()}."
        )
        return

    oauth_state = state or secrets.token_urlsafe(16)
    auth_url, verifier = client.authorization_url(state=oauth_state)
    click.echo(f"Open this URL in your browser:\n{auth_url}")
    click.echo(f"Use this code verifier for token exchange:\n{verifier}")
    click.echo("After callback, run:")
    click.echo("cpapacket auth qbo login --code <AUTH_CODE> --code-verifier <VERIFIER>")


@auth_qbo_group.command("status")
def auth_qbo_status() -> None:
    """Show whether a QBO token is currently stored."""
    token = OAuthTokenStore("qbo").load_token()
    if token is None:
        click.echo("QBO status: not authenticated")
        return
    state = "expired" if token.is_expired() else "active"
    click.echo(f"QBO status: authenticated ({state})")
    click.echo(f"Token expiry: {token.expires_at.isoformat()}")


@auth_qbo_group.command("logout")
def auth_qbo_logout() -> None:
    """Clear stored QBO token."""
    OAuthTokenStore("qbo").clear_token()
    click.echo("QBO token cleared.")


@auth_group.group("gusto")
def auth_gusto_group() -> None:
    """Gusto OAuth commands."""


@auth_gusto_group.command("login")
@click.option("--state", type=str, default=None, help="OAuth state override.")
@click.option("--code", type=str, default=None, help="Authorization code from OAuth callback.")
@click.option("--code-verifier", type=str, default=None, help="PKCE code verifier.")
def auth_gusto_login(
    state: str | None,
    code: str | None,
    code_verifier: str | None,
) -> None:
    """Start Gusto login or exchange an auth code for a token."""
    client = _build_gusto_client()
    if code:
        if not code_verifier:
            raise click.ClickException("--code-verifier is required when --code is provided.")
        token = client.exchange_code_for_token(code=code, code_verifier=code_verifier)
        click.echo(f"Gusto token saved. Expires at {token.expires_at.isoformat()}.")
        return

    oauth_state = state or secrets.token_urlsafe(16)
    auth_url, verifier = client.authorization_url(state=oauth_state)
    click.echo(f"Open this URL in your browser:\n{auth_url}")
    click.echo(f"Use this code verifier for token exchange:\n{verifier}")
    click.echo("After callback, run:")
    click.echo("cpapacket auth gusto login --code <AUTH_CODE> --code-verifier <VERIFIER>")


@auth_gusto_group.command("status")
def auth_gusto_status() -> None:
    """Show whether a Gusto token is currently stored."""
    token = OAuthTokenStore("gusto").load_token()
    if token is None:
        click.echo("Gusto status: not configured")
        return
    state = "expired" if token.is_expired() else "active"
    click.echo(f"Gusto status: authenticated ({state})")
    click.echo(f"Token expiry: {token.expires_at.isoformat()}")


@auth_gusto_group.command("logout")
def auth_gusto_logout() -> None:
    """Clear stored Gusto token."""
    OAuthTokenStore("gusto").clear_token()
    click.echo("Gusto token cleared.")


def main(argv: list[str] | None = None) -> Any:
    return cli.main(args=argv, standalone_mode=False)
