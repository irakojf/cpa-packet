"""Doctor CLI command wrapper."""

from __future__ import annotations

import os
from dataclasses import dataclass

import click
from rich.console import Console

from cpapacket.clients.auth import OAuthTokenStore
from cpapacket.clients.gusto import GustoOAuthClient, GustoOAuthConfig
from cpapacket.clients.qbo import QboOAuthClient, QboOAuthConfig
from cpapacket.packet.doctor import (
    DoctorCheckResult,
    run_gusto_connectivity_check,
    run_gusto_token_check,
    run_python_environment_check,
    run_qbo_connectivity_check,
    run_qbo_token_check,
)


@dataclass(frozen=True)
class DoctorCommandSummary:
    results: list[DoctorCheckResult]
    failure_count: int


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
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


def _run_doctor_checks() -> DoctorCommandSummary:
    qbo_store = OAuthTokenStore("qbo")
    gusto_store = OAuthTokenStore("gusto")

    qbo_token_result = run_qbo_token_check(
        load_token=qbo_store.load_token,
        refresh_probe=lambda refresh_token: _build_qbo_client().refresh_access_token(
            refresh_token
        ),
    )
    gusto_token_result = run_gusto_token_check(
        load_token=gusto_store.load_token,
        refresh_probe=lambda refresh_token: _build_gusto_client().refresh_access_token(
            refresh_token
        ),
    )

    def _gusto_identity_probe() -> dict[str, object]:
        response = _build_gusto_client().request("GET", "/companies/me", required=False)
        if response is None:
            return {}
        payload = response.json()
        if isinstance(payload, dict):
            return payload
        return {}

    results = [
        run_python_environment_check(),
        qbo_token_result,
        run_qbo_connectivity_check(
            company_info_probe=lambda: _build_qbo_client().get_company_info(),
        ),
        gusto_token_result,
        run_gusto_connectivity_check(
            load_token=gusto_store.load_token,
            company_identity_probe=_gusto_identity_probe,
        ),
    ]
    failure_count = sum(1 for result in results if result.status == "fail")
    return DoctorCommandSummary(results=results, failure_count=failure_count)


def _render_doctor_results(summary: DoctorCommandSummary) -> None:
    console = Console()
    for result in summary.results:
        is_pass = result.status == "pass"
        icon = "[green]✓[/green]" if is_pass else "[red]X[/red]"
        label = "PASS" if is_pass else "FAIL"
        console.print(f"{icon} [bold]{result.check_name}[/bold] ({label}): {result.summary}")
        for detail in result.details:
            console.print(f"  - {detail}")
        if result.guidance and not is_pass:
            console.print(f"  [yellow]Guidance:[/yellow] {result.guidance}")


def register_doctor_command(cli_group: click.Group) -> None:
    """Add `cpapacket doctor` command to the provided click group."""

    @cli_group.command("doctor")  # type: ignore[untyped-decorator]
    def doctor_command() -> None:
        """Run environment and auth health checks."""
        summary = _run_doctor_checks()
        _render_doctor_results(summary)
        if summary.failure_count > 0:
            raise click.exceptions.Exit(1)
