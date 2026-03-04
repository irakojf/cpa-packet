"""CLI helpers for the privacy guardrail command."""

from __future__ import annotations

from pathlib import Path

import click

from cpapacket.privacy import (
    PATTERNS,
    scan_fixtures_for_patterns,
    scan_repo_for_sensitive_paths,
)


def register_privacy_command(cli_group: click.Group) -> None:
    """Register the `privacy` command group and its subcommands."""

    @cli_group.group("privacy")
    def privacy_group() -> None:
        """Privacy and preflight checks."""

    @privacy_group.command("check")
    def privacy_check() -> None:
        """Scan the repo tree and fixtures for sensitive artifacts."""

        root = Path.cwd()
        path_failures = scan_repo_for_sensitive_paths(root)
        fixture_failures = scan_fixtures_for_patterns(root, patterns=PATTERNS)

        if not path_failures and not fixture_failures:
            click.secho("Privacy check passed (no sensitive artifacts detected).", fg="green")
            return

        click.secho("Privacy guardrails detected violations:", fg="red", err=True)
        for violation in path_failures:
            click.echo(f"  {violation.path}: {violation.reason}", err=True)
        for pattern_violation in fixture_failures:
            pattern_line = (
                f"  {pattern_violation.path}:{pattern_violation.line_no} "
                f"[{pattern_violation.pattern}] {pattern_violation.excerpt}"
            )
            click.echo(
                pattern_line,
                err=True,
            )
        raise click.ClickException("Privacy check failed.")
