"""Build CLI command orchestrating full packet generation."""

from __future__ import annotations

import json
import os
from collections.abc import Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path, PurePosixPath
from typing import Any, cast

import click
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table
from rich.tree import Tree

from cpapacket.clients.auth import OAuthTokenStore
from cpapacket.clients.gusto import GustoOAuthClient, GustoOAuthConfig
from cpapacket.clients.qbo import QboOAuthClient, QboOAuthConfig
from cpapacket.core.context import RunContext
from cpapacket.data.providers import DataProviders
from cpapacket.data.store import SessionDataStore
from cpapacket.deliverables.base import DeliverableResult
from cpapacket.deliverables.registry import get_ordered_registry
from cpapacket.packet.manifest import DeliverableManifestEntry, write_packet_manifest
from cpapacket.packet.summary import PacketSummary, PacketSummaryDeliverable, write_packet_summary
from cpapacket.packet.validator import (
    ValidationResult,
    validate_packet_deliverables,
    write_validation_report,
)
from cpapacket.packet.zipper import create_packet_zip

_CONCURRENT_STEP_KEYS = frozenset(
    {
        "pnl",
        "balance_sheet",
        "prior_balance_sheet",
        "general_ledger",
    }
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


def _build_optional_gusto_client() -> GustoOAuthClient | None:
    client_id = os.getenv("CPAPACKET_GUSTO_CLIENT_ID", "").strip()
    client_secret = os.getenv("CPAPACKET_GUSTO_CLIENT_SECRET", "").strip()
    redirect_uri = os.getenv("CPAPACKET_GUSTO_REDIRECT_URI", "").strip()
    if not client_id or not client_secret or not redirect_uri:
        return None

    return GustoOAuthClient(
        GustoOAuthConfig(
            client_id=client_id,
            client_secret=client_secret,
            redirect_uri=redirect_uri,
        )
    )


def _tool_version() -> str:
    try:
        return version("cpapacket")
    except PackageNotFoundError:
        return "0.0.0"


def _format_file_size(size_bytes: int) -> str:
    if size_bytes < 1024:
        return f"{size_bytes} B"
    if size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    return f"{size_bytes / (1024 * 1024):.1f} MB"


def _artifact_tree_rows(
    *,
    packet_root: Path,
    manifest_entries: Sequence[DeliverableManifestEntry],
) -> list[tuple[PurePosixPath, str, str]]:
    rows: list[tuple[PurePosixPath, str, str]] = []
    seen: set[PurePosixPath] = set()
    for entry in manifest_entries:
        for artifact in entry.artifacts:
            rel_path = PurePosixPath(artifact)
            if rel_path in seen:
                continue
            seen.add(rel_path)
            absolute_path = packet_root / rel_path
            if absolute_path.exists():
                size_label = _format_file_size(absolute_path.stat().st_size)
                status = "Present"
            else:
                size_label = "n/a"
                status = "Missing"
            rows.append((rel_path, size_label, status))
    return sorted(rows, key=lambda row: row[0].as_posix())


def _render_packet_tree_text(
    *,
    packet_root: Path,
    manifest_entries: Sequence[DeliverableManifestEntry],
    plain: bool,
) -> str:
    rows = _artifact_tree_rows(packet_root=packet_root, manifest_entries=manifest_entries)
    if not rows:
        return "Packet directory tree: no generated artifacts."

    if plain:
        lines = ["Packet directory tree:"]
        for rel_path, size_label, status in rows:
            lines.append(f"- {rel_path.as_posix()} ({size_label}) [{status}]")
        return "\n".join(lines)

    root = Tree(f"Packet directory: {packet_root.name}")
    node_by_dir: dict[PurePosixPath, Tree] = {PurePosixPath("."): root}
    for rel_path, size_label, status in rows:
        parent = PurePosixPath(".")
        for part in rel_path.parts[:-1]:
            current = parent / part
            if current not in node_by_dir:
                node_by_dir[current] = node_by_dir[parent].add(f"[bold]{part}/[/bold]")
            parent = current
        status_markup = "[green]Present[/green]" if status == "Present" else "[red]Missing[/red]"
        node_by_dir[parent].add(f"{rel_path.name} [dim]({size_label})[/dim] {status_markup}")

    console = Console(record=True, force_terminal=False, color_system=None, width=120)
    console.print(root)
    return console.export_text().rstrip()


def _render_status_panel_text(
    *,
    title: str,
    message: str,
    plain: bool,
    level: str,
) -> str:
    if plain:
        return f"{title}: {message}"

    border_style = {
        "success": "green",
        "warning": "yellow",
        "error": "red",
    }.get(level, "white")
    panel = Panel.fit(message, title=title, border_style=border_style)
    console = Console(record=True, force_terminal=False, color_system=None, width=120)
    console.print(panel)
    return console.export_text().rstrip()


def _render_validation_summary_text(
    *,
    validation: ValidationResult,
    manifest_entries: Sequence[DeliverableManifestEntry],
    plain: bool,
) -> str:
    if not validation.records:
        return "Validation summary: no records."

    warning_counts = {entry.key: len(entry.warnings) for entry in manifest_entries}
    if plain:
        lines = ["Validation summary:", "deliverable | status | files | warnings"]
        for record in sorted(validation.records, key=lambda item: item.key):
            warning_count = warning_counts.get(record.key, 0) + len(record.missing_patterns)
            lines.append(
                f"{record.key} | {record.status} | {len(record.found_files)} | {warning_count}"
            )
        return "\n".join(lines)

    table = Table(title="Validation Summary", show_lines=False)
    table.add_column("Deliverable")
    table.add_column("Status")
    table.add_column("Files", justify="right")
    table.add_column("Warnings", justify="right")

    for record in sorted(validation.records, key=lambda item: item.key):
        status_markup = {
            "present": "[green]Present[/green]",
            "missing": "[red]Missing[/red]",
            "incomplete": "[yellow]Incomplete[/yellow]",
            "skipped": "[dim]Skipped[/dim]",
        }.get(record.status, record.status)
        warning_count = warning_counts.get(record.key, 0) + len(record.missing_patterns)
        table.add_row(
            record.key,
            status_markup,
            str(len(record.found_files)),
            str(warning_count),
        )

    console = Console(record=True, force_terminal=False, color_system=None, width=120)
    console.print(table)
    return console.export_text().rstrip()


def _ensure_qbo_authenticated() -> None:
    token = OAuthTokenStore("qbo").load_token()
    if token is None:
        raise click.ClickException(
            "QBO authentication is required. Run `cpapacket auth qbo login` and retry."
        )


def _metadata_path_candidates(*, packet_root: Path, deliverable_key: str) -> tuple[Path, ...]:
    return (
        packet_root / "_meta" / f"{deliverable_key}_metadata.json",
        packet_root / "_meta" / "private" / "deliverables" / f"{deliverable_key}_metadata.json",
    )


def _cached_artifacts(*, packet_root: Path, deliverable_key: str) -> list[str]:
    for metadata_path in _metadata_path_candidates(
        packet_root=packet_root,
        deliverable_key=deliverable_key,
    ):
        if not metadata_path.exists():
            continue
        try:
            payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        artifacts = payload.get("artifacts")
        if not isinstance(artifacts, Sequence) or isinstance(artifacts, (str, bytes)):
            continue
        return [item for item in artifacts if isinstance(item, str) and item]
    return []


def _generate_deliverable(
    *,
    deliverable: Any,
    run_context: RunContext,
    providers: DataProviders,
    prompts_by_key: dict[str, dict[str, Any]],
) -> tuple[DeliverableResult, int]:
    start_ns = datetime.now(UTC)
    try:
        result = deliverable.generate(
            run_context,
            providers,
            prompts_by_key.get(deliverable.key, {}),
        )
    except Exception as exc:
        result = DeliverableResult(
            deliverable_key=deliverable.key,
            success=False,
            artifacts=[],
            warnings=[],
            error=str(exc),
        )
    elapsed_ms = max(int((datetime.now(UTC) - start_ns).total_seconds() * 1000), 0)
    return result, elapsed_ms


def register_build_command(cli_group: click.Group) -> None:
    """Register `cpapacket build` command."""

    @cli_group.command("build")
    @click.option(
        "--skip",
        "skip_keys",
        multiple=True,
        help="Deliverable key(s) to skip (repeat flag for multiple keys).",
    )
    @click.option(
        "--validate-only",
        is_flag=True,
        help="Validate existing packet artifacts without generating deliverables.",
    )
    @click.option(
        "--continue-on-failure/--fail-fast",
        default=True,
        show_default=True,
        help="Continue remaining deliverables after an error.",
    )
    @click.pass_context
    def build_command(
        ctx: click.Context,
        skip_keys: tuple[str, ...],
        validate_only: bool,
        continue_on_failure: bool,
    ) -> None:
        """Generate full CPA packet deliverables in dependency-safe order."""
        run_context = cast(RunContext | None, ctx.obj.get("run_context") if ctx.obj else None)
        if run_context is None:
            raise click.ClickException("Run context is not initialized.")

        user_requested_skips = {item.strip() for item in run_context.skip + list(skip_keys) if item}

        if validate_only:
            validation = validate_packet_deliverables(
                packet_root=run_context.out_dir,
                skipped_keys=user_requested_skips,
                gusto_available=run_context.gusto_available,
            )
            validation_report_path = write_validation_report(
                output_root=run_context.out_dir,
                result=validation,
            )
            archive_path = create_packet_zip(
                packet_root=run_context.out_dir,
                on_conflict=run_context.on_conflict,
                non_interactive=run_context.non_interactive,
                include_debug_log=run_context.include_debug,
            )
            click.echo("Validation-only complete.")
            click.echo(f"Validation report: {validation_report_path}")
            click.echo(f"Archive: {archive_path}")
            exit_code = validation.recommended_exit_code()
            if exit_code != 0:
                raise click.exceptions.Exit(exit_code)
            return

        _ensure_qbo_authenticated()

        started_at = datetime.now(UTC)
        gusto_client = _build_optional_gusto_client()
        gusto_available = run_context.gusto_available and gusto_client is not None
        if run_context.gusto_available and gusto_client is None:
            click.echo(
                "WARNING: Gusto token present but OAuth env vars are missing; "
                "payroll deliverables will be skipped."
            )
        elif not run_context.gusto_available:
            click.echo("WARNING: Gusto not authenticated; payroll deliverables will be skipped.")

        cache_dir: Path | None = run_context.out_dir / "_meta" / "private" / "cache"
        if run_context.force:
            cache_dir = None
        store = SessionDataStore(cache_dir=cache_dir)
        providers = DataProviders(
            store=store,
            qbo_client=_build_qbo_client(),
            gusto_client=gusto_client if gusto_available else None,
        )

        ordered = get_ordered_registry()
        manifest_entries: list[DeliverableManifestEntry] = []
        summary_rows: list[PacketSummaryDeliverable] = []
        skipped_keys: set[str] = set()
        prompts_by_key: dict[str, dict[str, Any]] = {}
        active_deliverables: list[Any] = []
        has_hard_failures = False

        for deliverable in ordered:
            if deliverable.key in user_requested_skips:
                skipped_keys.add(deliverable.key)
                manifest_entries.append(
                    DeliverableManifestEntry(
                        key=deliverable.key,
                        required=deliverable.required,
                        status="skipped",
                        artifacts=[],
                        timing_ms=0,
                        warnings=[],
                    )
                )
                summary_rows.append(
                    PacketSummaryDeliverable(
                        key=deliverable.key,
                        status="skipped",
                        reason="user_skip",
                    )
                )
                continue

            if deliverable.requires_gusto and not gusto_available:
                skipped_keys.add(deliverable.key)
                manifest_entries.append(
                    DeliverableManifestEntry(
                        key=deliverable.key,
                        required=deliverable.required,
                        status="skipped",
                        artifacts=[],
                        timing_ms=0,
                        warnings=[],
                    )
                )
                summary_rows.append(
                    PacketSummaryDeliverable(
                        key=deliverable.key,
                        status="skipped",
                        reason="gusto_unavailable",
                    )
                )
                continue

            if (
                run_context.incremental
                and not run_context.force
                and deliverable.is_current(run_context)
            ):
                skipped_keys.add(deliverable.key)
                cached_artifacts = _cached_artifacts(
                    packet_root=run_context.out_dir,
                    deliverable_key=deliverable.key,
                )
                manifest_entries.append(
                    DeliverableManifestEntry(
                        key=deliverable.key,
                        required=deliverable.required,
                        status="skipped",
                        artifacts=cached_artifacts,
                        timing_ms=0,
                        warnings=["Skipped incremental run; deliverable is current."],
                    )
                )
                summary_rows.append(
                    PacketSummaryDeliverable(
                        key=deliverable.key,
                        status="skipped",
                        reason="incremental_current",
                    )
                )
                continue
            prompts_by_key[deliverable.key] = deliverable.gather_prompts(run_context)
            active_deliverables.append(deliverable)

        def _record_result(
            deliverable: Any,
            result: DeliverableResult,
            elapsed_ms: int,
        ) -> None:
            status = "warning" if result.warnings else "success"
            if not result.success:
                status = "error"

            manifest_entries.append(
                DeliverableManifestEntry(
                    key=deliverable.key,
                    required=deliverable.required,
                    status=cast(Any, status),
                    artifacts=list(result.artifacts),
                    timing_ms=elapsed_ms,
                    warnings=list(result.warnings),
                )
            )
            summary_rows.append(
                PacketSummaryDeliverable(
                    key=deliverable.key,
                    status=status,
                    reason=result.error,
                )
            )

        concurrent_deliverables = [
            deliverable
            for deliverable in active_deliverables
            if deliverable.key in _CONCURRENT_STEP_KEYS
        ]
        concurrent_by_key = {
            deliverable.key: deliverable for deliverable in concurrent_deliverables
        }
        concurrent_results: dict[str, tuple[DeliverableResult, int]] = {}
        if len(concurrent_deliverables) > 1:
            with ThreadPoolExecutor(max_workers=min(4, len(concurrent_deliverables))) as executor:
                futures_by_key: dict[str, Future[tuple[DeliverableResult, int]]] = {
                    deliverable.key: executor.submit(
                        _generate_deliverable,
                        deliverable=deliverable,
                        run_context=run_context,
                        providers=providers,
                        prompts_by_key=prompts_by_key,
                    )
                    for deliverable in concurrent_deliverables
                }
                for key, future in futures_by_key.items():
                    concurrent_results[key] = future.result()

        def _run_deliverable(deliverable: Any) -> tuple[DeliverableResult, int]:
            if deliverable.key in concurrent_by_key and len(concurrent_deliverables) > 1:
                return concurrent_results[deliverable.key]
            return _generate_deliverable(
                deliverable=deliverable,
                run_context=run_context,
                providers=providers,
                prompts_by_key=prompts_by_key,
            )

        if run_context.plain:
            for deliverable in active_deliverables:
                result, elapsed_ms = _run_deliverable(deliverable)
                _record_result(deliverable, result, elapsed_ms)

                if result.success:
                    continue
                has_hard_failures = True
                if continue_on_failure:
                    warning_message = result.error or "generation error"
                    click.echo(f"WARNING: {deliverable.key} failed: {warning_message}")
                    continue
                raise click.ClickException(result.error or f"{deliverable.key} generation failed.")
        else:
            with Progress(
                SpinnerColumn(),
                TextColumn("{task.description}"),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
                transient=True,
            ) as progress:
                progress_task_id = progress.add_task(
                    "Generating deliverables...",
                    total=len(active_deliverables),
                )
                for deliverable in active_deliverables:
                    progress.update(
                        progress_task_id, description=f"Generating {deliverable.key}..."
                    )
                    result, elapsed_ms = _run_deliverable(deliverable)
                    _record_result(deliverable, result, elapsed_ms)
                    progress.advance(progress_task_id, 1)

                    if result.success:
                        continue
                    has_hard_failures = True
                    if continue_on_failure:
                        warning_message = result.error or "generation error"
                        click.echo(f"WARNING: {deliverable.key} failed: {warning_message}")
                        continue
                    raise click.ClickException(
                        result.error or f"{deliverable.key} generation failed."
                    )

        validation = validate_packet_deliverables(
            packet_root=run_context.out_dir,
            skipped_keys=skipped_keys,
            gusto_available=gusto_available,
        )
        validation_report_path = write_validation_report(
            output_root=run_context.out_dir,
            result=validation,
        )

        finished_at = datetime.now(UTC)
        manifest_path = write_packet_manifest(
            output_root=run_context.out_dir,
            tool_version=_tool_version(),
            run_id=run_context.run_id,
            year=run_context.year,
            method=run_context.method,
            started_at=started_at,
            finished_at=finished_at,
            deliverables=manifest_entries,
        )

        validation_warnings = tuple(
            f"{record.key}:{record.status}"
            for record in validation.records
            if record.status in {"missing", "incomplete"}
        )
        summary_path = write_packet_summary(
            output_root=run_context.out_dir,
            summary=PacketSummary(
                tool_version=_tool_version(),
                year=run_context.year,
                accounting_method=run_context.method,
                deliverables=tuple(summary_rows),
                validation_warnings=validation_warnings,
                payroll_available=gusto_available,
            ),
        )

        archive_path = create_packet_zip(
            packet_root=run_context.out_dir,
            on_conflict=run_context.on_conflict,
            non_interactive=run_context.non_interactive,
            include_debug_log=run_context.include_debug,
        )
        packet_tree_text = _render_packet_tree_text(
            packet_root=run_context.out_dir,
            manifest_entries=manifest_entries,
            plain=run_context.plain,
        )

        click.echo("Build complete.")
        click.echo(f"Validation report: {validation_report_path}")
        click.echo(f"Packet summary: {summary_path}")
        click.echo(f"Packet manifest: {manifest_path}")
        click.echo(f"Archive: {archive_path}")
        click.echo(packet_tree_text)
        click.echo(
            _render_validation_summary_text(
                validation=validation,
                manifest_entries=manifest_entries,
                plain=run_context.plain,
            )
        )

        success_count = sum(1 for entry in manifest_entries if entry.status == "success")
        warning_count = sum(1 for entry in manifest_entries if entry.status == "warning")
        error_count = sum(1 for entry in manifest_entries if entry.status == "error")
        if has_hard_failures:
            click.echo(
                _render_status_panel_text(
                    title="Error",
                    message=(
                        f"{error_count} deliverable(s) failed. "
                        "See _meta/cpapacket.log and validation report for details."
                    ),
                    plain=run_context.plain,
                    level="error",
                )
            )
        elif validation.recommended_exit_code() != 0:
            click.echo(
                _render_status_panel_text(
                    title="Warning",
                    message=(
                        f"{warning_count} deliverable(s) completed with warnings "
                        "or missing artifacts. Review the validation report."
                    ),
                    plain=run_context.plain,
                    level="warning",
                )
            )
        else:
            click.echo(
                _render_status_panel_text(
                    title="Success",
                    message=f"{success_count} deliverable(s) completed successfully.",
                    plain=run_context.plain,
                    level="success",
                )
            )

        exit_code = 1 if has_hard_failures else validation.recommended_exit_code()
        if exit_code != 0:
            raise click.exceptions.Exit(exit_code)
