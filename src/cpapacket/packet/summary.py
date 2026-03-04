"""Packet summary markdown generation."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, cast

from cpapacket.core.filesystem import atomic_write


@dataclass(frozen=True)
class PacketSummaryDeliverable:
    """One deliverable row in packet summary output."""

    key: str
    status: str
    reason: str | None = None


@dataclass(frozen=True)
class PacketSummary:
    """Structured packet summary payload for markdown rendering."""

    tool_version: str
    year: int
    accounting_method: str
    deliverables: tuple[PacketSummaryDeliverable, ...] = ()
    validation_warnings: tuple[str, ...] = ()
    reconciliation_flags: tuple[str, ...] = ()
    payroll_available: bool = True
    notes: tuple[str, ...] = field(default_factory=tuple)


def render_packet_summary(summary: PacketSummary) -> str:
    """Render human-readable markdown summary file content."""
    lines: list[str] = [
        "# Packet Summary",
        "",
        "## Run Details",
        f"- Tool Version: {summary.tool_version}",
        f"- Tax Year: {summary.year}",
        f"- Accounting Method: {summary.accounting_method}",
        f"- Payroll Available: {'Yes' if summary.payroll_available else 'No'}",
        "",
        "## Deliverables",
    ]

    if summary.deliverables:
        for item in summary.deliverables:
            entry = f"- {item.key}: {item.status}"
            if item.reason:
                entry += f" ({item.reason})"
            lines.append(entry)
    else:
        lines.append("- None")

    lines.extend(["", "## Validation Warnings"])
    if summary.validation_warnings:
        lines.extend(f"- {warning}" for warning in summary.validation_warnings)
    else:
        lines.append("- None")

    lines.extend(["", "## Reconciliation Flags"])
    if summary.reconciliation_flags:
        lines.extend(f"- {flag}" for flag in summary.reconciliation_flags)
    else:
        lines.append("- None")

    if summary.notes:
        lines.extend(["", "## Notes"])
        lines.extend(f"- {note}" for note in summary.notes)

    return "\n".join(lines).rstrip() + "\n"


def write_packet_summary(*, output_root: Path | str, summary: PacketSummary) -> Path:
    """Write ``00_PACKET_SUMMARY.md`` at packet root atomically."""
    destination = Path(output_root) / "00_PACKET_SUMMARY.md"
    destination.parent.mkdir(parents=True, exist_ok=True)
    payload = render_packet_summary(summary)
    with atomic_write(destination, mode="w", encoding="utf-8", newline="\n") as handle:
        cast(IO[str], handle).write(payload)
    return destination
