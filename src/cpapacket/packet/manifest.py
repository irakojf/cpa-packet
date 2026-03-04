"""Packet manifest generation for build summaries."""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from cpapacket.core.filesystem import atomic_write

DeliverableStatus = Literal["success", "warning", "missing", "skipped", "error"]


class DeliverableManifestEntry(BaseModel):
    """One deliverable execution record in the packet manifest."""

    model_config = ConfigDict(frozen=True)

    key: str
    required: bool
    status: DeliverableStatus
    artifacts: list[str] = Field(default_factory=list)
    timing_ms: int = Field(ge=0)
    warnings: list[str] = Field(default_factory=list)


class ValidationSummary(BaseModel):
    """Status counts and recommended process exit code."""

    model_config = ConfigDict(frozen=True)

    counts_by_status: dict[str, int]
    recommended_exit_code: int = Field(ge=0)


class PacketManifest(BaseModel):
    """Serialized packet-level run metadata written under ``_meta/public``."""

    model_config = ConfigDict(frozen=True)

    tool_version: str
    run_id: str
    year: int
    method: str
    started_at: str
    finished_at: str
    deliverables: list[DeliverableManifestEntry]
    validation_summary: ValidationSummary


def write_packet_manifest(
    *,
    output_root: str | Path,
    tool_version: str,
    run_id: str,
    year: int,
    method: str,
    started_at: datetime | str,
    finished_at: datetime | str,
    deliverables: list[DeliverableManifestEntry],
) -> Path:
    """Write ``_meta/public/packet_manifest.json`` atomically and return its path."""
    manifest = PacketManifest(
        tool_version=tool_version,
        run_id=run_id,
        year=year,
        method=method,
        started_at=_to_iso8601(started_at),
        finished_at=_to_iso8601(finished_at),
        deliverables=deliverables,
        validation_summary=_build_validation_summary(deliverables),
    )

    destination = Path(output_root) / "_meta" / "public" / "packet_manifest.json"
    destination.parent.mkdir(parents=True, exist_ok=True)

    payload = manifest.model_dump(mode="json")
    with atomic_write(destination, mode="w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")

    return destination


def _build_validation_summary(
    deliverables: list[DeliverableManifestEntry],
) -> ValidationSummary:
    counts = Counter(item.status for item in deliverables)
    has_warnings = any(item.warnings for item in deliverables)

    recommended_exit_code = 0
    if counts.get("warning", 0) > 0 or counts.get("missing", 0) > 0 or has_warnings:
        recommended_exit_code = 2
    if counts.get("error", 0) > 0:
        recommended_exit_code = 2

    return ValidationSummary(
        counts_by_status=dict(sorted(counts.items())),
        recommended_exit_code=recommended_exit_code,
    )


def _to_iso8601(value: datetime | str) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return value.strip()
