"""Packet deliverable validation engine."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Literal, cast

from cpapacket.core.filesystem import atomic_write
from cpapacket.core.metadata import DeliverableMetadata, read_deliverable_metadata
from cpapacket.deliverables.base import Deliverable
from cpapacket.deliverables.registry import DELIVERABLE_REGISTRY, get_ordered_registry

ValidationStatus = Literal["present", "missing", "incomplete", "skipped"]


@dataclass(frozen=True)
class DeliverableValidationRecord:
    """Validation details for a single deliverable."""

    key: str
    required: bool
    status: ValidationStatus
    expected_patterns: tuple[str, ...]
    found_files: tuple[str, ...]
    missing_patterns: tuple[str, ...]


@dataclass(frozen=True)
class ValidationResult:
    """Packet-wide validation results for all deliverables in scope."""

    records: tuple[DeliverableValidationRecord, ...]

    def counts_by_status(self) -> dict[str, int]:
        counts: dict[str, int] = {
            "present": 0,
            "missing": 0,
            "incomplete": 0,
            "skipped": 0,
        }
        for record in self.records:
            counts[record.status] = counts.get(record.status, 0) + 1
        return counts

    def review_required(self) -> bool:
        return any(record.status in {"missing", "incomplete"} for record in self.records)


def validate_packet_deliverables(
    *,
    packet_root: Path | str,
    registry: tuple[Deliverable, ...] | None = None,
    skipped_keys: set[str] | None = None,
    gusto_available: bool = True,
) -> ValidationResult:
    """Validate expected deliverable artifacts for a generated packet."""
    root = Path(packet_root)
    normalized_skips = skipped_keys or set()
    ordered_registry = tuple(get_ordered_registry(registry=registry or DELIVERABLE_REGISTRY))

    all_files = _list_packet_files(root)
    records: list[DeliverableValidationRecord] = []

    for deliverable in ordered_registry:
        skip_for_flags = deliverable.key in normalized_skips or (
            deliverable.requires_gusto and not gusto_available
        )
        if skip_for_flags:
            records.append(
                DeliverableValidationRecord(
                    key=deliverable.key,
                    required=deliverable.required,
                    status="skipped",
                    expected_patterns=(),
                    found_files=(),
                    missing_patterns=(),
                )
            )
            continue

        metadata = _read_metadata_if_present(root=root, deliverable_key=deliverable.key)
        expected_patterns = _expected_patterns(
            deliverable=deliverable,
            metadata_artifacts=metadata.artifacts if metadata is not None else None,
        )

        found_files = _match_patterns(
            all_files=all_files,
            expected_patterns=expected_patterns,
        )
        missing_patterns = tuple(
            pattern
            for pattern in expected_patterns
            if not _matches_any_pattern(all_files=all_files, pattern=pattern)
        )

        if metadata is None:
            status: ValidationStatus = "incomplete" if found_files else "missing"
        elif missing_patterns:
            status = "incomplete"
        else:
            status = "present"

        records.append(
            DeliverableValidationRecord(
                key=deliverable.key,
                required=deliverable.required,
                status=status,
                expected_patterns=expected_patterns,
                found_files=found_files,
                missing_patterns=missing_patterns,
            )
        )

    return ValidationResult(records=tuple(records))


def render_validation_report(result: ValidationResult) -> str:
    """Render a text validation summary suitable for ``_meta/public`` output."""
    counts = result.counts_by_status()
    lines: list[str] = [
        "CPA Packet Validation Report",
        "==========================",
        "",
        "Summary",
        f"- Present: {counts.get('present', 0)}",
        f"- Missing: {counts.get('missing', 0)}",
        f"- Incomplete: {counts.get('incomplete', 0)}",
        f"- Skipped: {counts.get('skipped', 0)}",
        f"- Review Required: {'YES' if result.review_required() else 'NO'}",
        "",
        "Deliverables",
    ]

    for record in result.records:
        lines.append(f"- {record.key} [{record.status.upper()}]")
        if record.expected_patterns:
            lines.append("  expected:")
            lines.extend(f"  - {pattern}" for pattern in record.expected_patterns)
        else:
            lines.append("  expected: (none)")

        if record.found_files:
            lines.append("  found:")
            lines.extend(f"  - {path}" for path in record.found_files)
        else:
            lines.append("  found: (none)")

        if record.missing_patterns:
            lines.append("  missing:")
            lines.extend(f"  - {pattern}" for pattern in record.missing_patterns)
            lines.append("  flag: REVIEW_REQUIRED")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def write_validation_report(*, output_root: Path | str, result: ValidationResult) -> Path:
    """Write ``_meta/public/validation_report.txt`` atomically and return path."""
    destination = Path(output_root) / "_meta" / "public" / "validation_report.txt"
    destination.parent.mkdir(parents=True, exist_ok=True)
    payload = render_validation_report(result)
    with atomic_write(destination, mode="w", encoding="utf-8", newline="\n") as handle:
        cast(IO[str], handle).write(payload)
    return destination


def _read_metadata_if_present(*, root: Path, deliverable_key: str) -> DeliverableMetadata | None:
    candidate_paths = (
        root / "_meta" / f"{deliverable_key}_metadata.json",
        root / "_meta" / "private" / "deliverables" / f"{deliverable_key}_metadata.json",
    )
    for path in candidate_paths:
        if path.exists():
            return read_deliverable_metadata(path)
    return None


def _expected_patterns(
    *,
    deliverable: Deliverable,
    metadata_artifacts: list[str] | None,
) -> tuple[str, ...]:
    if metadata_artifacts:
        return tuple(_exact_path_pattern(path) for path in metadata_artifacts)

    folder = deliverable.folder.strip("/")
    if folder:
        return (rf"^{re.escape(folder)}/[^/].+$",)
    return ()


def _exact_path_pattern(path: str) -> str:
    normalized = path.replace("\\", "/").lstrip("/")
    return rf"^{re.escape(normalized)}$"


def _list_packet_files(root: Path) -> tuple[str, ...]:
    if not root.exists():
        return ()

    output: list[str] = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(root).as_posix()
        if rel.endswith(".tmp"):
            continue
        output.append(rel)
    return tuple(sorted(output))


def _matches_any_pattern(*, all_files: tuple[str, ...], pattern: str) -> bool:
    regex = re.compile(pattern)
    return any(regex.search(path) for path in all_files)


def _match_patterns(
    *,
    all_files: tuple[str, ...],
    expected_patterns: tuple[str, ...],
) -> tuple[str, ...]:
    found: list[str] = []
    for path in all_files:
        if any(re.search(pattern, path) for pattern in expected_patterns):
            found.append(path)
    return tuple(found)
