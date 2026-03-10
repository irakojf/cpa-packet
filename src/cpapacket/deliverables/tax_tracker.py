"""Estimated tax tracker deliverable orchestration."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, Any, cast

from platformdirs import user_config_path
from pydantic import ValidationError

from cpapacket.core.context import RunContext
from cpapacket.core.filesystem import atomic_write, ensure_directory
from cpapacket.core.metadata import (
    DeliverableMetadata,
    compute_input_fingerprint,
    write_deliverable_metadata,
)
from cpapacket.core.default_tax_deadlines import generate_default_tax_deadlines
from cpapacket.deliverables.base import DeliverableResult
from cpapacket.models.tax import EstimatedTaxPayment, TaxDeadline
from cpapacket.utils.constants import DELIVERABLE_FOLDERS, SCHEMA_VERSIONS
from cpapacket.utils.prompts import resolve_output_path
from cpapacket.writers.csv_writer import CsvWriter
from cpapacket.writers.pdf_writer import PdfTableRow, PdfWriter

_APP_NAME = "cpapacket"


class TaxTrackerDeliverable:
    """Deliverable that snapshots local estimated-tax tracker state into the packet."""

    key = "estimated_tax"
    folder = DELIVERABLE_FOLDERS["estimated_tax"]
    required = False
    dependencies: list[str] = []
    requires_gusto = False

    def __init__(self, *, config_root: Path | None = None) -> None:
        self._config_root = config_root

    def gather_prompts(self, _ctx: object) -> dict[str, Any]:
        return {}

    def is_current(self, _ctx: object) -> bool:
        return False

    def generate(
        self,
        ctx: RunContext,
        store: object,
        prompts: dict[str, Any],
    ) -> DeliverableResult:
        del prompts
        company_name = _extract_company_name(store)

        warnings: list[str] = []
        config_root = self._resolve_config_root()
        tracker_source = config_root / f"tax_tracker_{ctx.year}.json"
        deadlines_source = config_root / f"tax_deadlines_{ctx.year}.json"

        tracker_payload = _load_json_payload(
            tracker_source,
            label="Estimated tax tracker",
            warnings=warnings,
        )
        deadlines_payload = _load_json_payload(
            deadlines_source,
            label="Tax deadlines",
            warnings=warnings,
        )

        tracker_entries = _parse_tracker_entries(
            payload=tracker_payload,
            warnings=warnings,
            source_name=tracker_source.name,
        )
        deadline_entries = _parse_deadline_entries(
            payload=deadlines_payload,
            warnings=warnings,
            source_name=deadlines_source.name,
        )
        if not deadline_entries:
            deadline_entries = generate_default_tax_deadlines(year=ctx.year)

        deliverable_dir = ensure_directory(ctx.out_dir / self.folder)
        meta_dir = ensure_directory(ctx.out_dir / "_meta")
        cpa_dir = ensure_directory(deliverable_dir / "cpa")
        dev_dir = ensure_directory(deliverable_dir / "dev")

        tracker_csv_path = _resolve_output_path(
            cpa_dir / f"estimated_tax_tracker_{ctx.year}.csv",
            on_conflict=ctx.on_conflict,
            non_interactive=ctx.non_interactive,
        )
        tracker_pdf_path = _resolve_output_path(
            cpa_dir / f"estimated_tax_tracker_{ctx.year}.pdf",
            on_conflict=ctx.on_conflict,
            non_interactive=ctx.non_interactive,
        )
        deadlines_csv_path = _resolve_output_path(
            cpa_dir / f"tax_deadlines_{ctx.year}.csv",
            on_conflict=ctx.on_conflict,
            non_interactive=ctx.non_interactive,
        )
        deadlines_pdf_path = _resolve_output_path(
            cpa_dir / f"tax_deadlines_{ctx.year}.pdf",
            on_conflict=ctx.on_conflict,
            non_interactive=ctx.non_interactive,
        )
        metadata_path = _resolve_output_path(
            meta_dir / f"{self.key}_metadata.json",
            on_conflict=ctx.on_conflict,
            non_interactive=ctx.non_interactive,
        )

        _write_tracker_csv(path=tracker_csv_path, rows=tracker_entries)
        _write_deadlines_csv(path=deadlines_csv_path, rows=deadline_entries)
        _write_tracker_pdf(path=tracker_pdf_path, year=ctx.year, rows=tracker_entries, company_name=company_name)
        _write_deadlines_pdf(path=deadlines_pdf_path, year=ctx.year, rows=deadline_entries, company_name=company_name)

        artifacts: list[Path] = [
            tracker_csv_path,
            tracker_pdf_path,
            deadlines_csv_path,
            deadlines_pdf_path,
        ]

        artifacts.extend(
            _copy_source_json_if_present(
                source_path=tracker_source,
                deliverable_dir=dev_dir,
                meta_dir=meta_dir,
                ctx=ctx,
            )
        )
        artifacts.extend(
            _copy_source_json_if_present(
                source_path=deadlines_source,
                deliverable_dir=dev_dir,
                meta_dir=meta_dir,
                ctx=ctx,
            )
        )

        metadata_inputs = {
            "year": ctx.year,
            "tracker_source_present": tracker_source.exists(),
            "deadlines_source_present": deadlines_source.exists(),
            "tracker_entry_count": len(tracker_entries),
            "deadline_entry_count": len(deadline_entries),
        }
        metadata = DeliverableMetadata(
            deliverable=self.key,
            inputs=metadata_inputs,
            input_fingerprint=compute_input_fingerprint(metadata_inputs),
            schema_versions=SCHEMA_VERSIONS[self.key],
            artifacts=[str(path) for path in artifacts],
            warnings=warnings,
            data_sources={
                "tracker_json": str(tracker_source),
                "deadlines_json": str(deadlines_source),
            },
        )
        write_deliverable_metadata(metadata_path, metadata)

        return DeliverableResult(
            deliverable_key=self.key,
            success=True,
            artifacts=[str(path) for path in artifacts],
            warnings=warnings,
        )

    def _resolve_config_root(self) -> Path:
        if self._config_root is not None:
            return self._config_root
        return Path(user_config_path(_APP_NAME, _APP_NAME))


def _load_json_payload(path: Path, *, label: str, warnings: list[str]) -> object | None:
    if not path.exists():
        warnings.append(f"{label} source not found at {path}; generated empty outputs.")
        return None

    try:
        return cast(object, json.loads(path.read_text(encoding="utf-8")))
    except json.JSONDecodeError as exc:
        warnings.append(
            f"{label} source at {path} is invalid JSON ({exc.msg}); generated empty outputs."
        )
        return None


def _parse_tracker_entries(
    *,
    payload: object | None,
    warnings: list[str],
    source_name: str,
) -> list[EstimatedTaxPayment]:
    raw_entries = _extract_entries(payload, key_candidates=("payments", "entries", "tracker"))
    normalized: list[EstimatedTaxPayment] = []
    now = datetime.now(UTC)

    for index, entry in enumerate(raw_entries, start=1):
        if not isinstance(entry, Mapping):
            warnings.append(f"{source_name} entry {index} is not an object; skipped.")
            continue

        candidate = dict(entry)
        candidate.setdefault("last_updated", now)
        try:
            normalized.append(EstimatedTaxPayment.model_validate(candidate))
        except ValidationError as exc:
            warnings.append(
                f"{source_name} entry {index} failed validation: {_first_validation_error(exc)}."
            )

    return sorted(
        normalized,
        key=lambda row: (row.jurisdiction, row.due_date, row.amount, row.last_updated),
    )


def _parse_deadline_entries(
    *,
    payload: object | None,
    warnings: list[str],
    source_name: str,
) -> list[TaxDeadline]:
    raw_entries = _extract_entries(payload, key_candidates=("deadlines", "entries", "items"))
    normalized: list[TaxDeadline] = []

    for index, entry in enumerate(raw_entries, start=1):
        if not isinstance(entry, Mapping):
            warnings.append(f"{source_name} entry {index} is not an object; skipped.")
            continue
        try:
            normalized.append(TaxDeadline.model_validate(dict(entry)))
        except ValidationError as exc:
            warnings.append(
                f"{source_name} entry {index} failed validation: {_first_validation_error(exc)}."
            )

    return sorted(normalized, key=lambda row: (row.jurisdiction, row.due_date, row.name))


def _extract_entries(payload: object | None, *, key_candidates: tuple[str, ...]) -> list[object]:
    if payload is None:
        return []
    if isinstance(payload, list):
        return list(payload)
    if isinstance(payload, Mapping):
        for key in key_candidates:
            value = payload.get(key)
            if isinstance(value, list):
                return list(value)
    return []


def _first_validation_error(exc: ValidationError) -> str:
    first = exc.errors()[0]
    location = ".".join(str(segment) for segment in first.get("loc", []))
    message = str(first.get("msg", "validation error"))
    if not location:
        return message
    return f"{location}: {message}"


def _write_tracker_csv(*, path: Path, rows: Sequence[EstimatedTaxPayment]) -> None:
    writer = CsvWriter()
    writer.write_rows(
        path,
        fieldnames=[
            "jurisdiction",
            "due_date",
            "amount",
            "status",
            "paid_date",
            "last_updated",
        ],
        rows=[
            {
                "jurisdiction": row.jurisdiction,
                "due_date": row.due_date.isoformat(),
                "amount": f"{row.amount:.2f}",
                "status": row.status,
                "paid_date": row.paid_date.isoformat() if row.paid_date else "",
                "last_updated": row.last_updated.astimezone(UTC).isoformat(),
            }
            for row in rows
        ],
    )


def _write_deadlines_csv(*, path: Path, rows: Sequence[TaxDeadline]) -> None:
    writer = CsvWriter()
    writer.write_rows(
        path,
        fieldnames=["jurisdiction", "name", "due_date", "category", "completed"],
        rows=[
            {
                "jurisdiction": row.jurisdiction,
                "name": row.name,
                "due_date": row.due_date.isoformat(),
                "category": row.category,
                "completed": str(row.completed).lower(),
            }
            for row in rows
        ],
    )


def _extract_company_name(store: object) -> str:
    get_info = getattr(store, "get_company_info", None)
    if get_info is None:
        return "Unknown Company"
    try:
        payload = get_info()
    except Exception:
        return "Unknown Company"
    if not isinstance(payload, Mapping):
        return "Unknown Company"
    company_info = payload.get("CompanyInfo")
    if isinstance(company_info, Mapping):
        for key in ("LegalName", "CompanyName"):
            value = company_info.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return "Unknown Company"


def _write_tracker_pdf(*, path: Path, year: int, rows: Sequence[EstimatedTaxPayment], company_name: str = "Unknown Company") -> None:
    writer = PdfWriter()
    writer.write_table_report(
        path,
        company_name=company_name,
        report_title="Estimated Tax Tracker",
        date_range_label=f"{year}-01-01 to {year}-12-31",
        columns=["Jurisdiction", "Due Date", "Amount", "Status", "Paid Date", "Last Updated"],
        rows=[
            PdfTableRow(
                cells=(
                    row.jurisdiction,
                    row.due_date.isoformat(),
                    f"{row.amount:.2f}",
                    row.status,
                    row.paid_date.isoformat() if row.paid_date else "",
                    row.last_updated.astimezone(UTC).isoformat(),
                )
            )
            for row in rows
        ],
    )


_CATEGORY_LABELS: dict[str, str] = {
    "estimated_tax": "Estimated Tax",
    "filing": "Filing",
    "extension": "Extension",
}


def _write_deadlines_pdf(*, path: Path, year: int, rows: Sequence[TaxDeadline], company_name: str = "Unknown Company") -> None:
    writer = PdfWriter()
    writer.write_table_report(
        path,
        company_name=company_name,
        report_title="Tax Deadlines",
        date_range_label=f"{year}-01-01 to {year}-12-31",
        columns=["Due Date", "Jurisdiction", "Description", "Category", "Status"],
        rows=[
            PdfTableRow(
                cells=(
                    row.due_date.strftime("%b %d, %Y"),
                    row.jurisdiction,
                    row.name,
                    _CATEGORY_LABELS.get(row.category, row.category),
                    "Done" if row.completed else "Pending",
                ),
                status="reconciled" if row.completed else None,
            )
            for row in rows
        ],
    )


def _copy_source_json_if_present(
    *,
    source_path: Path,
    deliverable_dir: Path,
    meta_dir: Path,
    ctx: RunContext,
) -> list[Path]:
    if not source_path.exists():
        return []

    output_path = _resolve_output_path(
        deliverable_dir / source_path.name,
        on_conflict=ctx.on_conflict,
        non_interactive=ctx.non_interactive,
    )
    meta_snapshot_path = _resolve_output_path(
        meta_dir / source_path.name,
        on_conflict=ctx.on_conflict,
        non_interactive=ctx.non_interactive,
    )
    source_payload = source_path.read_bytes()
    for destination in (output_path, meta_snapshot_path):
        with atomic_write(destination, mode="wb") as handle:
            binary_handle = cast(IO[bytes], handle)
            binary_handle.write(source_payload)
    return [output_path, meta_snapshot_path]


def _resolve_output_path(path: Path, *, on_conflict: str, non_interactive: bool) -> Path:
    normalized_conflict = None if on_conflict == "prompt" else on_conflict
    return resolve_output_path(
        path,
        on_conflict=normalized_conflict,
        non_interactive=non_interactive,
    )
