"""P&L normalization helpers for transforming QBO report rows."""

from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from tempfile import NamedTemporaryFile
from time import gmtime, strftime
from typing import Any, Mapping

from cpapacket.models.normalized import NormalizedRow
from cpapacket.utils.constants import DELIVERABLE_FOLDERS, SCHEMA_VERSIONS

_DEFAULT_SECTION = "Uncategorized"
_SECTION_MAP = {
    "income": "Income",
    "cost of goods sold": "COGS",
    "cost of sales": "COGS",
    "expenses": "Expenses",
    "other income": "Other Income",
    "other expenses": "Other Expense",
    "other expense": "Other Expense",
}


def normalize_pnl_rows(report_payload: Mapping[str, Any]) -> list[NormalizedRow]:
    """Normalize QBO P&L report payload into flat NormalizedRow records."""
    rows_container = report_payload.get("Rows", {})
    rows = rows_container.get("Row", []) if isinstance(rows_container, Mapping) else []
    if not isinstance(rows, list):
        return []

    output: list[NormalizedRow] = []
    _walk_rows(
        rows=rows,
        section=_DEFAULT_SECTION,
        path_parts=[],
        level=0,
        out=output,
    )
    return output


def _walk_rows(
    *,
    rows: list[Any],
    section: str,
    path_parts: list[str],
    level: int,
    out: list[NormalizedRow],
) -> None:
    for raw in rows:
        if not isinstance(raw, Mapping):
            continue

        header = raw.get("Header")
        nested_rows = raw.get("Rows")
        summary = raw.get("Summary")
        col_data = raw.get("ColData")

        if isinstance(header, Mapping) and isinstance(nested_rows, Mapping):
            label, amount = _parse_col_data(header.get("ColData"))
            if not label:
                label = "Section"

            next_section = _resolve_section(label, fallback=section)
            header_path_parts = [*path_parts, label]
            out.append(
                NormalizedRow(
                    section=next_section,
                    label=label,
                    amount=amount,
                    row_type="header",
                    level=level,
                    path=" > ".join(header_path_parts),
                )
            )

            inner = nested_rows.get("Row")
            if isinstance(inner, list):
                _walk_rows(
                    rows=inner,
                    section=next_section,
                    path_parts=header_path_parts,
                    level=level + 1,
                    out=out,
                )

            if isinstance(summary, Mapping):
                summary_label, summary_amount = _parse_col_data(summary.get("ColData"))
                if summary_label:
                    out.append(
                        NormalizedRow(
                            section=next_section,
                            label=summary_label,
                            amount=summary_amount,
                            row_type=_classify_summary(summary_label),
                            level=level,
                            path=" > ".join([*path_parts, summary_label]),
                        )
                    )
            continue

        if isinstance(col_data, list):
            label, amount = _parse_col_data(col_data)
            if not label:
                continue
            resolved_section = _resolve_section(section, fallback=_DEFAULT_SECTION)
            out.append(
                NormalizedRow(
                    section=resolved_section,
                    label=label,
                    amount=amount,
                    row_type="account",
                    level=level,
                    path=" > ".join([*path_parts, label]),
                )
            )
            continue

        if isinstance(summary, Mapping):
            summary_label, summary_amount = _parse_col_data(summary.get("ColData"))
            if not summary_label:
                continue
            resolved_section = _resolve_section(section, fallback=_DEFAULT_SECTION)
            out.append(
                NormalizedRow(
                    section=resolved_section,
                    label=summary_label,
                    amount=summary_amount,
                    row_type=_classify_summary(summary_label),
                    level=max(level - 1, 0),
                    path=" > ".join([*path_parts, summary_label]),
                )
            )


def _parse_col_data(col_data: Any) -> tuple[str, Decimal]:
    if not isinstance(col_data, list):
        return "", Decimal("0")

    values: list[str] = []
    for entry in col_data:
        if not isinstance(entry, Mapping):
            continue
        raw = entry.get("value")
        if isinstance(raw, str):
            trimmed = raw.strip()
            if trimmed:
                values.append(trimmed)

    if not values:
        return "", Decimal("0")

    label = values[0]
    amount = _parse_amount(values[-1]) if len(values) > 1 else Decimal("0")
    return label, amount


def _parse_amount(value: str) -> Decimal:
    cleaned = value.replace(",", "").replace("$", "").strip()
    if cleaned.startswith("(") and cleaned.endswith(")"):
        cleaned = f"-{cleaned[1:-1]}"
    try:
        return Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _resolve_section(candidate: str, *, fallback: str) -> str:
    normalized = candidate.strip().lower()
    return _SECTION_MAP.get(normalized, fallback)


def _classify_summary(label: str) -> str:
    lower = label.lower()
    if lower.startswith("net ") or lower.startswith("total "):
        return "total"
    return "subtotal"


@dataclass(frozen=True, slots=True)
class PnlDeliverableResult:
    """Result payload returned by PnlDeliverable.generate."""

    deliverable_key: str
    success: bool
    artifacts: list[str]
    warnings: list[str]
    error: str | None = None


class PnlDeliverable:
    """Profit and Loss deliverable orchestrating normalization and artifact writes."""

    key = "pnl"
    folder = DELIVERABLE_FOLDERS["pnl"]
    required = True
    dependencies: list[str] = []
    requires_gusto = False

    def gather_prompts(self, _ctx: object) -> dict[str, Any]:
        return {}

    def generate(
        self,
        *,
        report_payload: Mapping[str, Any],
        output_root: Path,
        year: int,
        on_conflict: str = "abort",
        no_raw: bool = False,
    ) -> PnlDeliverableResult:
        warnings: list[str] = []

        rows = normalize_pnl_rows(report_payload)
        if not rows:
            warnings.append("P&L report normalized to zero rows.")

        deliverable_dir = output_root / self.folder
        deliverable_dir.mkdir(parents=True, exist_ok=True)
        meta_dir = output_root / "_meta"
        meta_dir.mkdir(parents=True, exist_ok=True)

        base_name = f"Profit_and_Loss_{year}"
        csv_path = _resolve_output_path(deliverable_dir / f"{base_name}.csv", on_conflict)
        pdf_path = _resolve_output_path(deliverable_dir / f"{base_name}.pdf", on_conflict)
        json_path = None if no_raw else _resolve_output_path(deliverable_dir / f"{base_name}_raw.json", on_conflict)
        metadata_path = _resolve_output_path(meta_dir / f"{self.key}_metadata.json", on_conflict)

        _write_csv(csv_path, rows)
        _write_pdf(pdf_path, rows, year)
        if json_path is not None:
            _write_json(json_path, report_payload)
        _write_metadata(
            path=metadata_path,
            key=self.key,
            report_payload=report_payload,
            artifacts=[csv_path, pdf_path] + ([json_path] if json_path is not None else []),
        )

        return PnlDeliverableResult(
            deliverable_key=self.key,
            success=True,
            artifacts=[str(csv_path), str(pdf_path)] + ([str(json_path)] if json_path is not None else []),
            warnings=warnings,
        )


def _resolve_output_path(path: Path, on_conflict: str) -> Path:
    if not path.exists():
        return path

    normalized = on_conflict.strip().lower()
    if normalized == "overwrite":
        return path
    if normalized == "copy":
        suffix = strftime("%Y%m%d_%H%M%S", gmtime())
        return path.with_name(f"{path.stem}__copy_{suffix}{path.suffix}")
    raise FileExistsError(f"{path} already exists and on_conflict=abort")


def _write_csv(path: Path, rows: list[NormalizedRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", delete=False, encoding="utf-8", newline="", dir=path.parent) as tmp_file:
        writer = csv.writer(tmp_file)
        writer.writerow(["section", "path", "label", "row_type", "level", "amount"])
        for row in rows:
            writer.writerow(
                [
                    row.section,
                    row.path,
                    row.label,
                    row.row_type,
                    row.level,
                    f"{row.amount:.2f}",
                ]
            )
        tmp_name = tmp_file.name
    Path(tmp_name).replace(path)


def _write_pdf(path: Path, rows: list[NormalizedRow], year: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.pdfgen import canvas
    except ImportError as exc:  # pragma: no cover - runtime dependency path
        raise RuntimeError("reportlab is required to generate PDF output") from exc

    with NamedTemporaryFile("wb", delete=False, dir=path.parent) as tmp_file:
        tmp_name = tmp_file.name

    pdf = canvas.Canvas(tmp_name, pagesize=letter)
    width, height = letter
    y = height - 48
    pdf.setFont("Helvetica-Bold", 12)
    pdf.drawString(40, y, f"Profit and Loss - {year}")
    y -= 20
    pdf.setFont("Helvetica", 9)
    for row in rows:
        if y < 48:
            pdf.showPage()
            y = height - 48
            pdf.setFont("Helvetica", 9)
        indent = " " * (row.level * 2)
        line = f"{indent}{row.label} ({row.row_type}) .... {row.amount:.2f}"
        pdf.drawString(40, y, line[:120])
        y -= 12
    pdf.save()
    Path(tmp_name).replace(path)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", delete=False, encoding="utf-8", dir=path.parent) as tmp_file:
        json.dump(payload, tmp_file, indent=2, sort_keys=True)
        tmp_name = tmp_file.name
    Path(tmp_name).replace(path)


def _write_metadata(
    *,
    path: Path,
    key: str,
    report_payload: Mapping[str, Any],
    artifacts: list[Path],
) -> None:
    canonical = json.dumps(report_payload, sort_keys=True, separators=(",", ":"))
    fingerprint = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    payload = {
        "deliverable": key,
        "input_fingerprint": fingerprint,
        "schema_versions": SCHEMA_VERSIONS.get(key, {}),
        "artifacts": [str(item) for item in artifacts],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", delete=False, encoding="utf-8", dir=path.parent) as tmp_file:
        json.dump(payload, tmp_file, indent=2, sort_keys=True)
        tmp_name = tmp_file.name
    Path(tmp_name).replace(path)
