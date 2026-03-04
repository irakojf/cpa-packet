"""P&L normalization helpers for transforming QBO report rows."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import IO, Any, Literal, Protocol, cast

from cpapacket.core.context import RunContext
from cpapacket.core.filesystem import atomic_write
from cpapacket.deliverables.base import DeliverableResult
from cpapacket.models.normalized import NormalizedRow
from cpapacket.utils.constants import DELIVERABLE_FOLDERS, SCHEMA_VERSIONS
from cpapacket.utils.prompts import resolve_output_path
from cpapacket.writers.csv_writer import CsvWriter
from cpapacket.writers.pdf_writer import PdfBodyLine, PdfWriter

_DEFAULT_SECTION = "Uncategorized"
_RowType = Literal["header", "account", "subtotal", "total"]
_REDACTED_VALUE = "[REDACTED]"
_SENSITIVE_JSON_KEYS = {
    "access_token",
    "refresh_token",
    "token",
    "secret",
    "api_key",
    "ssn",
    "ein",
}
_SECTION_MAP = {
    "income": "Income",
    "cost of goods sold": "COGS",
    "cost of sales": "COGS",
    "expenses": "Expenses",
    "other income": "Other Income",
    "other expenses": "Other Expense",
    "other expense": "Other Expense",
}


class PnlDataProvider(Protocol):
    """Minimal data-provider interface required by PnlDeliverable."""

    def get_pnl(self, year: int, method: str) -> dict[str, Any]:
        """Fetch Profit & Loss report payload."""

    def get_company_info(self) -> dict[str, Any]:
        """Fetch company profile payload."""


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


def _classify_summary(label: str) -> _RowType:
    lower = label.lower()
    if lower.startswith("net ") or lower.startswith("total "):
        return "total"
    return "subtotal"


class PnlDeliverable:
    """Profit and Loss deliverable orchestrating normalization and artifact writes."""

    key = "pnl"
    folder = DELIVERABLE_FOLDERS["pnl"]
    required = True
    dependencies: list[str] = []
    requires_gusto = False

    def gather_prompts(self, _ctx: object) -> dict[str, Any]:
        return {}

    def is_current(self, _ctx: object) -> bool:
        # Incremental freshness checks are implemented in a follow-up bead.
        return False

    def generate(
        self,
        ctx: RunContext,
        store: PnlDataProvider,
        prompts: dict[str, Any],
    ) -> DeliverableResult:
        del prompts

        warnings: list[str] = []
        report_payload = store.get_pnl(ctx.year, ctx.method)
        company_payload = store.get_company_info()

        rows = normalize_pnl_rows(report_payload)
        if not rows:
            warnings.append("P&L report normalized to zero rows.")
            rows = [
                NormalizedRow(
                    section=_DEFAULT_SECTION,
                    label="No transactions found",
                    amount=Decimal("0"),
                    row_type="total",
                    level=0,
                    path="No transactions found",
                )
            ]

        deliverable_dir = ctx.out_dir / self.folder
        deliverable_dir.mkdir(parents=True, exist_ok=True)
        meta_dir = ctx.out_dir / "_meta"
        meta_dir.mkdir(parents=True, exist_ok=True)

        start_date, end_date = _extract_report_date_range(report_payload, year=ctx.year)
        normalized_method = ctx.method.strip().lower() if ctx.method.strip() else "accrual"
        csv_name = f"Profit_and_Loss_{start_date}_to_{end_date}_{normalized_method}"
        base_name = f"Profit_and_Loss_{start_date}_to_{end_date}_{normalized_method}"
        csv_path = _resolve_output_path(
            deliverable_dir / f"{csv_name}.csv",
            on_conflict=ctx.on_conflict,
            non_interactive=ctx.non_interactive,
        )
        pdf_path = _resolve_output_path(
            deliverable_dir / f"{base_name}.pdf",
            on_conflict=ctx.on_conflict,
            non_interactive=ctx.non_interactive,
        )
        json_path = (
            None
            if ctx.no_raw
            else _resolve_output_path(
                deliverable_dir / f"{base_name}_raw.json",
                on_conflict=ctx.on_conflict,
                non_interactive=ctx.non_interactive,
            )
        )
        metadata_path = _resolve_output_path(
            meta_dir / f"{self.key}_metadata.json",
            on_conflict=ctx.on_conflict,
            non_interactive=ctx.non_interactive,
        )

        _write_csv(csv_path, rows)
        _write_pdf(
            pdf_path,
            rows,
            company_name=_extract_company_name(company_payload),
            date_range_label=f"{start_date} to {end_date} ({normalized_method} basis)",
        )
        if json_path is not None:
            payload_to_write = _redact_payload(report_payload) if ctx.redact else report_payload
            _write_json(json_path, payload_to_write)
        _write_metadata(
            path=metadata_path,
            key=self.key,
            report_payload=report_payload,
            artifacts=[csv_path, pdf_path] + ([json_path] if json_path is not None else []),
            warnings=warnings,
            context_inputs={
                "year": ctx.year,
                "method": normalized_method,
                "start_date": start_date,
                "end_date": end_date,
                "no_raw": ctx.no_raw,
                "redact": ctx.redact,
            },
        )

        return DeliverableResult(
            deliverable_key=self.key,
            success=True,
            artifacts=[str(csv_path), str(pdf_path)]
            + ([str(json_path)] if json_path is not None else []),
            warnings=warnings,
        )


def _resolve_output_path(path: Path, *, on_conflict: str, non_interactive: bool) -> Path:
    normalized_conflict = None if on_conflict == "prompt" else on_conflict
    return resolve_output_path(
        path,
        on_conflict=normalized_conflict,
        non_interactive=non_interactive,
    )


def _write_csv(path: Path, rows: list[NormalizedRow]) -> None:
    writer = CsvWriter()
    writer.write_rows(
        path,
        fieldnames=["section", "level", "row_type", "label", "amount", "path"],
        rows=[
            {
                "section": row.section,
                "level": row.level,
                "row_type": row.row_type,
                "label": row.label,
                "amount": f"{row.amount:.2f}",
                "path": row.path,
            }
            for row in rows
        ],
    )


def _write_pdf(
    path: Path,
    rows: list[NormalizedRow],
    *,
    company_name: str,
    date_range_label: str,
) -> None:
    writer = PdfWriter()
    body_lines = [
        PdfBodyLine(
            text=f"{row.label}  {row.amount:.2f}",
            level=row.level,
            row_type=row.row_type,
        )
        for row in rows
    ]
    writer.write_report(
        path,
        company_name=company_name,
        report_title="Profit and Loss",
        date_range_label=date_range_label,
        body_lines=body_lines,
    )


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    with atomic_write(path, mode="w", encoding="utf-8") as handle:
        text_handle = cast(IO[str], handle)
        json.dump(payload, text_handle, indent=2, sort_keys=True)
        text_handle.write("\n")


def _redact_payload(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    return cast(Mapping[str, Any], _redact_value(payload))


def _redact_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        redacted: dict[str, Any] = {}
        for key, nested in value.items():
            key_lower = str(key).strip().lower()
            if key_lower in _SENSITIVE_JSON_KEYS:
                redacted[str(key)] = _REDACTED_VALUE
                continue
            redacted[str(key)] = _redact_value(nested)
        return redacted
    if isinstance(value, list):
        return [_redact_value(item) for item in value]
    return value


def _write_metadata(
    *,
    path: Path,
    key: str,
    report_payload: Mapping[str, Any],
    artifacts: list[Path],
    warnings: list[str],
    context_inputs: Mapping[str, Any],
) -> None:
    metadata_inputs = {
        **dict(context_inputs),
        "report_payload": report_payload,
    }
    canonical = json.dumps(metadata_inputs, sort_keys=True, separators=(",", ":"))
    fingerprint = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    payload = {
        "deliverable": key,
        "input_fingerprint": fingerprint,
        "schema_versions": SCHEMA_VERSIONS.get(key, {}),
        "artifacts": [str(item) for item in artifacts],
        "warnings": warnings,
    }
    with atomic_write(path, mode="w", encoding="utf-8") as handle:
        text_handle = cast(IO[str], handle)
        json.dump(payload, text_handle, indent=2, sort_keys=True)
        text_handle.write("\n")


def _extract_report_date_range(report_payload: Mapping[str, Any], *, year: int) -> tuple[str, str]:
    header = report_payload.get("Header")
    if isinstance(header, Mapping):
        start = header.get("StartPeriod")
        end = header.get("EndPeriod")
        if isinstance(start, str) and start.strip() and isinstance(end, str) and end.strip():
            return start.strip(), end.strip()
    return f"{year}-01-01", f"{year}-12-31"


def _extract_company_name(company_payload: Mapping[str, Any]) -> str:
    company_info = company_payload.get("CompanyInfo")
    if isinstance(company_info, Mapping):
        legal_name = company_info.get("LegalName")
        if isinstance(legal_name, str) and legal_name.strip():
            return legal_name.strip()
        company_name = company_info.get("CompanyName")
        if isinstance(company_name, str) and company_name.strip():
            return company_name.strip()
    return "Unknown Company"
