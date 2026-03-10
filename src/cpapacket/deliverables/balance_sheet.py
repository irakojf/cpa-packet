"""Balance sheet normalization helpers for transforming QBO report rows."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import IO, Any, Literal, Protocol, cast

from cpapacket.core.context import RunContext
from cpapacket.core.filesystem import atomic_write
from cpapacket.deliverables.base import DeliverableResult
from cpapacket.models.normalized import NormalizedRow
from cpapacket.utils.constants import (
    BALANCE_EQUATION_TOLERANCE,
    DELIVERABLE_FOLDERS,
    SCHEMA_VERSIONS,
)
from cpapacket.utils.prompts import resolve_output_path
from cpapacket.writers.csv_writer import CsvWriter
from cpapacket.writers.pdf_writer import PdfBodyLine, PdfWriter

_SECTION_BY_KEY = {
    "assets": "Assets",
    "liabilities": "Liabilities",
    "equity": "Equity",
}
# QBO sometimes returns "LIABILITIES AND EQUITY" as a combined wrapper section.
# We treat it as a pass-through so nested headers ("Liabilities", "Equity") define
# their own canonical sections.
_PASSTHROUGH_SECTIONS = {"liabilities and equity"}
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


class BalanceSheetDataProvider(Protocol):
    """Minimal data-provider interface required by BalanceSheetDeliverable."""

    def get_balance_sheet(self, year: int, as_of: str) -> dict[str, Any]:
        """Fetch balance sheet report payload."""

    def get_company_info(self) -> dict[str, Any]:
        """Fetch company profile payload."""


def normalize_balance_sheet_rows(report_payload: Mapping[str, Any]) -> list[NormalizedRow]:
    """Normalize QBO Balance Sheet payload into flat ``NormalizedRow`` records."""
    rows_container = report_payload.get("Rows", {})
    rows = rows_container.get("Row", []) if isinstance(rows_container, Mapping) else []
    if not isinstance(rows, list):
        return []

    output: list[NormalizedRow] = []
    _walk_rows(rows=rows, section=None, path_parts=[], level=0, out=output)
    return output


@dataclass(frozen=True)
class BalanceEquationCheck:
    assets: Decimal
    liabilities: Decimal
    equity: Decimal
    difference: Decimal
    balanced: bool
    warning: str | None


class BalanceSheetDeliverable:
    """Balance sheet deliverable orchestrating normalization and artifact writes."""

    key = "balance_sheet"
    folder = DELIVERABLE_FOLDERS["balance_sheet"]
    required = True
    dependencies: list[str] = []
    requires_gusto = False
    year_offset = 0
    empty_placeholder_label = "No transactions found"

    def gather_prompts(self, _ctx: object) -> dict[str, Any]:
        return {}

    def is_current(self, _ctx: object) -> bool:
        # Incremental freshness checks are implemented in a follow-up bead.
        return False

    def generate(
        self,
        ctx: RunContext,
        store: BalanceSheetDataProvider,
        prompts: dict[str, Any],
    ) -> DeliverableResult:
        del prompts

        warnings: list[str] = []
        report_year = ctx.year - self.year_offset
        as_of = f"{report_year}-12-31"
        report_payload = store.get_balance_sheet(report_year, as_of)
        company_payload = store.get_company_info()

        rows = normalize_balance_sheet_rows(report_payload)
        if not rows:
            warnings.append("Balance sheet report normalized to zero rows.")
            rows = [
                NormalizedRow(
                    section="Assets",
                    label=self.empty_placeholder_label,
                    amount=Decimal("0"),
                    row_type="total",
                    level=0,
                    path=self.empty_placeholder_label,
                )
            ]

        equation = validate_balance_equation(rows)
        if equation.warning is not None:
            warnings.append(equation.warning)

        deliverable_dir = ctx.out_dir / self.folder
        deliverable_dir.mkdir(parents=True, exist_ok=True)
        meta_dir = ctx.out_dir / "_meta"
        meta_dir.mkdir(parents=True, exist_ok=True)
        cpa_dir = deliverable_dir / "cpa"
        cpa_dir.mkdir(parents=True, exist_ok=True)
        dev_dir = deliverable_dir / "dev"
        dev_dir.mkdir(parents=True, exist_ok=True)

        base_name = f"Balance_Sheet_{as_of}"
        csv_path = _resolve_output_path(
            cpa_dir / f"{base_name}.csv",
            on_conflict=ctx.on_conflict,
            non_interactive=ctx.non_interactive,
        )
        pdf_path = _resolve_output_path(
            cpa_dir / f"{base_name}.pdf",
            on_conflict=ctx.on_conflict,
            non_interactive=ctx.non_interactive,
        )
        json_path = (
            None
            if ctx.no_raw
            else _resolve_output_path(
                dev_dir / f"{base_name}_raw.json",
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
            date_range_label=f"As of {as_of}",
            equation=equation,
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
                "report_year": report_year,
                "as_of_date": as_of,
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


class PriorBalanceSheetDeliverable(BalanceSheetDeliverable):
    """Prior-year (optional) balance sheet output alongside current-year output."""

    key = "prior_balance_sheet"
    required = False
    year_offset = 1
    empty_placeholder_label = "No prior-year data available"

def validate_balance_equation(rows: Sequence[NormalizedRow]) -> BalanceEquationCheck:
    """Validate Assets = Liabilities + Equity within configured tolerance."""
    assets = _extract_section_total(rows=rows, section="Assets")
    liabilities = _extract_section_total(rows=rows, section="Liabilities")
    equity = _extract_section_total(rows=rows, section="Equity")

    difference = assets - (liabilities + equity)
    balanced = abs(difference) <= BALANCE_EQUATION_TOLERANCE
    warning = (
        None
        if balanced
        else (
            "Balance equation mismatch: "
            f"Assets={assets}, Liabilities+Equity={liabilities + equity}, "
            f"difference={difference}, tolerance={BALANCE_EQUATION_TOLERANCE}."
        )
    )

    return BalanceEquationCheck(
        assets=assets,
        liabilities=liabilities,
        equity=equity,
        difference=difference,
        balanced=balanced,
        warning=warning,
    )


def _walk_rows(
    *,
    rows: list[Any],
    section: str | None,
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

            # Top-level headers define canonical sections. Nested headers keep the
            # parent section (for groups like "Current Assets").
            # Pass-through sections (e.g. "LIABILITIES AND EQUITY") are wrappers
            # whose children define their own canonical sections.
            is_passthrough = label.strip().lower() in _PASSTHROUGH_SECTIONS
            if is_passthrough:
                next_section = None
            elif section is None:
                next_section = _resolve_section(label)
            else:
                next_section = section

            header_path_parts = [*path_parts, label] if not is_passthrough else list(path_parts)
            if not is_passthrough:
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
                    level=level if is_passthrough else level + 1,
                    out=out,
                )

            if isinstance(summary, Mapping) and not is_passthrough:
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
            if section is None:
                raise ValueError(f"Account row '{label}' appears before a section header")
            out.append(
                NormalizedRow(
                    section=section,
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
            if section is None:
                raise ValueError(f"Summary row '{summary_label}' appears before a section header")
            out.append(
                NormalizedRow(
                    section=section,
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


def _resolve_section(candidate: str) -> str:
    normalized = candidate.strip().lower()
    resolved = _SECTION_BY_KEY.get(normalized)
    if resolved is None:
        raise ValueError(
            f"Unsupported balance sheet section '{candidate}'. "
            "Expected one of: Assets, Liabilities, Equity."
        )
    return resolved


def _classify_summary(label: str) -> _RowType:
    lower = label.lower()
    if lower.startswith("total "):
        return "total"
    return "subtotal"


def _extract_section_total(*, rows: Sequence[NormalizedRow], section: str) -> Decimal:
    section_totals: list[Decimal] = []
    for row in rows:
        if row.section != section or row.row_type != "total":
            continue
        if not row.label.lower().startswith("total "):
            continue
        section_totals.append(_coerce_decimal(row.amount))

    if section_totals:
        return section_totals[-1]

    total = Decimal("0")
    for row in rows:
        if row.section == section and row.row_type == "account":
            total += _coerce_decimal(row.amount)
    return total


def _coerce_decimal(value: object) -> Decimal:
    try:
        return value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")


def _resolve_output_path(path: Path, *, on_conflict: str, non_interactive: bool) -> Path:
    normalized_conflict = None if on_conflict == "prompt" else on_conflict
    return cast(
        Path,
        resolve_output_path(
            path,
            on_conflict=normalized_conflict,
            non_interactive=non_interactive,
        ),
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
    equation: BalanceEquationCheck,
) -> None:
    writer = PdfWriter()
    body_lines = [
        PdfBodyLine(
            text=row.label,
            level=row.level,
            row_type=row.row_type,
            amount=f"{row.amount:,.2f}",
        )
        for row in rows
    ]
    body_lines.extend(
        [
            PdfBodyLine(text="Balance Equation Summary", level=0, row_type="header"),
            PdfBodyLine(text="Assets", level=1, row_type="account", amount=f"{equation.assets:,.2f}"),
            PdfBodyLine(
                text="Liabilities + Equity",
                level=1,
                row_type="account",
                amount=f"{(equation.liabilities + equation.equity):,.2f}",
            ),
            PdfBodyLine(text="Difference", level=1, row_type="total", amount=f"{equation.difference:,.2f}"),
        ]
    )
    writer.write_report(
        path,
        company_name=company_name,
        report_title="Balance Sheet",
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
