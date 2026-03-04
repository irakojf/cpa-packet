"""Balance sheet normalization helpers for transforming QBO report rows."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

from cpapacket.models.normalized import NormalizedRow
from cpapacket.utils.constants import BALANCE_EQUATION_TOLERANCE

_SECTION_BY_KEY = {
    "assets": "Assets",
    "liabilities": "Liabilities",
    "equity": "Equity",
}
_RowType = Literal["header", "account", "subtotal", "total"]


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
            next_section = _resolve_section(label) if section is None else section
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
