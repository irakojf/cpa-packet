"""QBO Profit and Loss JSON normalization helpers."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Literal

from cpapacket.models.normalized import NormalizedRow

_SECTION_ALIASES = {
    "income": "Income",
    "cost of goods sold": "Cost of Goods Sold",
    "cogs": "Cost of Goods Sold",
    "expenses": "Expenses",
    "other income": "Other Income",
    "other expense": "Other Expense",
}
RowType = Literal["header", "account", "subtotal", "total"]


def normalize_pnl_report(report_payload: dict[str, Any]) -> list[NormalizedRow]:
    """Flatten QBO P&L nested rows to canonical NormalizedRow entries."""
    rows_node = report_payload.get("Rows", {})
    rows = rows_node.get("Row", []) if isinstance(rows_node, dict) else []
    if not isinstance(rows, list):
        return []

    normalized: list[NormalizedRow] = []
    _walk_rows(rows=rows, output=normalized, current_section="", parent_path="", level=0)
    return normalized


def _walk_rows(
    *,
    rows: list[dict[str, Any]],
    output: list[NormalizedRow],
    current_section: str,
    parent_path: str,
    level: int,
) -> None:
    for row in rows:
        if not isinstance(row, dict):
            continue

        has_children = _has_child_rows(row)
        if has_children:
            header_col_data = _extract_col_data(row.get("Header"))
            header_label = _extract_label(header_col_data) or _extract_label(_extract_col_data(row))
            if not header_label:
                header_label = "Untitled"
            section = _resolve_section(current_section, header_label, level)
            header_path = _build_path(parent_path, header_label)

            output.append(
                NormalizedRow(
                    section=section,
                    label=header_label,
                    amount=_extract_amount(header_col_data),
                    row_type="header",
                    level=level,
                    path=header_path,
                )
            )

            child_rows_node = row.get("Rows", {})
            child_rows = child_rows_node.get("Row", []) if isinstance(child_rows_node, dict) else []
            if isinstance(child_rows, list):
                _walk_rows(
                    rows=child_rows,
                    output=output,
                    current_section=section,
                    parent_path=header_path,
                    level=level + 1,
                )

            summary_col_data = _extract_col_data(row.get("Summary"))
            summary_label = _extract_label(summary_col_data)
            if summary_label:
                output.append(
                    NormalizedRow(
                        section=section,
                        label=summary_label,
                        amount=_extract_amount(summary_col_data),
                        row_type=_row_type_for_label(summary_label),
                        level=level + 1,
                        path=_build_path(header_path, summary_label),
                    )
                )
            continue

        col_data = _extract_col_data(row)
        label = _extract_label(col_data)
        if not label:
            continue

        section = _resolve_section(current_section, label, level)
        output.append(
            NormalizedRow(
                section=section,
                label=label,
                amount=_extract_amount(col_data),
                row_type=_row_type_for_label(label, default="account"),
                level=level,
                path=_build_path(parent_path, label),
            )
        )


def _has_child_rows(row: dict[str, Any]) -> bool:
    rows_node = row.get("Rows")
    if not isinstance(rows_node, dict):
        return False
    child_rows = rows_node.get("Row")
    return isinstance(child_rows, list) and len(child_rows) > 0


def _extract_col_data(node: Any) -> list[dict[str, Any]]:
    if not isinstance(node, dict):
        return []
    col_data = node.get("ColData")
    if not isinstance(col_data, list):
        return []
    return [item for item in col_data if isinstance(item, dict)]


def _extract_label(col_data: list[dict[str, Any]]) -> str:
    if not col_data:
        return ""
    raw = col_data[0].get("value", "")
    return str(raw).strip()


def _extract_amount(col_data: list[dict[str, Any]]) -> Decimal:
    if len(col_data) < 2:
        return Decimal("0.00")
    raw = str(col_data[1].get("value", "")).strip()
    if raw == "":
        return Decimal("0.00")

    negative = raw.startswith("(") and raw.endswith(")")
    cleaned = raw.strip("()").replace(",", "").replace("$", "")
    try:
        amount = Decimal(cleaned)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"invalid monetary amount in ColData: {raw!r}") from exc
    return -amount if negative else amount


def _resolve_section(current_section: str, label: str, level: int) -> str:
    if current_section:
        return current_section
    if level == 0:
        mapped = _SECTION_ALIASES.get(label.strip().lower())
        if mapped:
            return mapped
    return "Income"


def _row_type_for_label(label: str, default: RowType = "subtotal") -> RowType:
    normalized = label.strip().lower()
    if normalized.startswith("total "):
        return "total"
    return default


def _build_path(parent_path: str, label: str) -> str:
    cleaned_label = label.strip()
    if not parent_path:
        return cleaned_label
    return f"{parent_path} > {cleaned_label}"
