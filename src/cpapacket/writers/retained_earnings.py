"""Retained earnings rollforward artifact writers."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

from cpapacket.models.retained_earnings import RetainedEarningsRollforward
from cpapacket.writers.csv_writer import CsvWriter
from cpapacket.writers.json_writer import JsonWriter
from cpapacket.writers.pdf_writer import PdfTableRow, PdfTableSection, PdfWriter

_CSV_FIELDS: tuple[str, ...] = (
    "year",
    "beginning_re",
    "net_income",
    "distributions",
    "expected_ending_re",
    "actual_ending_re",
    "difference",
    "status",
    "flags",
    "miscoded_distribution_count",
)


def to_rollforward_csv_row(
    *,
    year: int,
    rollforward: RetainedEarningsRollforward,
    miscoded_distribution_count: int,
) -> dict[str, str]:
    """Build a single retained-earnings rollforward CSV row."""
    return {
        "year": str(year),
        "beginning_re": f"{rollforward.beginning_re:.2f}",
        "net_income": f"{rollforward.net_income:.2f}",
        "distributions": f"{rollforward.distributions:.2f}",
        "expected_ending_re": f"{rollforward.expected_ending_re:.2f}",
        "actual_ending_re": f"{rollforward.actual_ending_re:.2f}",
        "difference": f"{rollforward.difference:.2f}",
        "status": rollforward.status,
        "flags": "|".join(rollforward.flags),
        "miscoded_distribution_count": str(max(miscoded_distribution_count, 0)),
    }


def write_rollforward_csv(
    *,
    path: Path,
    year: int,
    rollforward: RetainedEarningsRollforward,
    miscoded_distribution_count: int,
) -> None:
    """Write retained-earnings rollforward CSV artifact."""
    writer = CsvWriter()
    writer.write_rows(
        path,
        fieldnames=list(_CSV_FIELDS),
        rows=[
            to_rollforward_csv_row(
                year=year,
                rollforward=rollforward,
                miscoded_distribution_count=miscoded_distribution_count,
            )
        ],
    )


def write_rollforward_data_json(
    *,
    path: Path,
    year: int,
    rollforward: RetainedEarningsRollforward,
    miscoded_distribution_count: int,
    data_sources: dict[str, str] | None = None,
) -> None:
    """Write retained-earnings ``_data.json`` payload artifact."""
    payload: dict[str, Any] = {
        "year": year,
        "rollforward": rollforward.model_dump(mode="json"),
        "miscoded_distribution_count": max(miscoded_distribution_count, 0),
        "data_sources": dict(data_sources or {}),
    }

    writer = JsonWriter()
    writer.write_payload(path, payload=payload, no_raw=False, redact=False)


def write_rollforward_pdf(
    *,
    path: Path,
    year: int,
    rollforward: RetainedEarningsRollforward,
    miscoded_distribution_count: int,
    company_name: str = "Unknown Company",
) -> None:
    """Write retained-earnings reconciliation PDF artifact."""
    writer = PdfWriter()
    line_items: Sequence[PdfTableRow] = (
        PdfTableRow(cells=("Beginning RE", f"{rollforward.beginning_re:.2f}")),
        PdfTableRow(cells=("Net Income", f"{rollforward.net_income:.2f}")),
        PdfTableRow(cells=("Distributions", f"{rollforward.distributions:.2f}")),
        PdfTableRow(cells=("Expected Ending RE", f"{rollforward.expected_ending_re:.2f}")),
        PdfTableRow(cells=("Actual Ending RE", f"{rollforward.actual_ending_re:.2f}")),
        PdfTableRow(cells=("Difference", f"{rollforward.difference:.2f}")),
        PdfTableRow(cells=("Status", rollforward.status)),
        PdfTableRow(cells=("Flags", _format_flags(rollforward.flags))),
        PdfTableRow(
            cells=("Likely Miscoded Distributions", str(max(miscoded_distribution_count, 0))),
            row_type="total",
        ),
    )
    sections = (
        PdfTableSection(
            title="Retained Earnings Rollforward",
            headers=("Line Item", "Value"),
            rows=line_items,
        ),
    )
    writer.write_reconciliation_report(
        path,
        company_name=company_name,
        report_title="Retained Earnings Rollforward",
        date_range_label=f"{year}-01-01 to {year}-12-31",
        sections=sections,
    )


def _format_flags(flags: Sequence[str]) -> str:
    if not flags:
        return "none"
    return ", ".join(flags)
