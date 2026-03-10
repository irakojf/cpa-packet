"""Packet-level CPA review dashboard generation."""

from __future__ import annotations

import csv
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import IO, cast

from cpapacket.core.filesystem import atomic_write
from cpapacket.core.metadata import (
    DeliverableMetadata,
    compute_input_fingerprint,
    write_deliverable_metadata,
)
from cpapacket.utils.constants import SCHEMA_VERSIONS
from cpapacket.writers.pdf_writer import PdfBodyLine, PdfWriter


def write_review_dashboard(*, output_root: Path | str, year: int) -> tuple[Path, Path]:
    """Write packet-level CPA review dashboard markdown and PDF artifacts."""
    root = Path(output_root)
    root.mkdir(parents=True, exist_ok=True)

    pnl_csv = _find_year_specific(
        root / "01_Year-End_Profit_and_Loss" / "cpa",
        f"Profit_and_Loss_{year}-01-01_to_{year}-12-31_*.csv",
    )
    balance_sheet_csv = _find_year_specific(
        root / "02_Year-End_Balance_Sheet" / "cpa",
        f"Balance_Sheet_{year}-12-31.csv",
    )
    equity_csv = _find_year_specific(
        root / "09_Retained_Earnings_Rollforward" / "cpa",
        f"Book_Equity_Rollforward_{year}.csv",
    )
    contractor_csv = _find_year_specific(
        root / "07_Contractor_1099_Summary" / "cpa",
        f"Contractor_1099_Review_{year}.csv",
    )

    net_income = _lookup_csv_amount(pnl_csv, "label", "Net Income")
    ending_cash = _sum_cash_rows(balance_sheet_csv)
    ending_liabilities = _lookup_csv_amount(balance_sheet_csv, "label", "Total Liabilities")
    shareholder_receivable = _lookup_contains_amount(
        balance_sheet_csv,
        "label",
        "shareholder receivable",
    )
    distributions_total = _read_equity_value(equity_csv, "current_year_distributions_gl")
    contributions_total = _read_equity_value(equity_csv, "current_year_contributions_gl")
    review_flags = _read_pipe_list(equity_csv, "flags")
    contractor_review_count = _count_flagged_contractors(contractor_csv)

    qbo_sources_present = {
        "pnl": pnl_csv is not None,
        "balance_sheet": balance_sheet_csv is not None,
        "equity_review": equity_csv is not None,
        "contractor_review": contractor_csv is not None,
    }

    markdown_path = root / "00_REVIEW_DASHBOARD.md"
    pdf_path = root / "00_REVIEW_DASHBOARD.pdf"
    metadata_path = root / "_meta" / "private" / "deliverables" / "review_dashboard_metadata.json"

    markdown_payload = "\n".join(
        [
            "# Review Dashboard",
            "",
            f"- Tax Year: {year}",
            f"- QBO Source Reports Present: {_format_source_status(qbo_sources_present)}",
            f"- Net Income: {_fmt(net_income)}",
            f"- Ending Cash: {_fmt(ending_cash)}",
            f"- Ending Liabilities: {_fmt(ending_liabilities)}",
            f"- Distributions Total: {_fmt(distributions_total)}",
            f"- Contributions Total: {_fmt(contributions_total)}",
            f"- Shareholder Receivable Ending Balance: {_fmt(shareholder_receivable)}",
            f"- Contractor/1099 Review Count: {contractor_review_count}",
            f"- Open Review Flags: {', '.join(review_flags) if review_flags else 'none'}",
            "- Book review only; not shareholder basis or AAA",
            "",
        ]
    )
    with atomic_write(markdown_path, mode="w", encoding="utf-8", newline="\n") as handle:
        cast(IO[str], handle).write(markdown_payload)

    PdfWriter().write_report(
        pdf_path,
        company_name="CPA Packet",
        report_title="Review Dashboard",
        date_range_label=f"{year}-01-01 to {year}-12-31",
        body_lines=[
            PdfBodyLine(
                text=f"QBO Source Reports Present: {_format_source_status(qbo_sources_present)}"
            ),
            PdfBodyLine(text="Net Income", amount=_fmt(net_income)),
            PdfBodyLine(text="Ending Cash", amount=_fmt(ending_cash)),
            PdfBodyLine(text="Ending Liabilities", amount=_fmt(ending_liabilities)),
            PdfBodyLine(text="Distributions Total", amount=_fmt(distributions_total)),
            PdfBodyLine(text="Contributions Total", amount=_fmt(contributions_total)),
            PdfBodyLine(
                text="Shareholder Receivable Ending Balance",
                amount=_fmt(shareholder_receivable),
            ),
            PdfBodyLine(text=f"Contractor/1099 Review Count: {contractor_review_count}"),
            PdfBodyLine(
                text=f"Open Review Flags: {', '.join(review_flags) if review_flags else 'none'}"
            ),
            PdfBodyLine(
                text="Book review only; not shareholder basis or AAA",
                row_type="subtotal",
            ),
        ],
    )

    inputs = {
        "year": year,
        "net_income": _fmt(net_income),
        "ending_cash": _fmt(ending_cash),
        "ending_liabilities": _fmt(ending_liabilities),
        "distributions_total": _fmt(distributions_total),
        "contributions_total": _fmt(contributions_total),
        "shareholder_receivable": _fmt(shareholder_receivable),
        "contractor_review_count": contractor_review_count,
        "review_flags": review_flags,
        "qbo_sources_present": qbo_sources_present,
    }
    metadata = DeliverableMetadata(
        deliverable="review_dashboard",
        inputs=inputs,
        input_fingerprint=compute_input_fingerprint(inputs),
        schema_versions=SCHEMA_VERSIONS["review_dashboard"],
        artifacts=[str(markdown_path), str(pdf_path)],
        warnings=[],
        data_sources={"packet": "generated_artifacts"},
    )
    write_deliverable_metadata(metadata_path, metadata)
    return markdown_path, pdf_path


def _find_first(directory: Path, pattern: str) -> Path | None:
    if not directory.exists():
        return None
    return next(iter(sorted(directory.glob(pattern))), None)


def _find_year_specific(directory: Path, pattern: str) -> Path | None:
    match = _find_first(directory, pattern)
    if match is not None:
        return match
    return None


def _lookup_csv_amount(path: Path | None, key: str, match: str) -> str:
    if path is None or not path.exists():
        return "Unavailable"
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row.get(key, "").strip().lower() == match.lower():
                return row.get("amount", "Unavailable") or "Unavailable"
    return "Unavailable"


def _lookup_contains_amount(path: Path | None, key: str, needle: str) -> str:
    if path is None or not path.exists():
        return "Unavailable"
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if needle.lower() in row.get(key, "").strip().lower():
                return row.get("amount", "Unavailable") or "Unavailable"
    return "Unavailable"


def _sum_cash_rows(path: Path | None) -> str:
    if path is None or not path.exists():
        return "Unavailable"
    total = Decimal("0.00")
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            path_value = row.get("path", "").strip().lower()
            if "bank accounts" not in path_value or row.get("row_type") != "account":
                continue
            try:
                total += Decimal(row.get("amount", "0") or "0")
            except (InvalidOperation, TypeError):
                continue
    return f"{total:.2f}"


def _read_equity_value(path: Path | None, key: str) -> str:
    if path is None or not path.exists():
        return "Unavailable"
    with path.open(newline="", encoding="utf-8") as handle:
        first_row = next(csv.DictReader(handle), None)
    if first_row is None:
        return "Unavailable"
    return first_row.get(key, "Unavailable") or "Unavailable"


def _read_pipe_list(path: Path | None, key: str) -> list[str]:
    if path is None or not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        first_row = next(csv.DictReader(handle), None)
    if first_row is None:
        return []
    value = first_row.get(key, "") or ""
    return [item for item in value.split("|") if item]


def _count_flagged_contractors(path: Path | None) -> int:
    if path is None or not path.exists():
        return 0
    flagged = 0
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row.get("flagged_for_1099_review", "").strip().lower() == "true":
                flagged += 1
    return flagged


def _format_source_status(statuses: dict[str, bool]) -> str:
    return ", ".join(f"{key}={'yes' if value else 'no'}" for key, value in sorted(statuses.items()))


def _fmt(value: str) -> str:
    return value if value else "Unavailable"
