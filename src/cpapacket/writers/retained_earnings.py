"""Equity review artifact writers."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import IO, Any, cast

from cpapacket.core.filesystem import atomic_write
from cpapacket.models.retained_earnings import RetainedEarningsRollforward
from cpapacket.reconciliation.retained_earnings import (
    DistributionBalanceBridge,
    DistributionBridgeDetailRow,
    EquityActivityRow,
    EquityTieOutRow,
)
from cpapacket.writers.csv_writer import CsvWriter
from cpapacket.writers.json_writer import JsonWriter
from cpapacket.writers.pdf_writer import PdfBodyLine, PdfWriter

_ROLLFORWARD_FIELDS: tuple[str, ...] = (
    "year",
    "beginning_book_equity_bucket",
    "current_year_net_income",
    "current_year_distributions_gl",
    "current_year_distributions_bs_change",
    "current_year_contributions",
    "other_direct_equity_postings",
    "expected_ending_book_equity_bucket_gl_basis",
    "expected_ending_book_equity_bucket_bs_basis",
    "actual_ending_book_equity_bucket",
    "gl_basis_difference",
    "bs_basis_difference",
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
    """Build a single book-equity rollforward CSV row."""
    return {
        "year": str(year),
        "beginning_book_equity_bucket": f"{rollforward.beginning_book_equity_bucket:.2f}",
        "current_year_net_income": f"{rollforward.current_year_net_income:.2f}",
        "current_year_distributions_gl": f"{rollforward.current_year_distributions_gl:.2f}",
        "current_year_distributions_bs_change": (
            f"{rollforward.current_year_distributions_bs_change:.2f}"
        ),
        "current_year_contributions": f"{rollforward.current_year_contributions:.2f}",
        "other_direct_equity_postings": f"{rollforward.other_direct_equity_postings:.2f}",
        "expected_ending_book_equity_bucket_gl_basis": (
            f"{rollforward.expected_ending_book_equity_bucket_gl_basis:.2f}"
        ),
        "expected_ending_book_equity_bucket_bs_basis": (
            f"{rollforward.expected_ending_book_equity_bucket_bs_basis:.2f}"
        ),
        "actual_ending_book_equity_bucket": f"{rollforward.actual_ending_book_equity_bucket:.2f}",
        "gl_basis_difference": f"{rollforward.gl_basis_difference:.2f}",
        "bs_basis_difference": f"{rollforward.bs_basis_difference:.2f}",
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
    """Write book-equity rollforward CSV artifact."""
    CsvWriter().write_rows(
        path,
        fieldnames=list(_ROLLFORWARD_FIELDS),
        rows=[
            to_rollforward_csv_row(
                year=year,
                rollforward=rollforward,
                miscoded_distribution_count=miscoded_distribution_count,
            )
        ],
    )


def write_equity_tie_out_csv(*, path: Path, rows: Sequence[EquityTieOutRow]) -> None:
    """Write the balance-sheet tie-out CSV."""
    CsvWriter().write_rows(
        path,
        fieldnames=[
            "year",
            "as_of_date",
            "source_statement",
            "line_label",
            "classification",
            "amount",
            "included_in_book_equity_bucket",
            "bucket_component",
            "review_note",
        ],
        rows=[
            {
                "year": item.year,
                "as_of_date": item.as_of_date,
                "source_statement": item.source_statement,
                "line_label": item.line_label,
                "classification": item.classification,
                "amount": f"{item.amount:.2f}",
                "included_in_book_equity_bucket": str(item.included_in_book_equity_bucket).lower(),
                "bucket_component": item.bucket_component,
                "review_note": item.review_note,
            }
            for item in rows
        ],
    )


def write_distribution_bridge_csv(
    *,
    path: Path,
    year: int,
    bridge: DistributionBalanceBridge,
) -> None:
    """Write the high-level distribution bridge CSV."""
    CsvWriter().write_rows(
        path,
        fieldnames=[
            "year",
            "distribution_total_gl",
            "distribution_total_bs_change",
            "difference",
            "difference_status",
        ],
        rows=[
            {
                "year": year,
                "distribution_total_gl": f"{bridge.distribution_total_gl:.2f}",
                "distribution_total_bs_change": f"{bridge.distribution_total_bs_change:.2f}",
                "difference": f"{bridge.difference:.2f}",
                "difference_status": bridge.status,
            }
        ],
    )


def write_distribution_bridge_detail_csv(
    *,
    path: Path,
    rows: Sequence[DistributionBridgeDetailRow],
) -> None:
    """Write best-effort detail rows behind the GL-vs-BS distribution bridge."""
    CsvWriter().write_rows(
        path,
        fieldnames=[
            "date",
            "txn_type",
            "doc_num",
            "payee",
            "account_name",
            "memo",
            "signed_amount",
            "bridge_bucket",
            "reason",
        ],
        rows=[
            {
                "date": item.date.isoformat(),
                "txn_type": item.txn_type,
                "doc_num": item.doc_num,
                "payee": item.payee,
                "account_name": item.account_name,
                "memo": item.memo,
                "signed_amount": f"{item.signed_amount:.2f}",
                "bridge_bucket": item.bridge_bucket,
                "reason": item.reason,
            }
            for item in rows
        ],
    )


def write_equity_activity_csv(*, path: Path, rows: Sequence[EquityActivityRow]) -> None:
    """Write a generic equity activity CSV."""
    CsvWriter().write_rows(
        path,
        fieldnames=[
            "date",
            "txn_type",
            "doc_num",
            "payee",
            "account_name",
            "memo",
            "debit",
            "credit",
            "signed_amount",
            "classification",
            "review_flag",
        ],
        rows=[
            {
                "date": item.date.isoformat(),
                "txn_type": item.txn_type,
                "doc_num": item.doc_num,
                "payee": item.payee,
                "account_name": item.account_name,
                "memo": item.memo,
                "debit": f"{item.debit:.2f}",
                "credit": f"{item.credit:.2f}",
                "signed_amount": f"{item.signed_amount:.2f}",
                "classification": item.classification,
                "review_flag": item.review_flag,
            }
            for item in rows
        ],
    )


def write_rollforward_data_json(
    *,
    path: Path,
    year: int,
    rollforward: RetainedEarningsRollforward,
    miscoded_distribution_count: int,
    data_sources: dict[str, str] | None = None,
    equity_tie_out_rows: Sequence[EquityTieOutRow] = (),
    distribution_bridge: DistributionBalanceBridge | None = None,
) -> None:
    """Write equity-review ``_data.json`` payload artifact."""
    payload: dict[str, Any] = {
        "year": year,
        "rollforward": rollforward.model_dump(mode="json"),
        "miscoded_distribution_count": max(miscoded_distribution_count, 0),
        "data_sources": dict(data_sources or {}),
        "equity_tie_out_rows": [
            {
                "year": row.year,
                "as_of_date": row.as_of_date,
                "source_statement": row.source_statement,
                "line_label": row.line_label,
                "classification": row.classification,
                "amount": f"{row.amount:.2f}",
                "included_in_book_equity_bucket": row.included_in_book_equity_bucket,
                "bucket_component": row.bucket_component,
                "review_note": row.review_note,
            }
            for row in equity_tie_out_rows
        ],
    }
    if distribution_bridge is not None:
        payload["distribution_bridge"] = {
            "prior_distribution_balance": f"{distribution_bridge.prior_distribution_balance:.2f}",
            "current_distribution_balance": (
                f"{distribution_bridge.current_distribution_balance:.2f}"
            ),
            "distribution_total_gl": f"{distribution_bridge.distribution_total_gl:.2f}",
            "distribution_total_bs_change": (
                f"{distribution_bridge.distribution_total_bs_change:.2f}"
            ),
            "difference": f"{distribution_bridge.difference:.2f}",
            "status": distribution_bridge.status,
        }

    JsonWriter().write_payload(path, payload=payload, no_raw=False, redact=False)


def write_rollforward_pdf(
    *,
    path: Path,
    year: int,
    rollforward: RetainedEarningsRollforward,
    miscoded_distribution_count: int,
    company_name: str = "Unknown Company",
) -> None:
    """Write the CPA-facing book-equity review PDF."""
    note_lines = [
        PdfBodyLine(
            text=(
                "This is a book-equity review schedule. It is not shareholder basis, AAA, "
                "or a tax-basis capital schedule."
            ),
            row_type="subtotal",
        ),
        PdfBodyLine(
            text=f"Likely Miscoded Distributions: {max(miscoded_distribution_count, 0)}",
            row_type="account",
        ),
    ]
    PdfWriter().write_report(
        path,
        company_name=company_name,
        report_title="Book Equity Rollforward and QBO Tie-Out",
        date_range_label=f"{year}-01-01 to {year}-12-31",
        body_lines=[
            *note_lines,
            PdfBodyLine(text="Summary", row_type="header"),
            PdfBodyLine(
                text="Beginning book equity bucket",
                amount=f"{rollforward.beginning_book_equity_bucket:.2f}",
            ),
            PdfBodyLine(
                text="Net income",
                amount=f"{rollforward.current_year_net_income:.2f}",
            ),
            PdfBodyLine(
                text="Distributions from GL",
                amount=f"{rollforward.current_year_distributions_gl:.2f}",
            ),
            PdfBodyLine(
                text="Distributions from balance-sheet change",
                amount=f"{rollforward.current_year_distributions_bs_change:.2f}",
            ),
            PdfBodyLine(
                text="Contributions",
                amount=f"{rollforward.current_year_contributions:.2f}",
            ),
            PdfBodyLine(
                text="Other direct equity postings",
                amount=f"{rollforward.other_direct_equity_postings:.2f}",
            ),
            PdfBodyLine(text="Rollforward", row_type="header"),
            PdfBodyLine(
                text="Expected ending bucket on GL basis",
                amount=f"{rollforward.expected_ending_book_equity_bucket_gl_basis:.2f}",
            ),
            PdfBodyLine(
                text="Expected ending bucket on BS basis",
                amount=f"{rollforward.expected_ending_book_equity_bucket_bs_basis:.2f}",
            ),
            PdfBodyLine(
                text="Actual ending bucket",
                amount=f"{rollforward.actual_ending_book_equity_bucket:.2f}",
                row_type="total",
            ),
            PdfBodyLine(
                text="GL basis difference",
                amount=f"{rollforward.gl_basis_difference:.2f}",
            ),
            PdfBodyLine(
                text="BS basis difference",
                amount=f"{rollforward.bs_basis_difference:.2f}",
            ),
            PdfBodyLine(text="Review Flags", row_type="header"),
            PdfBodyLine(text=f"Status: {rollforward.status}"),
            PdfBodyLine(text=f"Flags: {_format_flags(rollforward.flags)}"),
        ],
    )


def write_cpa_notes(*, path: Path) -> None:
    """Write a short CPA methodology note file."""
    payload = "\n".join(
        [
            "# CPA Notes",
            "",
            "- This is a book-equity review package.",
            "- It is not shareholder basis.",
            "- It is not AAA/OAA.",
            "- QBO may display net income and distributions as separate equity lines.",
            "- When GL distributions and balance-sheet distributions do not agree, both are shown.",
            "- Shareholder receivable activity requires manual CPA judgment.",
            "",
        ]
    )
    with atomic_write(path, mode="w", encoding="utf-8", newline="\n") as handle:
        cast(IO[str], handle).write(payload)


def _format_flags(flags: Sequence[str]) -> str:
    if not flags:
        return "none"
    return ", ".join(flags)
