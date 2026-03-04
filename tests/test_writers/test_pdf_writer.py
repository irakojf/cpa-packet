from __future__ import annotations

from pathlib import Path

import pytest

from cpapacket.writers.pdf_writer import (
    PdfBodyLine,
    PdfTableRow,
    PdfTableSection,
    PdfWriter,
    PdfWriterConfig,
    _normalize_body_line,
    _normalize_table_row,
    _truncate_with_ellipsis,
)


def _ensure_reportlab_installed() -> None:
    pytest.importorskip("reportlab")


def test_pdf_writer_writes_pdf_file(tmp_path: Path) -> None:
    _ensure_reportlab_installed()

    destination = tmp_path / "report.pdf"
    writer = PdfWriter()

    lines = [f"Row {index:03d}" for index in range(1, 80)]
    result_path = writer.write_report(
        destination,
        company_name="Example Co",
        report_title="Year-End Profit and Loss",
        date_range_label="January 1, 2025 - December 31, 2025",
        body_lines=lines,
    )

    assert result_path == destination
    assert destination.exists()
    assert destination.stat().st_size > 0
    assert not list(tmp_path.glob("*.tmp"))


def test_pdf_writer_overwrites_existing_file_atomically(tmp_path: Path) -> None:
    _ensure_reportlab_installed()

    destination = tmp_path / "existing.pdf"
    destination.write_bytes(b"old-content")

    writer = PdfWriter()
    writer.write_report(
        destination,
        company_name="Example Co",
        report_title="Report",
        date_range_label="2025",
        body_lines=["single line"],
    )

    assert destination.exists()
    assert destination.read_bytes() != b"old-content"


def test_pdf_writer_uses_fallback_labels_for_blank_header_fields(tmp_path: Path) -> None:
    _ensure_reportlab_installed()

    destination = tmp_path / "fallback.pdf"
    writer = PdfWriter(config=PdfWriterConfig(body_line_height=12.0))

    writer.write_report(
        destination,
        company_name="   ",
        report_title="",
        date_range_label=" ",
        body_lines=["line"],
    )

    assert destination.exists()
    assert destination.stat().st_size > 0


def test_truncate_with_ellipsis_for_long_account_names() -> None:
    short = _truncate_with_ellipsis("Cash", max_chars=60)
    long = _truncate_with_ellipsis("A" * 100, max_chars=60)

    assert short == "Cash"
    assert len(long) == 60
    assert long.endswith("...")


def test_normalize_body_line_supports_plain_and_structured_lines() -> None:
    normalized_plain = _normalize_body_line("Revenue")
    structured = PdfBodyLine(text="Total Income", level=1, row_type="total")
    normalized_structured = _normalize_body_line(structured)

    assert normalized_plain == PdfBodyLine(text="Revenue")
    assert normalized_structured == structured


def test_normalize_table_row_supports_plain_and_structured_rows() -> None:
    normalized_plain = _normalize_table_row(("Payroll", 100, "ok"))
    structured = PdfTableRow(cells=("Total", "100.00"), row_type="total")
    normalized_structured = _normalize_table_row(structured)

    assert normalized_plain == PdfTableRow(cells=("Payroll", "100", "ok"))
    assert normalized_structured == structured


def test_pdf_writer_writes_reconciliation_table_pdf(tmp_path: Path) -> None:
    _ensure_reportlab_installed()

    destination = tmp_path / "recon-table.pdf"
    writer = PdfWriter()
    rows = [
        PdfTableRow(
            cells=("Payroll", "25000.00", "25000.00", "0.00", "Reconciled"),
            status="reconciled",
        ),
        PdfTableRow(
            cells=("Distributions", "5200.00", "5000.00", "200.00", "Mismatch"),
            status="mismatch",
        ),
        PdfTableRow(
            cells=("Total", "30200.00", "30000.00", "200.00", ""),
            row_type="total",
        ),
    ]
    result_path = writer.write_table_report(
        destination,
        company_name="Example Co",
        report_title="Retained Earnings Reconciliation",
        date_range_label="2025",
        columns=("Category", "Book", "Expected", "Delta", "Status"),
        rows=rows,
    )

    assert result_path == destination
    assert destination.exists()
    assert destination.stat().st_size > 0
    assert not list(tmp_path.glob("*.tmp"))


def test_pdf_writer_table_report_requires_non_blank_columns(tmp_path: Path) -> None:
    _ensure_reportlab_installed()

    writer = PdfWriter()
    with pytest.raises(ValueError, match="at least one non-blank heading"):
        writer.write_table_report(
            tmp_path / "invalid.pdf",
            company_name="Example Co",
            report_title="Report",
            date_range_label="2025",
            columns=(" ", ""),
            rows=[],
        )


def test_pdf_writer_writes_reconciliation_report_sections(tmp_path: Path) -> None:
    _ensure_reportlab_installed()

    destination = tmp_path / "reconciliation-sections.pdf"
    writer = PdfWriter()
    sections = [
        PdfTableSection(
            title="Payroll Reconciliation",
            headers=("Category", "Book", "Expected", "Delta", "Status"),
            rows=(
                PdfTableRow(
                    cells=(
                        "Payroll",
                        "25000.00",
                        "25000.00",
                        "0.00",
                        "RECONCILED",
                    ),
                    status="reconciled",
                ),
                PdfTableRow(
                    cells=(
                        "Distributions",
                        "5200.00",
                        "5000.00",
                        "200.00",
                        "MISMATCH",
                    ),
                    status="mismatch",
                ),
                PdfTableRow(
                    cells=("Total", "30200.00", "30000.00", "200.00", ""),
                    row_type="total",
                ),
            ),
        ),
        PdfTableSection(
            title="Retained Earnings Rollforward",
            headers=("Line Item", "Amount"),
            rows=(
                PdfTableRow(cells=("Beginning RE", "50000.00")),
                PdfTableRow(cells=("Net Income", "25000.00")),
                PdfTableRow(cells=("Distributions", "(10000.00)")),
                PdfTableRow(cells=("Ending RE", "65000.00"), row_type="total"),
            ),
        ),
    ]

    result_path = writer.write_reconciliation_report(
        destination,
        company_name="Example Co",
        report_title="Reconciliation Summary",
        date_range_label="2025",
        sections=sections,
    )

    assert result_path == destination
    assert destination.exists()
    assert destination.stat().st_size > 0
    assert not list(tmp_path.glob("*.tmp"))


def test_truncate_with_ellipsis_handles_small_limits() -> None:
    assert _truncate_with_ellipsis("abcdef", max_chars=1) == "a"
    assert _truncate_with_ellipsis("abcdef", max_chars=0) == ""
    assert _truncate_with_ellipsis("abcdef", max_chars=2) == "ab"


def test_lines_per_page_has_minimum_of_one() -> None:
    writer = PdfWriter(config=PdfWriterConfig(margin_top=1000.0, margin_bottom=1000.0))
    assert writer._lines_per_page(height=792.0) == 1


def test_pdf_writer_renders_structured_body_rows(tmp_path: Path) -> None:
    _ensure_reportlab_installed()

    destination = tmp_path / "structured.pdf"
    writer = PdfWriter()
    body_lines = [
        PdfBodyLine(text="Income", row_type="header"),
        PdfBodyLine(text="Services", level=1, row_type="account"),
        PdfBodyLine(text="Subtotal Income", level=1, row_type="subtotal"),
        PdfBodyLine(text="Net Income", row_type="total"),
    ]
    result_path = writer.write_report(
        destination,
        company_name="Example Co",
        report_title="Profit and Loss",
        date_range_label="2025",
        body_lines=body_lines,
    )

    assert result_path == destination
    assert destination.exists()
    assert destination.stat().st_size > 0
