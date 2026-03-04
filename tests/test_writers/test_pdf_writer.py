from __future__ import annotations

from pathlib import Path

import pytest

from cpapacket.writers.pdf_writer import (
    PdfBodyLine,
    PdfWriter,
    PdfWriterConfig,
    _normalize_body_line,
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
