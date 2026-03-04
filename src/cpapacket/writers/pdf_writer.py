"""Base ReportLab PDF writer utilities."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from typing import IO, Any, Literal, cast

from cpapacket.core.filesystem import atomic_write


@dataclass(frozen=True)
class PdfBodyLine:
    """Structured line item for level-aware PDF rendering."""

    text: str
    level: int = 0
    row_type: Literal["account", "header", "subtotal", "total"] = "account"


@dataclass(frozen=True)
class PdfWriterConfig:
    """Layout controls for generated PDF pages."""

    margin_left: float = 54.0
    margin_right: float = 54.0
    margin_top: float = 54.0
    margin_bottom: float = 54.0
    body_line_height: float = 14.0
    header_font_name: str = "Helvetica-Bold"
    header_font_size: int = 12
    body_font_name: str = "Helvetica"
    body_font_size: int = 10
    max_account_name_chars: int = 60
    indent_per_level: float = 12.0
    footer_font_name: str = "Helvetica"
    footer_font_size: int = 9


class PdfWriter:
    """Render text-based report content into a paginated PDF document."""

    def __init__(self, *, config: PdfWriterConfig | None = None) -> None:
        self._config = config or PdfWriterConfig()

    def write_report(
        self,
        output_path: str | Path,
        *,
        company_name: str,
        report_title: str,
        date_range_label: str,
        body_lines: Sequence[str | PdfBodyLine],
    ) -> Path:
        """Write a report PDF atomically to ``output_path``."""
        try:
            from reportlab.lib.pagesizes import letter  # type: ignore[import-untyped]
            from reportlab.pdfgen import canvas  # type: ignore[import-untyped]
        except ImportError as exc:  # pragma: no cover - environment-dependent
            raise RuntimeError("reportlab is required for PDF generation") from exc

        config = self._config
        normalized_company = company_name.strip() or "Unknown Company"
        normalized_title = report_title.strip() or "Report"
        normalized_range = date_range_label.strip() or "Date range unavailable"

        buffer = BytesIO()
        pdf = canvas.Canvas(buffer, pagesize=letter)
        width, height = letter

        page_number = 1
        normalized_lines = [_normalize_body_line(line) for line in body_lines]
        generated_label = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%SZ")
        lines_per_page = self._lines_per_page(height=height)
        total_pages = max((len(normalized_lines) + lines_per_page - 1) // lines_per_page, 1)
        y = self._draw_page_header(
            pdf=pdf,
            page_number=page_number,
            width=width,
            height=height,
            company_name=normalized_company,
            report_title=normalized_title,
            date_range_label=normalized_range,
        )

        for line in normalized_lines:
            if y < config.margin_bottom + config.body_line_height:
                self._draw_page_footer(
                    pdf=pdf,
                    page_number=page_number,
                    total_pages=total_pages,
                    width=width,
                    generated_label=generated_label,
                )
                pdf.showPage()
                page_number += 1
                y = self._draw_page_header(
                    pdf=pdf,
                    page_number=page_number,
                    width=width,
                    height=height,
                    company_name=normalized_company,
                    report_title=normalized_title,
                    date_range_label=normalized_range,
                )

            if line.row_type in {"header", "subtotal", "total"}:
                pdf.setFont(config.header_font_name, config.body_font_size)
            else:
                pdf.setFont(config.body_font_name, config.body_font_size)

            text = _truncate_with_ellipsis(line.text, max_chars=config.max_account_name_chars)
            indent = max(line.level, 0) * config.indent_per_level
            x = config.margin_left + indent
            pdf.drawString(x, y, text)

            if line.row_type == "subtotal":
                pdf.line(x, y - 2, width - config.margin_right, y - 2)
            elif line.row_type == "total":
                pdf.line(x, y - 2, width - config.margin_right, y - 2)
                pdf.line(x, y - 5, width - config.margin_right, y - 5)
            y -= config.body_line_height

        self._draw_page_footer(
            pdf=pdf,
            page_number=page_number,
            total_pages=total_pages,
            width=width,
            generated_label=generated_label,
        )
        pdf.save()

        output = Path(output_path)
        with atomic_write(output, mode="wb") as handle:
            binary_handle = cast(IO[bytes], handle)
            binary_handle.write(buffer.getvalue())
        return output

    def _draw_page_header(
        self,
        *,
        pdf: Any,
        page_number: int,
        width: float,
        height: float,
        company_name: str,
        report_title: str,
        date_range_label: str,
    ) -> float:
        del page_number
        config = self._config
        y = height - config.margin_top

        pdf.setFont(config.header_font_name, config.header_font_size)
        pdf.drawString(config.margin_left, y, company_name)
        y -= config.header_font_size + 4

        pdf.setFont(config.header_font_name, config.header_font_size)
        pdf.drawString(config.margin_left, y, report_title)
        y -= config.header_font_size + 2

        pdf.setFont(config.footer_font_name, config.footer_font_size)
        pdf.drawString(config.margin_left, y, date_range_label)
        y -= config.body_line_height

        return y

    def _draw_page_footer(
        self,
        *,
        pdf: Any,
        page_number: int,
        total_pages: int,
        width: float,
        generated_label: str,
    ) -> None:
        config = self._config
        label = f"Page {page_number} of {total_pages}"
        x = width / 2.0 - (len(label) * (config.footer_font_size * 0.26))
        y = config.margin_bottom / 2.0
        pdf.setFont(config.footer_font_name, config.footer_font_size)
        pdf.drawString(x, y, label)
        generated_x = config.margin_left
        pdf.drawString(generated_x, y, f"Generated {generated_label}")

    def _lines_per_page(self, *, height: float) -> int:
        config = self._config
        header_lines_height = (
            (config.header_font_size + 4)
            + (config.header_font_size + 2)
            + config.footer_font_size
            + config.body_line_height
        )
        available_height = height - config.margin_top - config.margin_bottom - header_lines_height
        return max(int(available_height // config.body_line_height), 1)


def _normalize_body_line(line: str | PdfBodyLine) -> PdfBodyLine:
    if isinstance(line, PdfBodyLine):
        return line
    return PdfBodyLine(text=line)


def _truncate_with_ellipsis(value: str, *, max_chars: int) -> str:
    if max_chars < 2:
        return value[:max(max_chars, 0)]
    if max_chars <= 3:
        return value[:max_chars]
    if len(value) <= max_chars:
        return value
    return f"{value[: max_chars - 3]}..."
