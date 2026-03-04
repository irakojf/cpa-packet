"""Base ReportLab PDF writer utilities."""

from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any, Sequence

from cpapacket.core.filesystem import atomic_write


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
        body_lines: Sequence[str],
    ) -> Path:
        """Write a report PDF atomically to ``output_path``."""
        try:
            from reportlab.lib.pagesizes import letter
            from reportlab.pdfgen import canvas
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
        y = self._draw_page_header(
            pdf=pdf,
            page_number=page_number,
            width=width,
            height=height,
            company_name=normalized_company,
            report_title=normalized_title,
            date_range_label=normalized_range,
        )

        for line in body_lines:
            if y < config.margin_bottom + config.body_line_height:
                self._draw_page_footer(pdf=pdf, page_number=page_number, width=width)
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

            pdf.setFont(config.body_font_name, config.body_font_size)
            max_chars = max(int((width - config.margin_left - config.margin_right) / 5.5), 1)
            pdf.drawString(config.margin_left, y, line[:max_chars])
            y -= config.body_line_height

        self._draw_page_footer(pdf=pdf, page_number=page_number, width=width)
        pdf.save()

        output = Path(output_path)
        with atomic_write(output, mode="wb") as handle:
            handle.write(buffer.getvalue())
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

    def _draw_page_footer(self, *, pdf: Any, page_number: int, width: float) -> None:
        config = self._config
        label = f"Page {page_number}"
        x = width / 2.0 - (len(label) * (config.footer_font_size * 0.26))
        y = config.margin_bottom / 2.0
        pdf.setFont(config.footer_font_name, config.footer_font_size)
        pdf.drawString(x, y, label)
