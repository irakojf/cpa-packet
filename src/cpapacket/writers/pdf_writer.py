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
    amount: str | None = None


@dataclass(frozen=True)
class PdfTableRow:
    """Structured row for reconciliation table rendering."""

    cells: tuple[str, ...]
    row_type: Literal["row", "total"] = "row"
    status: Literal["reconciled", "mismatch"] | None = None


@dataclass(frozen=True)
class PdfTableSection:
    """Table section with title, headers, and rows."""

    title: str
    headers: Sequence[str]
    rows: Sequence[PdfTableRow]


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
    table_header_font_name: str = "Helvetica-Bold"
    table_row_height: float = 16.0
    table_cell_padding: float = 4.0
    section_spacing: float = 8.0


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

            font_name = (
                config.header_font_name
                if line.row_type in {"header", "subtotal", "total"}
                else config.body_font_name
            )
            pdf.setFont(font_name, config.body_font_size)

            indent = max(line.level, 0) * config.indent_per_level
            x = config.margin_left + indent
            text = _truncate_with_ellipsis(line.text, max_chars=config.max_account_name_chars)
            pdf.drawString(x, y, text)

            if line.amount is not None:
                amount_x = width - config.margin_right
                pdf.drawRightString(amount_x, y, line.amount)

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

    def write_reconciliation_report(
        self,
        output_path: str | Path,
        *,
        company_name: str,
        report_title: str,
        date_range_label: str,
        sections: Sequence[PdfTableSection],
    ) -> Path:
        """Write a reconciliation-oriented report with table sections."""
        try:
            from reportlab.lib import colors  # type: ignore[import-untyped]
            from reportlab.lib.pagesizes import letter
            from reportlab.pdfgen import canvas
        except ImportError as exc:  # pragma: no cover - environment-dependent
            raise RuntimeError("reportlab is required for PDF generation") from exc

        config = self._config
        normalized_company = company_name.strip() or "Unknown Company"
        normalized_title = report_title.strip() or "Report"
        normalized_range = date_range_label.strip() or "Date range unavailable"
        generated_label = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%SZ")

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
        total_pages = self._estimate_table_pages(height=height, sections=sections)
        available_width = width - config.margin_left - config.margin_right

        for section in sections:
            headers = [_truncate_with_ellipsis(header, max_chars=40) for header in section.headers]
            if not headers:
                continue

            minimum_section_height = (
                config.table_row_height * 2.0
                + config.body_line_height
                + config.section_spacing
            )
            if y - minimum_section_height < config.margin_bottom:
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

            pdf.setFillColor(colors.black)
            pdf.setFont(config.header_font_name, config.body_font_size)
            section_title = _truncate_with_ellipsis(section.title, max_chars=80)
            pdf.drawString(config.margin_left, y, section_title)
            y -= config.body_line_height

            column_count = len(headers)
            column_width = available_width / float(column_count)

            y = self._draw_section_table_row(
                pdf=pdf,
                y=y,
                width=width,
                row=headers,
                column_width=column_width,
                is_header=True,
                row_type="row",
                status=None,
                colors=colors,
            )

            for row in section.rows:
                if y - config.table_row_height < config.margin_bottom:
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
                    y = self._draw_section_table_row(
                        pdf=pdf,
                        y=y,
                        width=width,
                        row=headers,
                        column_width=column_width,
                        is_header=True,
                        row_type="row",
                        status=None,
                        colors=colors,
                    )

                padded_cells = list(row.cells[:column_count])
                if len(padded_cells) < column_count:
                    padded_cells.extend([""] * (column_count - len(padded_cells)))
                y = self._draw_section_table_row(
                    pdf=pdf,
                    y=y,
                    width=width,
                    row=padded_cells,
                    column_width=column_width,
                    is_header=False,
                    row_type=row.row_type,
                    status=row.status,
                    colors=colors,
                )

            y -= config.section_spacing

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

    def write_table_report(
        self,
        output_path: str | Path,
        *,
        company_name: str,
        report_title: str,
        date_range_label: str,
        columns: Sequence[str],
        rows: Sequence[Sequence[object] | PdfTableRow],
    ) -> Path:
        """Write a reconciliation table PDF with optional status highlighting."""
        try:
            from reportlab.lib import colors
            from reportlab.lib.pagesizes import letter
            from reportlab.pdfgen import canvas
        except ImportError as exc:  # pragma: no cover - environment-dependent
            raise RuntimeError("reportlab is required for PDF generation") from exc

        normalized_columns = [column.strip() for column in columns if column.strip()]
        if not normalized_columns:
            raise ValueError("columns must contain at least one non-blank heading")

        config = self._config
        normalized_company = company_name.strip() or "Unknown Company"
        normalized_title = report_title.strip() or "Report"
        normalized_range = date_range_label.strip() or "Date range unavailable"
        normalized_rows = [_normalize_table_row(row) for row in rows]
        generated_label = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%SZ")

        buffer = BytesIO()
        pdf = canvas.Canvas(buffer, pagesize=letter)
        width, height = letter
        page_number = 1
        lines_per_page = self._table_rows_per_page(height=height)
        total_pages = max((len(normalized_rows) + lines_per_page - 1) // lines_per_page, 1)
        y = self._draw_page_header(
            pdf=pdf,
            page_number=page_number,
            width=width,
            height=height,
            company_name=normalized_company,
            report_title=normalized_title,
            date_range_label=normalized_range,
        )
        y = self._draw_table_header(
            pdf=pdf,
            width=width,
            y=y,
            columns=normalized_columns,
            color_fill=colors.whitesmoke,
            color_line=colors.black,
        )

        for row in normalized_rows:
            if y < config.margin_bottom + config.table_row_height * 2:
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
                y = self._draw_table_header(
                    pdf=pdf,
                    width=width,
                    y=y,
                    columns=normalized_columns,
                    color_fill=colors.whitesmoke,
                    color_line=colors.black,
                )

            y = self._draw_table_row(
                pdf=pdf,
                width=width,
                y=y,
                columns=len(normalized_columns),
                row=row,
                color_ok=colors.HexColor("#e8f5e9"),
                color_mismatch=colors.HexColor("#ffebee"),
                color_line=colors.black,
            )

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

    def _estimate_table_pages(self, *, height: float, sections: Sequence[PdfTableSection]) -> int:
        config = self._config
        header_lines_height = (
            (config.header_font_size + 4)
            + (config.header_font_size + 2)
            + config.footer_font_size
            + config.body_line_height
        )
        usable_height = height - config.margin_top - config.margin_bottom - header_lines_height
        if usable_height <= 0:
            return 1

        consumed = 0.0
        pages = 1
        for section in sections:
            section_rows = max(len(section.rows), 1)
            section_height = (
                config.body_line_height
                + config.table_row_height
                + (section_rows * config.table_row_height)
                + config.section_spacing
            )
            if consumed + section_height > usable_height and consumed > 0:
                pages += 1
                consumed = 0.0
            consumed += section_height
        return max(pages, 1)

    def _draw_section_table_row(
        self,
        *,
        pdf: Any,
        y: float,
        width: float,
        row: Sequence[str],
        column_width: float,
        is_header: bool,
        row_type: Literal["row", "total"],
        status: Literal["reconciled", "mismatch"] | None,
        colors: Any,
    ) -> float:
        config = self._config
        row_top = y
        row_bottom = y - config.table_row_height
        if is_header:
            pdf.setFillColor(colors.lightgrey)
            pdf.rect(
                config.margin_left,
                row_bottom,
                width - config.margin_left - config.margin_right,
                config.table_row_height,
                stroke=0,
                fill=1,
            )

        text_color = colors.black
        if not is_header:
            if status == "reconciled":
                text_color = colors.darkgreen
            elif status == "mismatch":
                text_color = colors.red
        pdf.setFillColor(text_color)

        font_name = (
            config.table_header_font_name
            if (is_header or row_type == "total")
            else config.body_font_name
        )
        pdf.setFont(font_name, config.body_font_size)

        max_chars = max(int((column_width - (2 * config.table_cell_padding)) // 4), 6)
        for index, cell in enumerate(row):
            x = config.margin_left + (index * column_width)
            cell_text = _truncate_with_ellipsis(str(cell), max_chars=max_chars)
            draw_x = x + config.table_cell_padding
            if index > 0:
                cell_width_estimate = len(cell_text) * (config.body_font_size * 0.5)
                draw_x = x + column_width - config.table_cell_padding - cell_width_estimate
            pdf.drawString(draw_x, row_bottom + (config.table_row_height * 0.3), cell_text)

        pdf.setFillColor(colors.black)
        pdf.setStrokeColor(colors.grey)
        pdf.line(config.margin_left, row_bottom, width - config.margin_right, row_bottom)
        if row_type == "total" and not is_header:
            pdf.setStrokeColor(colors.black)
            pdf.line(
                config.margin_left,
                row_bottom - 2,
                width - config.margin_right,
                row_bottom - 2,
            )

        return row_top - config.table_row_height

    def _table_rows_per_page(self, *, height: float) -> int:
        config = self._config
        header_lines_height = (
            (config.header_font_size + 4)
            + (config.header_font_size + 2)
            + config.footer_font_size
            + config.table_row_height
        )
        available_height = height - config.margin_top - config.margin_bottom - header_lines_height
        return max(int(available_height // config.table_row_height), 1)

    def _draw_table_header(
        self,
        *,
        pdf: Any,
        width: float,
        y: float,
        columns: Sequence[str],
        color_fill: Any,
        color_line: Any,
    ) -> float:
        config = self._config
        left = config.margin_left
        right = width - config.margin_right
        column_width = (right - left) / len(columns)

        pdf.setFillColor(color_fill)
        pdf.rect(
            left,
            y - config.table_row_height + 2,
            right - left,
            config.table_row_height,
            fill=1,
            stroke=0,
        )
        pdf.setFillColor(color_line)
        pdf.setFont(config.header_font_name, config.body_font_size)
        for index, heading in enumerate(columns):
            x = left + (index * column_width) + config.table_cell_padding
            max_chars = self._table_col_max_chars(column_width=column_width)
            heading_text = _truncate_with_ellipsis(heading, max_chars=max_chars)
            pdf.drawString(x, y - config.body_font_size, heading_text)

        pdf.line(left, y - config.table_row_height + 2, right, y - config.table_row_height + 2)
        return y - config.table_row_height

    def _draw_table_row(
        self,
        *,
        pdf: Any,
        width: float,
        y: float,
        columns: int,
        row: PdfTableRow,
        color_ok: Any,
        color_mismatch: Any,
        color_line: Any,
    ) -> float:
        config = self._config
        left = config.margin_left
        right = width - config.margin_right
        column_width = (right - left) / columns

        fill_color = None
        if row.status == "reconciled":
            fill_color = color_ok
        elif row.status == "mismatch":
            fill_color = color_mismatch
        if fill_color is not None:
            pdf.setFillColor(fill_color)
            pdf.rect(
                left,
                y - config.table_row_height + 2,
                right - left,
                config.table_row_height,
                fill=1,
                stroke=0,
            )

        if row.row_type == "total":
            pdf.setFont(config.table_header_font_name, config.body_font_size)
            pdf.line(left, y + 1, right, y + 1)
        else:
            pdf.setFont(config.body_font_name, config.body_font_size)

        pdf.setFillColor(color_line)
        for index in range(columns):
            cell = row.cells[index] if index < len(row.cells) else ""
            x = left + (index * column_width) + config.table_cell_padding
            max_chars = self._table_col_max_chars(column_width=column_width)
            cell_text = _truncate_with_ellipsis(cell, max_chars=max_chars)
            pdf.drawString(x, y - config.body_font_size, cell_text)

        pdf.line(left, y - config.table_row_height + 2, right, y - config.table_row_height + 2)
        return y - config.table_row_height

    def _table_col_max_chars(self, *, column_width: float) -> int:
        config = self._config
        content_width = column_width - (config.table_cell_padding * 2)
        return max(int(content_width / (config.body_font_size * 0.52)), 4)


def _normalize_body_line(line: str | PdfBodyLine) -> PdfBodyLine:
    if isinstance(line, PdfBodyLine):
        return line
    return PdfBodyLine(text=line)


def _normalize_table_row(row: Sequence[object] | PdfTableRow) -> PdfTableRow:
    if isinstance(row, PdfTableRow):
        return row
    return PdfTableRow(cells=tuple(str(cell) for cell in row))


def _truncate_with_ellipsis(value: str, *, max_chars: int) -> str:
    if max_chars < 2:
        return value[: max(max_chars, 0)]
    if max_chars <= 3:
        return value[:max_chars]
    if len(value) <= max_chars:
        return value
    return f"{value[: max_chars - 3]}..."
