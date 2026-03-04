"""Batch CSV writer utilities with atomic output semantics."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from cpapacket.core.filesystem import atomic_write


@dataclass(frozen=True)
class CsvWriterConfig:
    """Configuration for batch CSV serialization."""

    encoding: str = "utf-8"
    line_terminator: str = "\n"
    delimiter: str = ","


class CsvWriter:
    """Write tabular rows to CSV in one batch using atomic writes."""

    def __init__(self, *, config: CsvWriterConfig | None = None) -> None:
        self._config = config or CsvWriterConfig()

    def write_rows(
        self,
        output_path: str | Path,
        *,
        fieldnames: Sequence[str],
        rows: Iterable[Mapping[str, Any]],
    ) -> Path:
        if not fieldnames:
            raise ValueError("fieldnames must not be empty")

        normalized_headers = [name.strip() for name in fieldnames]
        if any(not name for name in normalized_headers):
            raise ValueError("fieldnames must not contain blank values")

        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        with atomic_write(
            output,
            mode="w",
            encoding=self._config.encoding,
            newline="",
        ) as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=normalized_headers,
                quoting=csv.QUOTE_MINIMAL,
                delimiter=self._config.delimiter,
                lineterminator=self._config.line_terminator,
                extrasaction="ignore",
            )
            writer.writeheader()
            for row in rows:
                writer.writerow(
                    {header: _serialize_cell(row.get(header)) for header in normalized_headers}
                )
        return output


def _serialize_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)
