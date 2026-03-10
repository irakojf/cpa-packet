"""Persistent storage helpers for estimated tax tracker data."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, TypeVar

from platformdirs import user_config_dir

from cpapacket.core.filesystem import atomic_write, ensure_directory
from cpapacket.models.tax import EstimatedTaxPayment, TaxDeadline

_T = TypeVar("_T", EstimatedTaxPayment, TaxDeadline)


class TaxTrackerStorage:
    """Read/write tax tracker JSON state under user config directory."""

    def __init__(self, *, config_root: Path | None = None) -> None:
        if config_root is not None:
            root = Path(config_root)
        else:
            root = Path(user_config_dir("cpapacket", "cpapacket"))
        self._config_root = root

    @property
    def config_root(self) -> Path:
        """Resolved config directory root."""
        return self._config_root

    def tracker_path(self, *, year: int) -> Path:
        """Return persistent tracker json path for year."""
        normalized_year = _normalize_year(year)
        return self._config_root / f"tax_tracker_{normalized_year}.json"

    def deadlines_path(self, *, year: int) -> Path:
        """Return persistent deadline json path for year."""
        normalized_year = _normalize_year(year)
        return self._config_root / f"tax_deadlines_{normalized_year}.json"

    def load_payments(self, *, year: int) -> list[EstimatedTaxPayment]:
        """Load tracker payment rows for a year; missing file returns empty list."""
        path = self.tracker_path(year=year)
        return _load_models(path=path, model_type=EstimatedTaxPayment)

    def save_payments(self, *, year: int, payments: list[EstimatedTaxPayment]) -> Path:
        """Persist tracker payment rows for a year using atomic write."""
        path = self.tracker_path(year=year)
        _save_models(path=path, records=payments)
        return path

    def load_deadlines(self, *, year: int) -> list[TaxDeadline]:
        """Load deadline rows for a year; missing file returns empty list."""
        path = self.deadlines_path(year=year)
        return _load_models(path=path, model_type=TaxDeadline)

    def save_deadlines(self, *, year: int, deadlines: list[TaxDeadline]) -> Path:
        """Persist deadline rows for a year using atomic write."""
        path = self.deadlines_path(year=year)
        _save_models(path=path, records=deadlines)
        return path


def _normalize_year(year: int) -> int:
    if year < 2000 or year > 2100:
        raise ValueError("year must be in [2000, 2100]")
    return year


def _load_models(*, path: Path, model_type: type[_T]) -> list[_T]:
    if not path.exists():
        return []

    raw_payload = path.read_text(encoding="utf-8")
    try:
        payload = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid json payload in {path}") from exc

    if not isinstance(payload, list):
        raise ValueError(f"Expected list payload in {path}")

    return [model_type.model_validate(item) for item in payload]


def _save_models(*, path: Path, records: list[_T]) -> None:
    ensure_directory(path.parent)
    serializable: list[dict[str, Any]] = [record.model_dump(mode="json") for record in records]
    with atomic_write(path, mode="w", encoding="utf-8", newline="\n") as handle:
        json.dump(serializable, handle, sort_keys=True)
