from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from cpapacket.models.tax import EstimatedTaxPayment, TaxDeadline
from cpapacket.tax_tracker import TaxTrackerStorage


def test_storage_paths_use_config_root(tmp_path) -> None:
    storage = TaxTrackerStorage(config_root=tmp_path)

    assert storage.tracker_path(year=2026) == tmp_path / "tax_tracker_2026.json"
    assert storage.deadlines_path(year=2026) == tmp_path / "tax_deadlines_2026.json"


def test_load_missing_files_returns_empty_lists(tmp_path) -> None:
    storage = TaxTrackerStorage(config_root=tmp_path)

    assert storage.load_payments(year=2026) == []
    assert storage.load_deadlines(year=2026) == []


def test_payment_round_trip(tmp_path) -> None:
    storage = TaxTrackerStorage(config_root=tmp_path)
    payment = EstimatedTaxPayment(
        jurisdiction="Federal",
        due_date=date(2026, 4, 15),
        amount="1234.5",
        status="not_paid",
        last_updated=datetime(2026, 1, 2, 3, 4, tzinfo=UTC),
    )

    path = storage.save_payments(year=2026, payments=[payment])
    loaded = storage.load_payments(year=2026)

    assert path.exists()
    assert loaded == [payment]


def test_deadline_round_trip(tmp_path) -> None:
    storage = TaxTrackerStorage(config_root=tmp_path)
    deadline = TaxDeadline(
        jurisdiction="NY",
        name="Q1 Estimated Tax",
        due_date=date(2026, 4, 15),
        category="estimated_tax",
        completed=True,
    )

    path = storage.save_deadlines(year=2026, deadlines=[deadline])
    loaded = storage.load_deadlines(year=2026)

    assert path.exists()
    assert loaded == [deadline]


def test_invalid_json_payload_raises(tmp_path) -> None:
    storage = TaxTrackerStorage(config_root=tmp_path)
    path = storage.tracker_path(year=2026)
    path.write_text("{not-json}", encoding="utf-8")

    with pytest.raises(ValueError, match="Invalid json payload"):
        storage.load_payments(year=2026)


def test_invalid_year_is_rejected(tmp_path) -> None:
    storage = TaxTrackerStorage(config_root=tmp_path)

    with pytest.raises(ValueError, match="year must be in"):
        storage.tracker_path(year=1999)
