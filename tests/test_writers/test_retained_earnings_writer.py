from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from cpapacket.models.retained_earnings import RetainedEarningsRollforward
from cpapacket.writers.retained_earnings import (
    to_rollforward_csv_row,
    write_rollforward_csv,
    write_rollforward_data_json,
    write_rollforward_pdf,
)

_GOLDEN_CSV_PATH = Path("tests/fixtures/qbo/retained_earnings_rollforward_2025_golden.csv")


def _sample_rollforward() -> RetainedEarningsRollforward:
    return RetainedEarningsRollforward(
        beginning_re="1000.00",
        net_income="250.25",
        distributions="100.00",
        expected_ending_re="1150.25",
        actual_ending_re="1150.20",
        difference="0.05",
        status="Mismatch",
        flags=["basis_risk_distributions_exceed_net_income"],
    )


def test_to_rollforward_csv_row_formats_fields() -> None:
    row = to_rollforward_csv_row(
        year=2025,
        rollforward=_sample_rollforward(),
        miscoded_distribution_count=3,
    )

    assert row["year"] == "2025"
    assert row["beginning_re"] == "1000.00"
    assert row["actual_ending_re"] == "1150.20"
    assert row["status"] == "Mismatch"
    assert row["flags"] == "basis_risk_distributions_exceed_net_income"
    assert row["miscoded_distribution_count"] == "3"


def test_write_rollforward_csv_persists_single_summary_row(tmp_path: Path) -> None:
    output = tmp_path / "Retained_Earnings_Rollforward_2025.csv"

    write_rollforward_csv(
        path=output,
        year=2025,
        rollforward=_sample_rollforward(),
        miscoded_distribution_count=2,
    )

    with output.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 1
    assert rows[0]["year"] == "2025"
    assert rows[0]["difference"] == "0.05"
    assert rows[0]["miscoded_distribution_count"] == "2"


def test_write_rollforward_csv_matches_golden_snapshot(tmp_path: Path) -> None:
    output = tmp_path / "Retained_Earnings_Rollforward_2025.csv"
    write_rollforward_csv(
        path=output,
        year=2025,
        rollforward=_sample_rollforward(),
        miscoded_distribution_count=3,
    )

    expected = _GOLDEN_CSV_PATH.read_text(encoding="utf-8")
    actual = output.read_text(encoding="utf-8")
    assert actual == expected

    with output.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        header = next(reader)
        row = next(reader)

    assert header == [
        "year",
        "beginning_re",
        "net_income",
        "distributions",
        "expected_ending_re",
        "actual_ending_re",
        "difference",
        "status",
        "flags",
        "miscoded_distribution_count",
    ]
    assert row[0] == "2025"
    assert row[7] == "Mismatch"
    assert "2025" in output.name


def test_write_rollforward_data_json_contains_rollforward_payload(tmp_path: Path) -> None:
    output = tmp_path / "Retained_Earnings_Rollforward_2025_data.json"

    write_rollforward_data_json(
        path=output,
        year=2025,
        rollforward=_sample_rollforward(),
        miscoded_distribution_count=4,
        data_sources={"pnl": "api", "balance_sheet": "cache"},
    )

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["year"] == 2025
    assert payload["miscoded_distribution_count"] == 4
    assert payload["rollforward"]["expected_ending_re"] == "1150.25"
    assert payload["data_sources"] == {"pnl": "api", "balance_sheet": "cache"}


def test_write_rollforward_pdf_creates_nonempty_file(tmp_path: Path) -> None:
    pytest.importorskip("reportlab")

    output = tmp_path / "Retained_Earnings_Rollforward_2025.pdf"
    write_rollforward_pdf(
        path=output,
        year=2025,
        rollforward=_sample_rollforward(),
        miscoded_distribution_count=1,
        company_name="Example Co",
    )

    assert output.exists()
    assert output.stat().st_size > 0
    assert not list(tmp_path.glob("*.tmp"))
