from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from cpapacket.writers.csv_writer import CsvWriter


def test_csv_writer_writes_batch_rows_with_expected_formats(tmp_path: Path) -> None:
    writer = CsvWriter()
    out = tmp_path / "report.csv"

    writer.write_rows(
        out,
        fieldnames=["name", "amount", "posted_on", "active", "notes"],
        rows=[
            {
                "name": "Consulting Revenue",
                "amount": Decimal("1234.50"),
                "posted_on": date(2025, 12, 31),
                "active": True,
                "notes": 'contains,comma and "quote"',
            },
            {
                "name": "Other Income",
                "amount": Decimal("-10.00"),
                "posted_on": datetime(2025, 1, 15, 8, 30),
                "active": False,
                "notes": None,
            },
        ],
    )

    content = out.read_text(encoding="utf-8")
    lines = content.splitlines()

    assert lines[0] == "name,amount,posted_on,active,notes"
    assert lines[1].startswith("Consulting Revenue,1234.50,2025-12-31,true,")
    assert '"contains,comma and ""quote"""' in lines[1]
    assert lines[2] == "Other Income,-10.00,2025-01-15,false,"
    assert "\r\n" not in content


def test_csv_writer_overwrites_atomically(tmp_path: Path) -> None:
    writer = CsvWriter()
    out = tmp_path / "overwrite.csv"
    out.write_text("old,data\n", encoding="utf-8")

    writer.write_rows(
        out,
        fieldnames=["id", "amount"],
        rows=[{"id": "1", "amount": Decimal("1.00")}],
    )

    assert out.read_text(encoding="utf-8") == "id,amount\n1,1.00\n"


def test_csv_writer_rejects_invalid_fieldnames(tmp_path: Path) -> None:
    writer = CsvWriter()
    out = tmp_path / "invalid.csv"

    with pytest.raises(ValueError, match="fieldnames must not be empty"):
        writer.write_rows(out, fieldnames=[], rows=[])

    with pytest.raises(ValueError, match="fieldnames must not contain blank values"):
        writer.write_rows(out, fieldnames=["ok", " "], rows=[])


def test_csv_writer_streaming_deduplicates_by_txn_id(tmp_path: Path) -> None:
    writer = CsvWriter()
    out = tmp_path / "streaming.csv"

    writer.write_rows_streaming(
        out,
        fieldnames=["txn_id", "name", "amount"],
        rows=[
            {"txn_id": "A1", "name": "row1", "amount": Decimal("10.00")},
            {"txn_id": "A1", "name": "dup", "amount": Decimal("99.00")},
            {"txn_id": "A2", "name": "row2", "amount": Decimal("20.00")},
        ],
    )

    lines = out.read_text(encoding="utf-8").splitlines()
    assert lines[0] == "txn_id,name,amount"
    assert lines[1] == "A1,row1,10.00"
    assert lines[2] == "A2,row2,20.00"
    assert len(lines) == 3


def test_csv_writer_streaming_without_dedupe_writes_all_rows(tmp_path: Path) -> None:
    writer = CsvWriter()
    out = tmp_path / "streaming_all.csv"

    writer.write_rows_streaming(
        out,
        fieldnames=["txn_id", "name"],
        rows=[
            {"txn_id": "A1", "name": "row1"},
            {"txn_id": "A1", "name": "dup"},
        ],
        dedupe_id_field=None,
    )

    lines = out.read_text(encoding="utf-8").splitlines()
    assert lines == ["txn_id,name", "A1,row1", "A1,dup"]
