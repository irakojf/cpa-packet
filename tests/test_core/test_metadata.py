from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

from cpapacket.core.metadata import (
    DeliverableMetadata,
    canonicalize_inputs,
    compute_input_fingerprint,
    default_metadata_path,
    read_deliverable_metadata,
    write_deliverable_metadata,
)


def test_compute_input_fingerprint_is_stable_for_key_order() -> None:
    left = {"year": 2025, "method": "accrual", "nested": {"b": 2, "a": 1}}
    right = {"nested": {"a": 1, "b": 2}, "method": "accrual", "year": 2025}

    assert compute_input_fingerprint(left) == compute_input_fingerprint(right)


def test_compute_input_fingerprint_changes_when_inputs_change() -> None:
    baseline = {"year": 2025, "method": "accrual", "include_debug": False}
    changed_year = {"year": 2024, "method": "accrual", "include_debug": False}
    changed_flag = {"year": 2025, "method": "accrual", "include_debug": True}

    baseline_fp = compute_input_fingerprint(baseline)
    assert baseline_fp != compute_input_fingerprint(changed_year)
    assert baseline_fp != compute_input_fingerprint(changed_flag)


def test_canonicalize_inputs_normalizes_supported_types() -> None:
    value = {
        "amount": Decimal("42.10"),
        "as_of": date(2025, 12, 31),
        "generated_at": datetime(2026, 1, 5, 8, 15, tzinfo=UTC),
        "path": Path("/tmp/example.csv"),
        "items": [Decimal("1.00"), Path("/tmp/child")],
    }

    canonical = canonicalize_inputs(value)
    assert canonical == (
        '{"amount":"42.10","as_of":"2025-12-31","generated_at":"2026-01-05T08:15:00+00:00",'
        '"items":["1.00","/tmp/child"],"path":"/tmp/example.csv"}'
    )


def test_default_metadata_path_targets_private_deliverables_dir(tmp_path: Path) -> None:
    path = default_metadata_path(output_root=tmp_path, deliverable_key="pnl")
    assert path == tmp_path / "_meta" / "private" / "deliverables" / "pnl_metadata.json"


def test_write_and_read_deliverable_metadata_roundtrip(tmp_path: Path) -> None:
    inputs = {"year": 2025, "method": "accrual"}
    metadata = DeliverableMetadata(
        deliverable="pnl",
        inputs=inputs,
        input_fingerprint=compute_input_fingerprint(inputs),
        schema_versions={"csv": "1.0"},
        artifacts=["01_Year-End_Profit_and_Loss/Profit_and_Loss_2025.csv"],
        warnings=["example warning"],
        data_sources={"qbo_pnl": "api"},
    )
    path = default_metadata_path(output_root=tmp_path, deliverable_key="pnl")

    write_deliverable_metadata(path, metadata)
    loaded = read_deliverable_metadata(path)

    assert path.exists()
    assert loaded == metadata
