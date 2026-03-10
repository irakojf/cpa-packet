from __future__ import annotations

import json
from collections.abc import Generator
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from cpapacket.core.context import RunContext
from cpapacket.deliverables.general_ledger import (
    GeneralLedgerDeliverable,
    GeneralLedgerMonthlySlice,
    fetch_general_ledger_monthly_slices,
    merge_general_ledger_monthly_slices,
)
from cpapacket.models.general_ledger import GeneralLedgerRow


def _ctx(tmp_path: Path, *, on_conflict: str = "abort") -> RunContext:
    return RunContext(
        year=2025,
        year_source="explicit",
        out_dir=tmp_path,
        method="accrual",
        non_interactive=True,
        on_conflict=on_conflict,
        incremental=False,
        force=False,
        no_cache=False,
        no_raw=False,
        redact=False,
        include_debug=False,
        verbose=False,
        quiet=False,
        plain=False,
        skip=[],
        owner_keywords=[],
        gusto_available=True,
    )


def _row(
    txn_id: str,
    doc_num: str,
    *,
    debit: Decimal = Decimal("100.00"),
    credit: Decimal = Decimal("0.00"),
) -> GeneralLedgerRow:
    return GeneralLedgerRow(
        txn_id=txn_id,
        date=date(2025, 1, 15),
        transaction_type="Journal Entry",
        document_number=doc_num,
        account_name="Cash",
        account_type="Bank",
        payee="Vendor",
        memo="Note",
        debit=debit,
        credit=credit,
    )


_ORIGINAL_FETCH = fetch_general_ledger_monthly_slices
_ORIGINAL_MERGE = merge_general_ledger_monthly_slices


class _StubProvider:
    def get_general_ledger(self, year: int, month: int) -> dict[str, Any]:
        return {"year": year, "month": month}


def _fake_slices() -> tuple[GeneralLedgerMonthlySlice, ...]:
    return tuple(
        GeneralLedgerMonthlySlice(month=month, payload={"month": month})
        for month in (1, 2)
    )


@pytest.fixture(autouse=True)  # type: ignore[untyped-decorator]
def _restore_fetch(monkeypatch: pytest.MonkeyPatch) -> Generator[None, None, None]:
    yield
    monkeypatch.setattr(
        "cpapacket.deliverables.general_ledger.fetch_general_ledger_monthly_slices",
        _ORIGINAL_FETCH,
    )
    monkeypatch.setattr(
        "cpapacket.deliverables.general_ledger.merge_general_ledger_monthly_slices",
        _ORIGINAL_MERGE,
    )


def test_general_ledger_deliverable_writes_csv_metadata_and_raw(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "cpapacket.deliverables.general_ledger.fetch_general_ledger_monthly_slices",
        lambda *args, **kwargs: _fake_slices(),
    )
    normalized_rows = (
        _row("txn-1", "DOC-1"),
        _row("txn-2", "DOC-2", debit=Decimal("0.00"), credit=Decimal("100.00")),
    )
    monkeypatch.setattr(
        "cpapacket.deliverables.general_ledger.merge_general_ledger_monthly_slices",
        lambda slices, *, normalizer=None: normalized_rows,
    )

    deliverable = GeneralLedgerDeliverable()
    result = deliverable.generate(_ctx(tmp_path), _StubProvider(), prompts={})

    assert result.success
    assert result.deliverable_key == "general_ledger"
    csv_path = tmp_path / "03_Full-Year_General_Ledger" / "cpa" / "General_Ledger_2025.csv"
    raw_path = tmp_path / "03_Full-Year_General_Ledger" / "dev" / "General_Ledger_2025_raw.json"
    meta_path = tmp_path / "_meta" / "general_ledger_metadata.json"
    assert csv_path.exists()
    lines = csv_path.read_text(encoding="utf-8").splitlines()
    assert lines[0].startswith("txn_id,date,transaction_type,document_number,account_name")
    assert "txn-1" in lines[1]
    assert raw_path.exists()
    raw_payload = json.loads(raw_path.read_text(encoding="utf-8"))
    assert raw_payload["year"] == 2025
    assert raw_payload["slices"][0]["month"] == 1
    assert meta_path.exists()
    metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    assert metadata["deliverable"] == "general_ledger"
    assert str(csv_path) in metadata["artifacts"]
    assert str(raw_path) in metadata["artifacts"]
    assert result.warnings == []


def test_general_ledger_deliverable_warns_when_signed_amounts_out_of_tolerance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "cpapacket.deliverables.general_ledger.fetch_general_ledger_monthly_slices",
        lambda *args, **kwargs: _fake_slices(),
    )
    normalized_rows = (_row("txn-1", "DOC-1"), _row("txn-2", "DOC-2"))
    monkeypatch.setattr(
        "cpapacket.deliverables.general_ledger.merge_general_ledger_monthly_slices",
        lambda slices, *, normalizer=None: normalized_rows,
    )

    deliverable = GeneralLedgerDeliverable()
    result = deliverable.generate(_ctx(tmp_path), _StubProvider(), prompts={})

    assert result.success
    assert any(
        "General ledger signed amounts do not balance" in warning for warning in result.warnings
    )


def test_general_ledger_deliverable_warns_when_no_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "cpapacket.deliverables.general_ledger.fetch_general_ledger_monthly_slices",
        lambda *args, **kwargs: _fake_slices(),
    )
    monkeypatch.setattr(
        "cpapacket.deliverables.general_ledger.merge_general_ledger_monthly_slices",
        lambda *args, **kwargs: (),
    )

    deliverable = GeneralLedgerDeliverable()
    result = deliverable.generate(_ctx(tmp_path), _StubProvider(), prompts={})

    assert result.success
    assert result.warnings == ["General ledger normalized to zero rows."]
    csv_path = tmp_path / "03_Full-Year_General_Ledger" / "cpa" / "General_Ledger_2025.csv"
    assert csv_path.exists()
    lines = csv_path.read_text(encoding="utf-8").splitlines()
    assert lines == [
        "txn_id,date,transaction_type,document_number,account_name,account_type,payee,memo,debit,credit,signed_amount"
    ]


def test_general_ledger_deliverable_does_not_warn_when_signed_amounts_balance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "cpapacket.deliverables.general_ledger.fetch_general_ledger_monthly_slices",
        lambda *args, **kwargs: _fake_slices(),
    )
    balanced_rows = (
        _row("txn-1", "DOC-1", debit=Decimal("100.00"), credit=Decimal("0.00")),
        _row("txn-2", "DOC-2", debit=Decimal("0.00"), credit=Decimal("100.00")),
    )
    monkeypatch.setattr(
        "cpapacket.deliverables.general_ledger.merge_general_ledger_monthly_slices",
        lambda slices, *, normalizer=None: balanced_rows,
    )

    deliverable = GeneralLedgerDeliverable()
    result = deliverable.generate(_ctx(tmp_path), _StubProvider(), prompts={})

    assert result.success
    assert result.warnings == []
