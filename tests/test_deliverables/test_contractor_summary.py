"""Tests for contractor summary helper functions."""

from __future__ import annotations

import csv
import json
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from cpapacket.core.context import RunContext
from cpapacket.deliverables.contractor_summary import (
    ContractorSummaryDeliverable,
    _prompt_selected_accounts,
    build_contractor_records,
    detect_contractor_accounts,
    should_flag_for_1099_review,
)
from cpapacket.models.general_ledger import GeneralLedgerRow


class _FakeProviders:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload
        self.calls = 0

    def get_accounts(self) -> dict[str, Any]:
        self.calls += 1
        return self._payload


class _ContractorProvider(_FakeProviders):
    def __init__(
        self,
        payload: dict[str, Any],
        *,
        monthly_rows: dict[int, list[dict[str, Any]]] | None = None,
    ) -> None:
        super().__init__(payload)
        self._monthly_rows = monthly_rows or {}
        self.gl_calls: list[tuple[int, int]] = []

    def get_general_ledger(self, year: int, month: int) -> dict[str, Any]:
        self.gl_calls.append((year, month))
        return {"Rows": {"Row": self._monthly_rows.get(month, [])}}


class _PromptCtx:
    def __init__(self, provider: _FakeProviders, *, non_interactive: bool) -> None:
        self.store = provider
        self.non_interactive = non_interactive


def _ctx(tmp_path: Path, *, no_raw: bool = False) -> RunContext:
    return RunContext(
        year=2025,
        year_source="explicit",
        out_dir=tmp_path,
        method="accrual",
        non_interactive=True,
        on_conflict="abort",
        no_raw=no_raw,
    )


def test_should_flag_for_1099_review_at_threshold() -> None:
    assert should_flag_for_1099_review(non_card_total=Decimal("600.00")) is True


def test_should_flag_for_1099_review_below_threshold() -> None:
    assert should_flag_for_1099_review(non_card_total=Decimal("599.99")) is False


def test_should_flag_for_1099_review_card_only_vendor_not_flagged() -> None:
    assert should_flag_for_1099_review(non_card_total=Decimal("0.00")) is False


def test_detect_contractor_accounts_filters_expense_and_cogs() -> None:
    providers = _FakeProviders(
        {
            "QueryResponse": {
                "Account": [
                    {"Id": "1", "Name": "Contract Labor", "AccountType": "Expense"},
                    {"Id": "2", "Name": "Subcontractors", "AccountType": "Cost of Goods Sold"},
                    {"Id": "3", "Name": "Office Expense", "AccountType": "Expense"},
                    {"Id": "4", "Name": "Contractor Income", "AccountType": "Income"},
                ]
            }
        }
    )

    detected = detect_contractor_accounts(providers=providers)

    assert detected == [
        {"id": "1", "name": "Contract Labor", "account_type": "Expense"},
        {"id": "2", "name": "Subcontractors", "account_type": "Cost of Goods Sold"},
    ]
    assert providers.calls == 1


def test_detect_contractor_accounts_accepts_root_account_list_shape() -> None:
    providers = _FakeProviders(
        {
            "Account": [
                {"Id": "10", "Name": "CONTRACTORS", "AccountType": "Expense"},
                {"Id": "11", "Name": "Stripe Fees", "AccountType": "Expense"},
            ]
        }
    )

    detected = detect_contractor_accounts(providers=providers)

    assert detected == [{"id": "10", "name": "CONTRACTORS", "account_type": "Expense"}]


def test_gather_prompts_auto_confirms_detected_accounts_in_non_interactive() -> None:
    provider = _FakeProviders(
        {
            "QueryResponse": {
                "Account": [
                    {"Id": "1", "Name": "Contract Labor", "AccountType": "Expense"},
                    {"Id": "2", "Name": "Subcontractors", "AccountType": "Cost of Goods Sold"},
                    {"Id": "3", "Name": "Office Expense", "AccountType": "Expense"},
                ]
            }
        }
    )
    deliverable = ContractorSummaryDeliverable()

    prompts = deliverable.gather_prompts(_PromptCtx(provider, non_interactive=True))

    assert prompts == {"selected_account_ids": ["1", "2"]}


def test_prompt_selected_accounts_allows_manual_selection() -> None:
    inputs = iter(["n", "2"])

    selected = _prompt_selected_accounts(
        detected_accounts=[
            {"id": "1", "name": "Contract Labor", "account_type": "Expense"},
            {"id": "2", "name": "Subcontractors", "account_type": "Cost of Goods Sold"},
        ],
        non_interactive=False,
        input_fn=lambda _prompt: next(inputs),
    )

    assert selected == [{"id": "2", "name": "Subcontractors", "account_type": "Cost of Goods Sold"}]


def test_build_contractor_records_flags_vendor_at_threshold() -> None:
    rows = [
        GeneralLedgerRow(
            txn_id="txn-1",
            date=date(2025, 1, 10),
            transaction_type="Expense",
            document_number="BILL-1",
            account_name="Contract Labor",
            account_type="Expense",
            payee="Alpha LLC",
            memo="ACH",
            debit=Decimal("600.00"),
            credit=Decimal("0.00"),
        )
    ]

    records = build_contractor_records(
        rows=rows,
        selected_account_names={"Contract Labor"},
    )

    assert len(records) == 1
    assert records[0].display_name == "Alpha LLC"
    assert records[0].non_card_total == Decimal("600.00")
    assert records[0].requires_1099_review is True


def test_build_contractor_records_aggregates_vendor_payments_and_methods() -> None:
    rows = [
        GeneralLedgerRow(
            txn_id="txn-1",
            date=date(2025, 1, 10),
            transaction_type="Expense",
            document_number="BILL-1",
            account_name="Contract Labor",
            account_type="Expense",
            payee="Alpha LLC",
            memo="ACH transfer",
            debit=Decimal("300.00"),
            credit=Decimal("0.00"),
        ),
        GeneralLedgerRow(
            txn_id="txn-2",
            date=date(2025, 1, 12),
            transaction_type="Expense",
            document_number="BILL-2",
            account_name="Contract Labor",
            account_type="Expense",
            payee="Alpha LLC",
            memo="Stripe card charge",
            debit=Decimal("200.00"),
            credit=Decimal("0.00"),
        ),
        GeneralLedgerRow(
            txn_id="txn-3",
            date=date(2025, 1, 15),
            transaction_type="JournalEntry",
            document_number="JE-1",
            account_name="Contract Labor",
            account_type="Expense",
            payee="Alpha LLC",
            memo="reclass",
            debit=Decimal("50.00"),
            credit=Decimal("0.00"),
        ),
        GeneralLedgerRow(
            txn_id="txn-4",
            date=date(2025, 1, 20),
            transaction_type="Expense",
            document_number="BILL-3",
            account_name="Contract Labor",
            account_type="Expense",
            payee="Beta Inc",
            memo="ACH transfer",
            debit=Decimal("125.00"),
            credit=Decimal("0.00"),
        ),
    ]

    records = build_contractor_records(
        rows=rows,
        selected_account_names={"Contract Labor"},
    )

    assert [record.display_name for record in records] == ["Alpha LLC", "Beta Inc"]
    alpha = records[0]
    beta = records[1]

    assert alpha.total_paid == Decimal("550.00")
    assert alpha.card_processor_total == Decimal("200.00")
    assert alpha.non_card_total == Decimal("350.00")
    assert beta.total_paid == Decimal("125.00")
    assert beta.card_processor_total == Decimal("0.00")
    assert beta.non_card_total == Decimal("125.00")


def test_build_contractor_records_detects_credit_card_account_type_as_card_payment() -> None:
    rows = [
        GeneralLedgerRow(
            txn_id="txn-1",
            date=date(2025, 2, 10),
            transaction_type="Expense",
            document_number="CC-1",
            account_name="Vendor Clearing",
            account_type="Credit Card",
            payee="Gamma LLC",
            memo="monthly settlement",
            debit=Decimal("410.00"),
            credit=Decimal("0.00"),
        )
    ]

    records = build_contractor_records(
        rows=rows,
        selected_account_names={"Vendor Clearing"},
    )

    assert len(records) == 1
    assert records[0].total_paid == Decimal("410.00")
    assert records[0].card_processor_total == Decimal("410.00")
    assert records[0].non_card_total == Decimal("0.00")


def test_build_contractor_records_card_only_vendor_not_flagged() -> None:
    rows = [
        GeneralLedgerRow(
            txn_id="txn-1",
            date=date(2025, 2, 10),
            transaction_type="Expense",
            document_number="CC-1",
            account_name="Contract Labor",
            account_type="Expense",
            payee="Card-Only Vendor",
            memo="Stripe charge",
            debit=Decimal("750.00"),
            credit=Decimal("0.00"),
        )
    ]

    records = build_contractor_records(
        rows=rows,
        selected_account_names={"Contract Labor"},
    )

    assert len(records) == 1
    assert records[0].display_name == "Card-Only Vendor"
    assert records[0].card_processor_total == Decimal("750.00")
    assert records[0].non_card_total == Decimal("0.00")
    assert records[0].requires_1099_review is False


def test_build_contractor_records_includes_vendor_linked_journal_entries() -> None:
    rows = [
        GeneralLedgerRow(
            txn_id="txn-1",
            date=date(2025, 3, 1),
            transaction_type="JournalEntry",
            document_number="JE-1",
            account_name="Contract Labor",
            account_type="Expense",
            payee="Linked JE Vendor",
            memo="year-end adjustment",
            debit=Decimal("210.00"),
            credit=Decimal("0.00"),
        )
    ]

    records = build_contractor_records(
        rows=rows,
        selected_account_names={"Contract Labor"},
    )

    assert len(records) == 1
    assert records[0].display_name == "Linked JE Vendor"
    assert records[0].total_paid == Decimal("210.00")


def test_contractor_summary_deliverable_generates_outputs_and_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_write_report(self, output_path, **kwargs):  # type: ignore[no-untyped-def]
        del self, kwargs
        path = Path(output_path)
        path.write_bytes(b"%PDF-1.4\n%stub\n")
        return path

    monkeypatch.setattr(
        "cpapacket.deliverables.contractor_summary.PdfWriter.write_report",
        fake_write_report,
    )

    provider = _ContractorProvider(
        {
            "QueryResponse": {
                "Account": [
                    {"Id": "acc-1", "Name": "Contract Labor", "AccountType": "Expense"},
                    {"Id": "acc-2", "Name": "Office Expense", "AccountType": "Expense"},
                ]
            }
        },
        monthly_rows={
            1: [
                {
                    "TxnId": "T-1",
                    "TxnDate": "2025-01-15",
                    "TxnType": "Expense",
                    "DocNum": "BILL-1",
                    "AccountName": "Contract Labor",
                    "AccountType": "Expense",
                    "Payee": "Alpha LLC",
                    "Memo": "ACH transfer",
                    "Debit": "500.00",
                    "Credit": "0.00",
                },
                {
                    "TxnId": "T-2",
                    "TxnDate": "2025-01-20",
                    "TxnType": "Expense",
                    "DocNum": "BILL-2",
                    "AccountName": "Office Expense",
                    "AccountType": "Expense",
                    "Payee": "Office Vendor",
                    "Memo": "office",
                    "Debit": "99.00",
                    "Credit": "0.00",
                },
            ],
            2: [
                {
                    "TxnId": "T-3",
                    "TxnDate": "2025-02-11",
                    "TxnType": "Expense",
                    "DocNum": "BILL-3",
                    "AccountName": "Contract Labor",
                    "AccountType": "Expense",
                    "Payee": "Alpha LLC",
                    "Memo": "Stripe card charge",
                    "Debit": "150.00",
                    "Credit": "0.00",
                }
            ],
            3: [
                {
                    "TxnId": "T-4",
                    "TxnDate": "2025-03-04",
                    "TxnType": "Expense",
                    "DocNum": "BILL-4",
                    "AccountName": "Contract Labor",
                    "AccountType": "Expense",
                    "Payee": "Alpha LLC",
                    "Memo": "ACH transfer",
                    "Debit": "200.00",
                    "Credit": "0.00",
                }
            ],
        },
    )
    deliverable = ContractorSummaryDeliverable()

    result = deliverable.generate(_ctx(tmp_path), provider, prompts={})

    assert result.success is True
    assert provider.calls == 1
    assert provider.gl_calls == [(2025, month) for month in range(1, 13)]
    assert result.warnings == []
    assert any(path.endswith("contractor_summary_2025.csv") for path in result.artifacts)
    assert any(path.endswith("flagged_for_review_2025.csv") for path in result.artifacts)
    assert any(path.endswith("contractor_summary_2025.pdf") for path in result.artifacts)
    assert any(path.endswith("contractor_summary_2025.json") for path in result.artifacts)
    assert any(path.endswith("contractor_metadata.json") for path in result.artifacts)

    summary_csv_path = next(
        Path(path)
        for path in result.artifacts
        if path.endswith("contractor_summary_2025.csv")
    )
    with summary_csv_path.open(newline="", encoding="utf-8") as handle:
        summary_rows = list(csv.DictReader(handle))
    assert len(summary_rows) == 1
    assert summary_rows[0]["display_name"] == "Alpha LLC"
    assert summary_rows[0]["total_paid"] == "850.00"
    assert summary_rows[0]["card_processor_total"] == "150.00"
    assert summary_rows[0]["non_card_total"] == "700.00"
    assert summary_rows[0]["requires_1099_review"] == "true"

    flagged_csv_path = next(
        Path(path)
        for path in result.artifacts
        if path.endswith("flagged_for_review_2025.csv")
    )
    with flagged_csv_path.open(newline="", encoding="utf-8") as handle:
        flagged_rows = list(csv.DictReader(handle))
    assert len(flagged_rows) == 1
    assert flagged_rows[0]["display_name"] == "Alpha LLC"
    assert flagged_rows[0]["requires_1099_review"] == "true"

    metadata_path = tmp_path / "_meta" / "contractor_metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert metadata["deliverable"] == "contractor"
    assert metadata["schema_versions"] == {"csv": "1.0"}
    assert metadata["inputs"]["selected_account_ids"] == ["acc-1"]
    assert "input_fingerprint" in metadata


def test_contractor_summary_deliverable_warns_when_no_accounts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_write_report(self, output_path, **kwargs):  # type: ignore[no-untyped-def]
        del self, kwargs
        path = Path(output_path)
        path.write_bytes(b"%PDF-1.4\n%stub\n")
        return path

    monkeypatch.setattr(
        "cpapacket.deliverables.contractor_summary.PdfWriter.write_report",
        fake_write_report,
    )

    provider = _ContractorProvider({"QueryResponse": {"Account": []}}, monthly_rows={})
    deliverable = ContractorSummaryDeliverable()

    result = deliverable.generate(_ctx(tmp_path, no_raw=True), provider, prompts={})

    assert result.success is True
    assert result.warnings == [
        "No contractor accounts detected; generated empty contractor summary."
    ]
    csv_path = next(
        Path(path)
        for path in result.artifacts
        if path.endswith("contractor_summary_2025.csv")
    )
    with csv_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows == []


def test_contractor_summary_deliverable_sets_reconciliation_mismatch_flag(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_write_report(self, output_path, **kwargs):  # type: ignore[no-untyped-def]
        del self, kwargs
        path = Path(output_path)
        path.write_bytes(b"%PDF-1.4\n%stub\n")
        return path

    monkeypatch.setattr(
        "cpapacket.deliverables.contractor_summary.PdfWriter.write_report",
        fake_write_report,
    )

    provider = _ContractorProvider(
        {
            "QueryResponse": {
                "Account": [
                    {"Id": "acc-1", "Name": "Contract Labor", "AccountType": "Expense"},
                ]
            }
        },
        monthly_rows={
            1: [
                {
                    "TxnId": "T-1",
                    "TxnDate": "2025-01-15",
                    "TxnType": "Expense",
                    "DocNum": "BILL-1",
                    "AccountName": "Contract Labor",
                    "AccountType": "Expense",
                    "Payee": "Alpha LLC",
                    "Memo": "ACH transfer",
                    "Debit": "600.00",
                    "Credit": "0.00",
                },
                {
                    "TxnId": "T-2",
                    "TxnDate": "2025-01-31",
                    "TxnType": "JournalEntry",
                    "DocNum": "JE-REV-1",
                    "AccountName": "Contract Labor",
                    "AccountType": "Expense",
                    "Payee": "Alpha LLC",
                    "Memo": "year-end reclass",
                    "Debit": "0.00",
                    "Credit": "100.00",
                },
            ]
        },
    )
    deliverable = ContractorSummaryDeliverable()

    result = deliverable.generate(_ctx(tmp_path), provider, prompts={})

    assert any("Contractor reconciliation mismatch:" in warning for warning in result.warnings)

    metadata_path = tmp_path / "_meta" / "contractor_metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert metadata["inputs"]["has_reconciliation_mismatch"] is True
    assert metadata["inputs"]["contractor_total_paid"] == "600.00"
    assert metadata["inputs"]["selected_account_total"] == "500.00"


def test_contractor_summary_pipeline_flags_only_threshold_vendors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_write_report(self, output_path, **kwargs):  # type: ignore[no-untyped-def]
        del self, kwargs
        path = Path(output_path)
        path.write_bytes(b"%PDF-1.4\n%stub\n")
        return path

    monkeypatch.setattr(
        "cpapacket.deliverables.contractor_summary.PdfWriter.write_report",
        fake_write_report,
    )

    provider = _ContractorProvider(
        {
            "QueryResponse": {
                "Account": [
                    {"Id": "acc-1", "Name": "Contract Labor", "AccountType": "Expense"},
                ]
            }
        },
        monthly_rows={
            1: [
                {
                    "TxnId": "A-1",
                    "TxnDate": "2025-01-15",
                    "TxnType": "Expense",
                    "DocNum": "BILL-A1",
                    "AccountName": "Contract Labor",
                    "AccountType": "Expense",
                    "Payee": "Alpha LLC",
                    "Memo": "ACH transfer",
                    "Debit": "700.00",
                    "Credit": "0.00",
                },
                {
                    "TxnId": "B-1",
                    "TxnDate": "2025-01-20",
                    "TxnType": "Expense",
                    "DocNum": "BILL-B1",
                    "AccountName": "Contract Labor",
                    "AccountType": "Expense",
                    "Payee": "Beta LLC",
                    "Memo": "ACH transfer",
                    "Debit": "550.00",
                    "Credit": "0.00",
                },
            ]
        },
    )
    deliverable = ContractorSummaryDeliverable()

    result = deliverable.generate(_ctx(tmp_path), provider, prompts={})

    summary_csv_path = next(
        Path(path)
        for path in result.artifacts
        if path.endswith("contractor_summary_2025.csv")
    )
    flagged_csv_path = next(
        Path(path)
        for path in result.artifacts
        if path.endswith("flagged_for_review_2025.csv")
    )
    with summary_csv_path.open(newline="", encoding="utf-8") as handle:
        summary_rows = list(csv.DictReader(handle))
    with flagged_csv_path.open(newline="", encoding="utf-8") as handle:
        flagged_rows = list(csv.DictReader(handle))

    assert len(summary_rows) == 2
    assert {row["display_name"] for row in summary_rows} == {"Alpha LLC", "Beta LLC"}
    assert len(flagged_rows) == 1
    assert flagged_rows[0]["display_name"] == "Alpha LLC"


def test_contractor_summary_csv_outputs_match_golden_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_write_report(self, output_path, **kwargs):  # type: ignore[no-untyped-def]
        del self, kwargs
        path = Path(output_path)
        path.write_bytes(b"%PDF-1.4\n%stub\n")
        return path

    monkeypatch.setattr(
        "cpapacket.deliverables.contractor_summary.PdfWriter.write_report",
        fake_write_report,
    )

    provider = _ContractorProvider(
        {
            "QueryResponse": {
                "Account": [
                    {"Id": "acc-1", "Name": "Contract Labor", "AccountType": "Expense"},
                ]
            }
        },
        monthly_rows={
            1: [
                {
                    "TxnId": "A-1",
                    "TxnDate": "2025-01-15",
                    "TxnType": "Expense",
                    "DocNum": "BILL-A1",
                    "AccountName": "Contract Labor",
                    "AccountType": "Expense",
                    "Payee": "Alpha LLC",
                    "Memo": "ACH transfer",
                    "Debit": "700.00",
                    "Credit": "0.00",
                },
                {
                    "TxnId": "B-1",
                    "TxnDate": "2025-01-20",
                    "TxnType": "Expense",
                    "DocNum": "BILL-B1",
                    "AccountName": "Contract Labor",
                    "AccountType": "Expense",
                    "Payee": "Beta LLC",
                    "Memo": "ACH transfer",
                    "Debit": "550.00",
                    "Credit": "0.00",
                },
            ]
        },
    )
    deliverable = ContractorSummaryDeliverable()
    result = deliverable.generate(_ctx(tmp_path), provider, prompts={})

    summary_csv = next(
        Path(path)
        for path in result.artifacts
        if path.endswith("contractor_summary_2025.csv")
    )
    flagged_csv = next(
        Path(path)
        for path in result.artifacts
        if path.endswith("flagged_for_review_2025.csv")
    )
    fixtures_dir = Path("tests/fixtures/qbo")
    expected_summary = (fixtures_dir / "contractor_summary_2025_golden.csv").read_text(
        encoding="utf-8"
    )
    expected_flagged = (fixtures_dir / "flagged_for_review_2025_golden.csv").read_text(
        encoding="utf-8"
    )

    assert summary_csv.read_text(encoding="utf-8") == expected_summary
    assert flagged_csv.read_text(encoding="utf-8") == expected_flagged
