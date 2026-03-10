from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

from cpapacket.models.distributions import MiscodedDistributionCandidate
from cpapacket.models.general_ledger import GeneralLedgerRow
from cpapacket.reconciliation.retained_earnings import (
    RetainedEarningsSourceData,
    build_retained_earnings_rollforward,
    evaluate_re_structural_flags,
    extract_distribution_total,
    extract_net_income_from_pnl_report,
    extract_retained_earnings_from_balance_sheet,
    integrate_miscoded_distributions,
    load_re_source_data,
)


class StubDetector:
    def __init__(self, candidates: list[MiscodedDistributionCandidate]) -> None:
        self._candidates = candidates

    def scan(
        self,
        gl_rows: list[GeneralLedgerRow],
        owner_keywords: list[str],
    ) -> list[MiscodedDistributionCandidate]:
        assert isinstance(gl_rows, list)
        assert isinstance(owner_keywords, list)
        return self._candidates


def _candidate(txn_id: str = "TXN-1") -> MiscodedDistributionCandidate:
    return MiscodedDistributionCandidate(
        txn_id=txn_id,
        date=date(2025, 1, 1),
        transaction_type="Transfer",
        payee="Owner",
        memo="owner draw",
        account="Office Expense",
        amount=Decimal("1200.00"),
        reason_codes=["R1_OWNER_PAYEE_EXPENSE", "R5_HIGH_AMOUNT"],
        confidence="Medium",
        score=4,
    )


def _gl_row() -> GeneralLedgerRow:
    return GeneralLedgerRow(
        txn_id="GL-1",
        date=date(2025, 1, 1),
        transaction_type="Transfer",
        document_number="DOC-1",
        account_name="Office Expense",
        account_type="Expense",
        payee="Owner",
        memo="owner draw",
        debit=Decimal("1200"),
        credit=Decimal("0"),
    )


def test_integrate_miscoded_distributions_writes_csv_when_missing(tmp_path: Path) -> None:
    detector = StubDetector([_candidate()])
    result = integrate_miscoded_distributions(
        gl_rows=[_gl_row()],
        owner_keywords=["owner"],
        packet_root=tmp_path,
        year=2025,
        detector=detector,
    )

    assert result.wrote_csv is True
    assert result.csv_path.exists()
    contents = result.csv_path.read_text(encoding="utf-8")
    assert "txn_id" in contents
    assert "TXN-1" in contents


def test_integrate_miscoded_distributions_reuses_existing_csv(tmp_path: Path) -> None:
    detector = StubDetector([_candidate("TXN-2")])

    first = integrate_miscoded_distributions(
        gl_rows=[_gl_row()],
        owner_keywords=["owner"],
        packet_root=tmp_path,
        year=2025,
        detector=detector,
    )
    first_contents = first.csv_path.read_text(encoding="utf-8")

    second = integrate_miscoded_distributions(
        gl_rows=[_gl_row()],
        owner_keywords=["owner"],
        packet_root=tmp_path,
        year=2025,
        detector=detector,
    )

    assert second.wrote_csv is False
    assert second.csv_path == first.csv_path
    assert second.csv_path.read_text(encoding="utf-8") == first_contents


def test_evaluate_re_structural_flags_all_conditions() -> None:
    gl_rows = [
        GeneralLedgerRow(
            txn_id="GL-RE",
            date=date(2025, 2, 1),
            transaction_type="Journal",
            document_number="DOC-RE",
            account_name="Retained Earnings",
            account_type="Equity",
            payee=None,
            memo="Year-end adjustment",
            debit=Decimal("0"),
            credit=Decimal("10"),
        )
    ]

    flags = evaluate_re_structural_flags(
        net_income=Decimal("100"),
        distributions=Decimal("150"),
        actual_ending_re=Decimal("-1"),
        gl_rows=gl_rows,
    )

    assert "basis_risk_distributions_exceed_net_income" in flags
    assert "negative_ending_retained_earnings" in flags
    assert "direct_retained_earnings_postings_detected" in flags


def test_evaluate_re_structural_flags_clean_case() -> None:
    flags = evaluate_re_structural_flags(
        net_income=Decimal("150"),
        distributions=Decimal("100"),
        actual_ending_re=Decimal("200"),
        gl_rows=[_gl_row()],
    )

    assert flags == []


def test_extract_distribution_total_from_equity_distribution_rows() -> None:
    rows = [
        GeneralLedgerRow(
            txn_id="D1",
            date=date(2025, 3, 1),
            transaction_type="Check",
            document_number="D1",
            account_name="Shareholder Distributions",
            account_type="Equity",
            payee="Owner",
            memo="draw",
            debit=Decimal("1000"),
            credit=Decimal("0"),
        ),
        GeneralLedgerRow(
            txn_id="D2",
            date=date(2025, 3, 2),
            transaction_type="Journal",
            document_number="D2",
            account_name="Owner Draw",
            account_type="Equity",
            payee="Owner",
            memo="adjustment",
            debit=Decimal("200"),
            credit=Decimal("0"),
        ),
        GeneralLedgerRow(
            txn_id="E1",
            date=date(2025, 3, 3),
            transaction_type="Expense",
            document_number="E1",
            account_name="Meals Expense",
            account_type="Expense",
            payee="Vendor",
            memo="lunch",
            debit=Decimal("999"),
            credit=Decimal("0"),
        ),
    ]

    assert extract_distribution_total(rows) == Decimal("1200.00")


def test_extract_distribution_total_detects_dividend_and_memo_signals() -> None:
    rows = [
        GeneralLedgerRow(
            txn_id="DIV-1",
            date=date(2025, 5, 1),
            transaction_type="Journal",
            document_number="DIV-1",
            account_name="Owner's Equity",
            account_type="Equity",
            payee="Owner",
            memo="q2 dividend payout",
            debit=Decimal("500"),
            credit=Decimal("0"),
        ),
        GeneralLedgerRow(
            txn_id="DIV-2",
            date=date(2025, 5, 2),
            transaction_type="Journal",
            document_number="DIV-2",
            account_name="Shareholder Distributions",
            account_type="Equity",
            payee="Owner",
            memo="distribution",
            debit=Decimal("300"),
            credit=Decimal("0"),
        ),
    ]

    assert extract_distribution_total(rows) == Decimal("800.00")


def test_extract_distribution_total_excludes_non_distribution_equity_accounts() -> None:
    rows = [
        GeneralLedgerRow(
            txn_id="EQ-1",
            date=date(2025, 6, 1),
            transaction_type="Journal",
            document_number="EQ-1",
            account_name="Retained Earnings",
            account_type="Equity",
            payee=None,
            memo="retained earnings rollforward",
            debit=Decimal("400"),
            credit=Decimal("0"),
        ),
        GeneralLedgerRow(
            txn_id="EQ-2",
            date=date(2025, 6, 2),
            transaction_type="Journal",
            document_number="EQ-2",
            account_name="Common Stock",
            account_type="Equity",
            payee=None,
            memo="share issuance",
            debit=Decimal("250"),
            credit=Decimal("0"),
        ),
    ]

    assert extract_distribution_total(rows) == Decimal("0.00")


def test_extract_net_income_from_pnl_report_handles_income_and_loss() -> None:
    income_payload: dict[str, object] = {
        "Rows": {
            "Row": [
                {
                    "Summary": {
                        "ColData": [
                            {"value": "Net Income"},
                            {"value": "1,234.50"},
                        ]
                    }
                }
            ]
        }
    }
    loss_payload: dict[str, object] = {
        "Rows": {
            "Row": [
                {
                    "Summary": {
                        "ColData": [
                            {"value": "Net Loss"},
                            {"value": "(321.00)"},
                        ]
                    }
                }
            ]
        }
    }

    assert extract_net_income_from_pnl_report(income_payload) == Decimal("1234.50")
    assert extract_net_income_from_pnl_report(loss_payload) == Decimal("-321.00")


def test_extract_net_income_from_pnl_report_defaults_to_zero_when_missing() -> None:
    assert extract_net_income_from_pnl_report({}) == Decimal("0.00")
    assert extract_net_income_from_pnl_report({"Rows": {"Row": []}}) == Decimal("0.00")


def test_extract_retained_earnings_from_balance_sheet_payload() -> None:
    payload: dict[str, object] = {
        "Rows": {
            "Row": [
                {
                    "Header": {"ColData": [{"value": "Equity"}]},
                    "Rows": {
                        "Row": [
                            {
                                "ColData": [
                                    {"value": "Common Stock"},
                                    {"value": "100.00"},
                                ]
                            },
                            {
                                "ColData": [
                                    {"value": "Retained Earnings"},
                                    {"value": "5,432.10"},
                                ]
                            },
                        ]
                    },
                }
            ]
        }
    }

    assert extract_retained_earnings_from_balance_sheet(payload) == Decimal("5432.10")


def test_extract_retained_earnings_from_balance_sheet_defaults_zero() -> None:
    assert extract_retained_earnings_from_balance_sheet({}) == Decimal("0.00")
    assert (
        extract_retained_earnings_from_balance_sheet(
            {
                "Rows": {
                    "Row": [
                        {
                            "ColData": [
                                {"value": "Common Stock"},
                                {"value": "100.00"},
                            ]
                        }
                    ]
                }
            }
        )
        == Decimal("0.00")
    )


class _ReProvider:
    def __init__(self) -> None:
        self.balance_sheet_calls: list[tuple[int, str]] = []
        self.pnl_calls: list[tuple[int, str]] = []
        self.gl_calls: list[tuple[int, int]] = []

    def get_balance_sheet(self, year: int, as_of: date | str) -> dict[str, Any]:
        as_of_text = as_of.isoformat() if isinstance(as_of, date) else as_of
        self.balance_sheet_calls.append((year, as_of_text))
        value = "100.00" if year == 2024 else "500.00"
        return {
            "Rows": {
                "Row": [
                    {
                        "ColData": [
                            {"value": "Retained Earnings"},
                            {"value": value},
                        ]
                    }
                ]
            }
        }

    def get_pnl(self, year: int, method: str) -> dict[str, Any]:
        self.pnl_calls.append((year, method))
        return {
            "Rows": {
                "Row": [
                    {
                        "Summary": {
                            "ColData": [
                                {"value": "Net Income"},
                                {"value": "250.00"},
                            ]
                        }
                    }
                ]
            }
        }

    def get_general_ledger(self, year: int, month: int) -> dict[str, Any]:
        self.gl_calls.append((year, month))
        if month == 1:
            rows = [
                {
                    "TxnId": "DIST-1",
                    "TxnDate": "2025-01-05",
                    "TxnType": "Journal",
                    "DocNum": "DOC-1",
                    "AccountName": "Shareholder Distributions",
                    "AccountType": "Equity",
                    "Payee": "Owner",
                    "Memo": "distribution",
                    "Amount": "125.00",
                }
            ]
        else:
            rows = []
        return {"Rows": {"Row": rows}}


def test_load_re_source_data_uses_provider_layer_and_calculates_fields() -> None:
    provider = _ReProvider()

    data = load_re_source_data(year=2025, provider=provider)

    assert data.beginning_retained_earnings == Decimal("100.00")
    assert data.net_income == Decimal("250.00")
    assert data.distributions == Decimal("125.00")
    assert data.actual_ending_retained_earnings == Decimal("500.00")
    assert len(data.gl_rows) == 1
    assert provider.balance_sheet_calls == [
        (2024, "2024-12-31"),
        (2025, "2025-12-31"),
    ]
    assert provider.pnl_calls == [(2025, "accrual")]
    assert provider.gl_calls == [(2025, month) for month in range(1, 13)]


def _sample_source_data(
    *,
    beginning: Decimal = Decimal("100.00"),
    net_income: Decimal = Decimal("50.00"),
    distributions: Decimal = Decimal("10.00"),
    actual: Decimal = Decimal("139.99"),
) -> RetainedEarningsSourceData:
    return RetainedEarningsSourceData(
        beginning_retained_earnings=beginning,
        net_income=net_income,
        distributions=distributions,
        actual_ending_retained_earnings=actual,
        gl_rows=[],
    )


def test_build_retained_earnings_rollforward_balanced_status() -> None:
    source = _sample_source_data(actual=Decimal("139.99"))
    result = build_retained_earnings_rollforward(
        source=source,
        structural_flags=["basis_risk_distributions_exceed_net_income"],
    )

    assert result.expected_ending_re == Decimal("140.00")
    assert result.difference == Decimal("0.01")
    assert result.status == "Balanced"
    assert result.flags == ["basis_risk_distributions_exceed_net_income"]


def test_build_retained_earnings_rollforward_mismatch_status() -> None:
    source = _sample_source_data(actual=Decimal("140.10"))
    result = build_retained_earnings_rollforward(source=source, structural_flags=[])

    assert result.expected_ending_re == Decimal("140.00")
    assert result.difference == Decimal("-0.10")
    assert result.status == "Mismatch"
    assert result.flags == []
