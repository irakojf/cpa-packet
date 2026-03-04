from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

from cpapacket.models.distributions import MiscodedDistributionCandidate
from cpapacket.models.general_ledger import GeneralLedgerRow
from cpapacket.reconciliation.retained_earnings import (
    evaluate_re_structural_flags,
    extract_distribution_total,
    integrate_miscoded_distributions,
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
