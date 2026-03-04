from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

from cpapacket.models.distributions import MiscodedDistributionCandidate
from cpapacket.models.general_ledger import GeneralLedgerRow
from cpapacket.reconciliation.retained_earnings import integrate_miscoded_distributions


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
