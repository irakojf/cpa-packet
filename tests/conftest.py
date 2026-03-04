"""Shared pytest fixtures for cpapacket tests."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pytest

from cpapacket.core.context import RunContext
from cpapacket.data.store import SessionDataStore
from cpapacket.models.general_ledger import GeneralLedgerRow
from cpapacket.models.normalized import NormalizedRow


@pytest.fixture
def packet_dir(tmp_path: Path) -> Path:
    """Create a packet output directory used by tests."""
    path = tmp_path / "Acme_2025_CPA_Packet"
    path.mkdir(parents=True, exist_ok=True)
    return path


@pytest.fixture
def packet_meta_cache_dir(packet_dir: Path) -> Path:
    """Return standard cache directory path under packet metadata tree."""
    path = packet_dir / "_meta" / "private" / "cache"
    path.mkdir(parents=True, exist_ok=True)
    return path


@pytest.fixture
def run_context_factory(packet_dir: Path) -> Callable[..., RunContext]:
    """Factory to build a RunContext with sensible defaults for tests."""

    def _factory(**overrides: object) -> RunContext:
        defaults: dict[str, object] = {
            "year": 2025,
            "year_source": "explicit",
            "out_dir": packet_dir,
            "method": "accrual",
            "non_interactive": True,
            "on_conflict": "abort",
            "incremental": False,
            "force": False,
            "no_cache": False,
            "no_raw": False,
            "redact": False,
            "include_debug": False,
            "verbose": False,
            "quiet": False,
            "plain": False,
            "skip": [],
            "owner_keywords": [],
            "gusto_available": True,
        }
        defaults.update(overrides)
        return RunContext(**cast(dict[str, Any], defaults))

    return _factory


@pytest.fixture
def mock_run_context(run_context_factory: Callable[..., RunContext]) -> RunContext:
    """A default immutable RunContext for tests."""
    return run_context_factory()


@pytest.fixture
def mock_session_data_store(packet_meta_cache_dir: Path) -> SessionDataStore:
    """SessionDataStore configured with a real on-disk cache path."""
    return SessionDataStore(cache_dir=packet_meta_cache_dir)


@pytest.fixture
def sample_normalized_rows() -> list[NormalizedRow]:
    """Canonical normalized rows for deliverable/reconciliation tests."""
    return [
        NormalizedRow(
            section="Income",
            label="Consulting Revenue",
            amount=Decimal("10000.00"),
            row_type="account",
            level=1,
            path="Income > Consulting Revenue",
        ),
        NormalizedRow(
            section="Expenses",
            label="Payroll Expense",
            amount=Decimal("2500.00"),
            row_type="account",
            level=1,
            path="Expenses > Payroll Expense",
        ),
        NormalizedRow(
            section="Net Income",
            label="Net Income",
            amount=Decimal("7500.00"),
            row_type="total",
            level=0,
            path="Net Income",
        ),
    ]


@pytest.fixture
def sample_general_ledger_rows() -> list[GeneralLedgerRow]:
    """Canonical GL rows used by reconciliation and detector tests."""
    return [
        GeneralLedgerRow(
            txn_id="txn-001",
            date=date(2025, 1, 15),
            transaction_type="Journal Entry",
            document_number="JE-001",
            account_name="Owner Distributions",
            account_type="Equity",
            payee="Owner A",
            memo="Quarterly distribution",
            debit=Decimal("0.00"),
            credit=Decimal("1500.00"),
        ),
        GeneralLedgerRow(
            txn_id="txn-002",
            date=date(2025, 2, 10),
            transaction_type="Check",
            document_number="1001",
            account_name="Payroll Expense",
            account_type="Expense",
            payee="Payroll Provider",
            memo="Biweekly payroll",
            debit=Decimal("2500.00"),
            credit=Decimal("0.00"),
        ),
    ]
