"""Project-wide constants and schema declarations."""

from __future__ import annotations

from decimal import Decimal
from typing import Final

DELIVERABLE_FOLDERS: Final[dict[str, str]] = {
    "pnl": "01_Year-End_Profit_and_Loss",
    "balance_sheet": "02_Year-End_Balance_Sheet",
    "general_ledger": "03_Full-Year_General_Ledger",
    "payroll_summary": "04_Annual_Payroll_Summary",
    "officer_w2": "05_Officer_W2_Equivalent",
    "distributions": "06_Shareholder_Distributions",
    "contractor": "07_Contractor_1099_Summary",
    "estimated_tax": "08_Estimated_Tax_Payments",
    "retained_earnings": "09_Retained_Earnings_Rollforward",
    "payroll_recon": "10_Payroll_Reconciliation",
    "meta": "_meta",
}

BALANCE_EQUATION_TOLERANCE: Final[Decimal] = Decimal("0.01")
PAYROLL_RECON_TOLERANCE: Final[Decimal] = Decimal("0.01")
RETAINED_EARNINGS_TOLERANCE: Final[Decimal] = Decimal("0.01")
CONTRACTOR_1099_THRESHOLD: Final[Decimal] = Decimal("600.00")

MISCODE_HIGH_AMOUNT_THRESHOLD: Final[Decimal] = Decimal("1000.00")
MISCODE_ROUND_NUMBER_DIVISOR: Final[int] = 100
MISCODE_CONFIDENCE_HIGH: Final[int] = 6
MISCODE_CONFIDENCE_MEDIUM: Final[int] = 4
MISCODE_CONFIDENCE_LOW: Final[int] = 2

RETRY_MAX_429: Final[int] = 5
RETRY_MAX_5XX: Final[int] = 3
QBO_MAX_CONCURRENCY: Final[int] = 8
GUSTO_MAX_CONCURRENCY: Final[int] = 3
CACHE_TTL_HOURS: Final[int] = 24

SCHEMA_VERSIONS: Final[dict[str, dict[str, str]]] = {
    "pnl": {"csv": "1.0"},
    "balance_sheet": {"csv": "1.0"},
    "general_ledger": {"csv": "1.0"},
    "payroll_summary": {"csv": "1.0"},
    "contractor": {"csv": "2.0"},
    "estimated_tax": {"csv": "1.0"},
    "payroll_recon": {"csv": "1.0"},
    "retained_earnings": {"csv": "3.0"},
    "distributions": {"csv": "2.0"},
    "review_dashboard": {"md": "1.0", "pdf": "1.0"},
}
