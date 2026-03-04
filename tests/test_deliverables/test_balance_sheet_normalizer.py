from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

from cpapacket.deliverables.balance_sheet import (
    normalize_balance_sheet_rows,
    validate_balance_equation,
)


def test_normalize_balance_sheet_rows_handles_real_fixture() -> None:
    payload = json.loads(Path("tests/fixtures/qbo/balance_sheet_2025.json").read_text("utf-8"))

    rows = normalize_balance_sheet_rows(payload)

    assert rows, "fixture should flatten to at least one row"
    assert rows[0].section == "Assets"
    assert any(row.section == "Liabilities" for row in rows)
    assert any(row.section == "Equity" for row in rows)
    assert any(row.label == "Total Assets" and row.row_type == "total" for row in rows)
    assert any(row.label == "Checking" and row.path == "Assets > Checking" for row in rows)


def test_normalize_balance_sheet_rows_keeps_parent_section_for_nested_headers() -> None:
    payload = {
        "Rows": {
            "Row": [
                {
                    "Header": {"ColData": [{"value": "Assets"}]},
                    "Rows": {
                        "Row": [
                            {
                                "Header": {"ColData": [{"value": "Current Assets"}]},
                                "Rows": {
                                    "Row": [
                                        {
                                            "ColData": [
                                                {"value": "Checking"},
                                                {"value": "1200.00"},
                                            ]
                                        }
                                    ]
                                },
                                "Summary": {
                                    "ColData": [
                                        {"value": "Current Assets subtotal"},
                                        {"value": "1200.00"},
                                    ]
                                },
                            }
                        ]
                    },
                    "Summary": {"ColData": [{"value": "Total Assets"}, {"value": "1200.00"}]},
                }
            ]
        }
    }

    rows = normalize_balance_sheet_rows(payload)
    current_assets = next(row for row in rows if row.label == "Current Assets")
    checking = next(row for row in rows if row.label == "Checking")
    subtotal = next(row for row in rows if row.label == "Current Assets subtotal")

    assert current_assets.section == "Assets"
    assert current_assets.level == 1
    assert checking.section == "Assets"
    assert checking.level == 2
    assert checking.path == "Assets > Current Assets > Checking"
    assert checking.amount == Decimal("1200.00")
    assert subtotal.row_type == "subtotal"


def test_normalize_balance_sheet_rows_rejects_non_standard_top_level_section() -> None:
    payload = {
        "Rows": {
            "Row": [
                {
                    "Header": {"ColData": [{"value": "Income"}]},
                    "Rows": {
                        "Row": [
                            {"ColData": [{"value": "Sales"}, {"value": "10.00"}]},
                        ]
                    },
                }
            ]
        }
    }

    with pytest.raises(ValueError, match="Unsupported balance sheet section 'Income'"):
        normalize_balance_sheet_rows(payload)


def test_normalize_balance_sheet_rows_handles_empty_or_invalid_shapes() -> None:
    assert normalize_balance_sheet_rows({}) == []
    assert normalize_balance_sheet_rows({"Rows": {"Row": "not-a-list"}}) == []


def test_validate_balance_equation_balanced_with_totals() -> None:
    payload = json.loads(Path("tests/fixtures/qbo/balance_sheet_2025.json").read_text("utf-8"))
    rows = normalize_balance_sheet_rows(payload)

    check = validate_balance_equation(rows)

    assert check.balanced is True
    assert check.assets == Decimal("150000.00")
    assert check.liabilities == Decimal("60000.00")
    assert check.equity == Decimal("90000.00")
    assert check.difference == Decimal("0.00")
    assert check.warning is None


def test_validate_balance_equation_warns_when_difference_exceeds_tolerance() -> None:
    payload = {
        "Rows": {
            "Row": [
                {
                    "Header": {"ColData": [{"value": "Assets"}]},
                    "Rows": {"Row": [{"ColData": [{"value": "Cash"}, {"value": "100.00"}]}]},
                    "Summary": {"ColData": [{"value": "Total Assets"}, {"value": "100.00"}]},
                },
                {
                    "Header": {"ColData": [{"value": "Liabilities"}]},
                    "Rows": {"Row": [{"ColData": [{"value": "Debt"}, {"value": "40.00"}]}]},
                    "Summary": {"ColData": [{"value": "Total Liabilities"}, {"value": "40.00"}]},
                },
                {
                    "Header": {"ColData": [{"value": "Equity"}]},
                    "Rows": {"Row": [{"ColData": [{"value": "Equity"}, {"value": "59.98"}]}]},
                    "Summary": {"ColData": [{"value": "Total Equity"}, {"value": "59.98"}]},
                },
            ]
        }
    }
    rows = normalize_balance_sheet_rows(payload)

    check = validate_balance_equation(rows)

    assert check.balanced is False
    assert check.difference == Decimal("0.02")
    assert check.warning is not None


def test_validate_balance_equation_allows_tolerance_boundary() -> None:
    payload = {
        "Rows": {
            "Row": [
                {
                    "Header": {"ColData": [{"value": "Assets"}]},
                    "Rows": {"Row": [{"ColData": [{"value": "Cash"}, {"value": "100.01"}]}]},
                    "Summary": {"ColData": [{"value": "Total Assets"}, {"value": "100.01"}]},
                },
                {
                    "Header": {"ColData": [{"value": "Liabilities"}]},
                    "Rows": {"Row": [{"ColData": [{"value": "Debt"}, {"value": "40.00"}]}]},
                    "Summary": {"ColData": [{"value": "Total Liabilities"}, {"value": "40.00"}]},
                },
                {
                    "Header": {"ColData": [{"value": "Equity"}]},
                    "Rows": {"Row": [{"ColData": [{"value": "Equity"}, {"value": "60.00"}]}]},
                    "Summary": {"ColData": [{"value": "Total Equity"}, {"value": "60.00"}]},
                },
            ]
        }
    }
    rows = normalize_balance_sheet_rows(payload)

    check = validate_balance_equation(rows)

    assert check.balanced is True
    assert check.difference == Decimal("0.01")
    assert check.warning is None
