from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

from cpapacket.deliverables.pnl import normalize_pnl_rows


def test_normalize_pnl_rows_nested_sections_and_summary_rows() -> None:
    payload = {
        "Rows": {
            "Row": [
                {
                    "Header": {"ColData": [{"value": "Income"}]},
                    "Rows": {
                        "Row": [
                            {
                                "ColData": [
                                    {"value": "Consulting Revenue"},
                                    {"value": "1234.56"},
                                ]
                            },
                            {
                                "Summary": {
                                    "ColData": [{"value": "Total Income"}, {"value": "1234.56"}]
                                }
                            },
                        ]
                    },
                    "Summary": {"ColData": [{"value": "Total Income"}, {"value": "1234.56"}]},
                },
                {
                    "Header": {"ColData": [{"value": "Cost of Goods Sold"}]},
                    "Rows": {
                        "Row": [
                            {
                                "ColData": [
                                    {"value": "Materials"},
                                    {"value": "(200.00)"},
                                ]
                            }
                        ]
                    },
                },
            ]
        }
    }

    rows = normalize_pnl_rows(payload)

    assert [row.row_type for row in rows] == [
        "header",
        "account",
        "total",
        "total",
        "header",
        "account",
    ]
    assert rows[0].section == "Income"
    assert rows[1].path == "Income > Consulting Revenue"
    assert rows[1].amount == Decimal("1234.56")
    assert rows[2].label == "Total Income"
    assert rows[4].section == "COGS"
    assert rows[5].amount == Decimal("-200.00")
    assert rows[5].path == "Cost of Goods Sold > Materials"


def test_normalize_pnl_rows_handles_empty_or_invalid_shapes() -> None:
    assert normalize_pnl_rows({}) == []
    assert normalize_pnl_rows({"Rows": {"Row": "not-a-list"}}) == []
    assert (
        normalize_pnl_rows({"Rows": {"Row": [None, 123, {"ColData": [{"value": "Only Label"}]}]}})
        != []
    )


def test_normalize_pnl_rows_handles_real_fixture() -> None:
    path = Path("tests/fixtures/qbo/profit_and_loss_annual.json")
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = normalize_pnl_rows(payload)

    assert rows, "fixture should flatten to at least one row"
    assert any(row.section == "Income" for row in rows)
    assert any(row.label == "Net Profit" for row in rows)


def test_normalize_pnl_rows_tracks_deep_levels_and_subtotal_classification() -> None:
    payload = {
        "Rows": {
            "Row": [
                {
                    "Header": {"ColData": [{"value": "Income"}]},
                    "Rows": {
                        "Row": [
                            {
                                "Header": {"ColData": [{"value": "Services"}]},
                                "Rows": {
                                    "Row": [
                                        {
                                            "ColData": [
                                                {"value": "Strategy Consulting"},
                                                {"value": "900.00"},
                                            ]
                                        },
                                        {
                                            "Summary": {
                                                "ColData": [
                                                    {"value": "Services subtotal"},
                                                    {"value": "900.00"},
                                                ]
                                            }
                                        },
                                    ]
                                },
                            }
                        ]
                    },
                },
                {
                    "Header": {"ColData": [{"value": "Expenses"}]},
                    "Rows": {
                        "Row": [
                            {
                                "ColData": [
                                    {"value": "Software"},
                                    {"value": "100.00"},
                                ]
                            }
                        ]
                    },
                    "Summary": {"ColData": [{"value": "Total Expenses"}, {"value": "100.00"}]},
                },
            ]
        }
    }

    rows = normalize_pnl_rows(payload)

    service_header = next(row for row in rows if row.label == "Services")
    consulting = next(row for row in rows if row.label == "Strategy Consulting")
    services_subtotal = next(row for row in rows if row.label == "Services subtotal")
    expenses_account = next(row for row in rows if row.label == "Software")
    expenses_total = next(row for row in rows if row.label == "Total Expenses")

    assert service_header.level == 1
    assert consulting.level == 2
    assert consulting.path == "Income > Services > Strategy Consulting"
    assert services_subtotal.row_type == "subtotal"
    assert services_subtotal.section == "Income"
    assert expenses_account.section == "Expenses"
    assert expenses_total.row_type == "total"


def test_normalize_pnl_rows_maps_other_expense_and_subtotal() -> None:
    payload = {
        "Rows": {
            "Row": [
                {
                    "Header": {"ColData": [{"value": "Other Expenses"}]},
                    "Rows": {"Row": [{"ColData": [{"value": "Bank Fees"}, {"value": "10.00"}]}]},
                    "Summary": {"ColData": [{"value": "Operating Margin"}, {"value": "90.00"}]},
                }
            ]
        }
    }

    rows = normalize_pnl_rows(payload)

    assert rows[0].section == "Other Expense"
    assert rows[1].section == "Other Expense"
    assert rows[2].row_type == "subtotal"
    assert rows[2].amount == Decimal("90.00")


def test_normalize_pnl_rows_uses_uncategorized_fallback_and_zero_amount() -> None:
    payload = {
        "Rows": {
            "Row": [
                {"ColData": [{"value": "Loose Line"}, {"value": "not-a-number"}]},
            ]
        }
    }

    rows = normalize_pnl_rows(payload)

    assert len(rows) == 1
    assert rows[0].section == "Uncategorized"
    assert rows[0].amount == Decimal("0")
