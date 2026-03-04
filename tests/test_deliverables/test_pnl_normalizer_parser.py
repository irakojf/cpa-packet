from decimal import Decimal

import pytest

from cpapacket.deliverables.pnl_normalizer import normalize_pnl_report


def test_normalize_pnl_report_walks_nested_rows_and_summary() -> None:
    payload = {
        "Rows": {
            "Row": [
                {
                    "Header": {"ColData": [{"value": "Income"}, {"value": "1500.00"}]},
                    "Rows": {
                        "Row": [
                            {"ColData": [{"value": "Consulting Revenue"}, {"value": "1000.00"}]},
                            {"ColData": [{"value": "Product Sales"}, {"value": "500.00"}]},
                        ]
                    },
                    "Summary": {"ColData": [{"value": "Total Income"}, {"value": "1500.00"}]},
                },
                {
                    "Header": {"ColData": [{"value": "Expenses"}, {"value": "200.00"}]},
                    "Rows": {"Row": [{"ColData": [{"value": "Software"}, {"value": "200.00"}]}]},
                    "Summary": {"ColData": [{"value": "Total Expenses"}, {"value": "200.00"}]},
                },
            ]
        }
    }

    rows = normalize_pnl_report(payload)

    assert [row.label for row in rows] == [
        "Income",
        "Consulting Revenue",
        "Product Sales",
        "Total Income",
        "Expenses",
        "Software",
        "Total Expenses",
    ]
    assert rows[1].section == "Income"
    assert rows[1].row_type == "account"
    assert rows[3].row_type == "total"
    assert rows[1].path == "Income > Consulting Revenue"
    assert rows[5].path == "Expenses > Software"


def test_normalize_pnl_report_parses_parenthesized_negative_amounts() -> None:
    payload = {
        "Rows": {
            "Row": [
                {
                    "Header": {"ColData": [{"value": "Other Expense"}, {"value": "(25.10)"}]},
                    "Rows": {"Row": [{"ColData": [{"value": "Penalties"}, {"value": "(25.10)"}]}]},
                }
            ]
        }
    }

    rows = normalize_pnl_report(payload)

    assert rows[0].amount == Decimal("-25.10")
    assert rows[1].amount == Decimal("-25.10")
    assert rows[0].section == "Other Expense"


def test_normalize_pnl_report_ignores_rows_without_label() -> None:
    payload = {
        "Rows": {
            "Row": [
                {"ColData": [{"value": ""}, {"value": "100.00"}]},
                {"ColData": [{"value": "Valid"}, {"value": "100.00"}]},
            ]
        }
    }

    rows = normalize_pnl_report(payload)
    assert len(rows) == 1
    assert rows[0].label == "Valid"


def test_normalize_pnl_report_raises_for_invalid_amount() -> None:
    payload = {"Rows": {"Row": [{"ColData": [{"value": "Revenue"}, {"value": "abc"}]}]}}
    with pytest.raises(ValueError, match="invalid monetary amount"):
        normalize_pnl_report(payload)

