from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic import ValidationError

from cpapacket.models.normalized import NormalizedRow


def test_normalized_row_happy_path() -> None:
    row = NormalizedRow(
        section=" Income ",
        label=" Consulting Revenue ",
        amount="123.456",
        row_type="account",
        level=1,
        path=" Income:Consulting ",
    )

    assert row.section == "Income"
    assert row.label == "Consulting Revenue"
    assert row.path == "Income:Consulting"
    assert row.amount == Decimal("123.46")


def test_normalized_row_rejects_invalid_row_type() -> None:
    with pytest.raises(ValidationError):
        NormalizedRow(
            section="Income",
            label="Revenue",
            amount="10.00",
            row_type="detail",
            level=0,
            path="Income:Revenue",
        )


def test_normalized_row_rejects_negative_level() -> None:
    with pytest.raises(ValidationError):
        NormalizedRow(
            section="Income",
            label="Revenue",
            amount="10.00",
            row_type="account",
            level=-1,
            path="Income:Revenue",
        )


def test_normalized_row_rejects_blank_text_fields() -> None:
    with pytest.raises(ValidationError):
        NormalizedRow(
            section="   ",
            label="Revenue",
            amount="10.00",
            row_type="account",
            level=0,
            path="Income:Revenue",
        )

    with pytest.raises(ValidationError):
        NormalizedRow(
            section="Income",
            label="   ",
            amount="10.00",
            row_type="account",
            level=0,
            path="Income:Revenue",
        )

    with pytest.raises(ValidationError):
        NormalizedRow(
            section="Income",
            label="Revenue",
            amount="10.00",
            row_type="account",
            level=0,
            path="   ",
        )


def test_normalized_row_rejects_non_finite_amount() -> None:
    with pytest.raises(ValidationError):
        NormalizedRow(
            section="Income",
            label="Revenue",
            amount=Decimal("NaN"),
            row_type="account",
            level=0,
            path="Income:Revenue",
        )

    with pytest.raises(ValidationError):
        NormalizedRow(
            section="Income",
            label="Revenue",
            amount=Decimal("Infinity"),
            row_type="account",
            level=0,
            path="Income:Revenue",
        )


def test_normalized_row_is_frozen() -> None:
    row = NormalizedRow(
        section="Income",
        label="Revenue",
        amount="10.00",
        row_type="account",
        level=0,
        path="Income:Revenue",
    )

    with pytest.raises(ValidationError):
        row.level = 2
