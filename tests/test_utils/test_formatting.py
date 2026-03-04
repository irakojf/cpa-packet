from __future__ import annotations

from decimal import Decimal

import pytest

from cpapacket.utils.formatting import format_currency_csv, format_currency_pdf, indent_for_level


@pytest.mark.parametrize(
    ("amount", "expected"),
    [
        (Decimal("0"), "0.00"),
        (Decimal("-17.505"), "-17.51"),
        (Decimal("1234567890.1"), "1234567890.10"),
        (Decimal("0.004"), "0.00"),
    ],
)
def test_format_currency_csv(amount: Decimal, expected: str) -> None:
    assert format_currency_csv(amount) == expected


@pytest.mark.parametrize(
    ("amount", "expected"),
    [
        (Decimal("0"), "$0.00"),
        (Decimal("-17.505"), "-$17.51"),
        (Decimal("1234567890.1"), "$1,234,567,890.10"),
        (Decimal("0.004"), "$0.00"),
    ],
)
def test_format_currency_pdf(amount: Decimal, expected: str) -> None:
    assert format_currency_pdf(amount) == expected


def test_currency_formatting_rejects_float_and_non_finite_values() -> None:
    with pytest.raises(TypeError, match="Decimal"):
        format_currency_csv(1.23)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="Decimal"):
        format_currency_pdf(1.23)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="finite"):
        format_currency_csv(Decimal("NaN"))
    with pytest.raises(ValueError, match="finite"):
        format_currency_pdf(Decimal("Infinity"))


def test_indent_for_level() -> None:
    assert indent_for_level(0) == ""
    assert indent_for_level(1) == "  "
    assert indent_for_level(3, spaces_per_level=4) == " " * 12

    with pytest.raises(ValueError, match="level must be >= 0"):
        indent_for_level(-1)
    with pytest.raises(ValueError, match="spaces_per_level must be >= 1"):
        indent_for_level(1, spaces_per_level=0)
