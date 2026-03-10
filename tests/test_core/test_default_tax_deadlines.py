from __future__ import annotations

from datetime import date

import pytest

from cpapacket.core.default_tax_deadlines import generate_default_tax_deadlines


def test_generate_default_tax_deadlines_expected_rows() -> None:
    deadlines = generate_default_tax_deadlines(year=2026)

    assert len(deadlines) == 11

    by_key = {(item.jurisdiction, item.name): item for item in deadlines}

    assert by_key[("Federal", "1120-S Filing")].due_date == date(2026, 3, 15)
    assert by_key[("Federal", "1120-S Filing")].category == "filing"

    assert by_key[("Federal", "Individual Filing")].due_date == date(2026, 4, 15)
    assert by_key[("Federal", "Individual Filing")].category == "filing"

    assert by_key[("Federal", "Q1 Estimated Tax")].due_date == date(2026, 4, 15)
    assert by_key[("Federal", "Q2 Estimated Tax")].due_date == date(2026, 6, 16)
    assert by_key[("Federal", "Q3 Estimated Tax")].due_date == date(2026, 9, 15)
    assert by_key[("Federal", "Q4 Estimated Tax")].due_date == date(2027, 1, 15)

    assert by_key[("NY", "Q1 Estimated Tax")].due_date == date(2026, 4, 15)
    assert by_key[("NY", "Q2 Estimated Tax")].due_date == date(2026, 6, 16)
    assert by_key[("NY", "Q3 Estimated Tax")].due_date == date(2026, 9, 15)
    assert by_key[("NY", "Q4 Estimated Tax")].due_date == date(2027, 1, 15)

    assert by_key[("DE", "Franchise Tax")].due_date == date(2026, 3, 1)
    assert by_key[("DE", "Franchise Tax")].category == "filing"


def test_generate_default_tax_deadlines_sorted_by_due_date() -> None:
    deadlines = generate_default_tax_deadlines(year=2026)

    assert [item.due_date for item in deadlines] == sorted(item.due_date for item in deadlines)


def test_generate_default_tax_deadlines_rejects_out_of_range_year() -> None:
    with pytest.raises(ValueError, match="year must be in"):
        generate_default_tax_deadlines(year=1999)
