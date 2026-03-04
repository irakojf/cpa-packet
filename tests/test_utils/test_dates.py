from datetime import date

import pytest

from cpapacket.utils.dates import fiscal_year_end, fiscal_year_start, iso_date, last_day_of_month


@pytest.mark.parametrize(
    ("year", "month", "expected_day"),
    [
        (2025, 1, 31),
        (2025, 2, 28),
        (2024, 2, 29),
        (2025, 3, 31),
        (2025, 4, 30),
        (2025, 5, 31),
        (2025, 6, 30),
        (2025, 7, 31),
        (2025, 8, 31),
        (2025, 9, 30),
        (2025, 10, 31),
        (2025, 11, 30),
        (2025, 12, 31),
    ],
)
def test_last_day_of_month_for_all_months(year: int, month: int, expected_day: int) -> None:
    assert last_day_of_month(year, month) == date(year, month, expected_day)


def test_last_day_of_month_rejects_invalid_month() -> None:
    with pytest.raises(ValueError, match="month must be in 1..12"):
        last_day_of_month(2025, 0)
    with pytest.raises(ValueError, match="month must be in 1..12"):
        last_day_of_month(2025, 13)


def test_year_helpers_and_iso_formatting() -> None:
    assert fiscal_year_start(2025) == date(2025, 1, 1)
    assert fiscal_year_end(2025) == date(2025, 12, 31)
    assert iso_date(date(2025, 7, 4)) == "2025-07-04"


def test_year_helpers_reject_non_positive_year() -> None:
    with pytest.raises(ValueError, match="year must be >= 1"):
        fiscal_year_start(0)
    with pytest.raises(ValueError, match="year must be >= 1"):
        fiscal_year_end(0)
    with pytest.raises(ValueError, match="year must be >= 1"):
        last_day_of_month(0, 1)

