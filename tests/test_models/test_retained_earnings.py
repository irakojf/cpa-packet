from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic import ValidationError

from cpapacket.models.retained_earnings import RetainedEarningsRollforward


@pytest.mark.parametrize("status", ["Balanced", "Mismatch"])
def test_retained_earnings_rollforward_accepts_supported_status_values(status: str) -> None:
    rollforward = RetainedEarningsRollforward(
        beginning_re="10",
        net_income="5",
        distributions="2",
        expected_ending_re="13",
        actual_ending_re="13",
        difference="0",
        status=status,  # type: ignore[arg-type]
        flags=[],
    )

    assert rollforward.status == status


def test_retained_earnings_rollforward_happy_path_quantizes_and_normalizes() -> None:
    rollforward = RetainedEarningsRollforward(
        beginning_re="1000",
        net_income="500.125",
        distributions="200",
        expected_ending_re="1300.128",
        actual_ending_re="1300.129",
        difference="-0.001",
        status="Mismatch",
        flags=[" basis_risk_distributions_exceed_net_income ", " "],
    )

    assert rollforward.beginning_re == Decimal("1000.00")
    assert rollforward.net_income == Decimal("500.13")
    assert rollforward.expected_ending_re == Decimal("1300.13")
    assert rollforward.actual_ending_re == Decimal("1300.13")
    assert rollforward.difference == Decimal("-0.00")
    assert rollforward.flags == ["basis_risk_distributions_exceed_net_income"]


def test_retained_earnings_rollforward_flags_keep_non_blank_entries() -> None:
    rollforward = RetainedEarningsRollforward(
        beginning_re="0",
        net_income="0",
        distributions="0",
        expected_ending_re="0",
        actual_ending_re="0",
        difference="0",
        status="Balanced",
        flags=[" flag_one ", "", "flag_two", "   "],
    )

    assert rollforward.flags == ["flag_one", "flag_two"]


def test_retained_earnings_rollforward_rejects_invalid_values() -> None:
    with pytest.raises(ValidationError):
        RetainedEarningsRollforward(
            beginning_re="nan",
            net_income="1",
            distributions="1",
            expected_ending_re="1",
            actual_ending_re="1",
            difference="0",
            status="Balanced",
            flags=[],
        )

    with pytest.raises(ValidationError):
        RetainedEarningsRollforward(
            beginning_re="1",
            net_income="1",
            distributions="1",
            expected_ending_re="1",
            actual_ending_re="1",
            difference="0",
            status="Unknown",
            flags=[],
        )


def test_retained_earnings_rollforward_is_frozen() -> None:
    rollforward = RetainedEarningsRollforward(
        beginning_re="100",
        net_income="50",
        distributions="10",
        expected_ending_re="140",
        actual_ending_re="140",
        difference="0",
        status="Balanced",
        flags=[],
    )

    with pytest.raises(ValidationError):
        rollforward.status = "Mismatch"  # type: ignore[misc]
