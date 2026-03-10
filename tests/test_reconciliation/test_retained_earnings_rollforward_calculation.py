from __future__ import annotations

from decimal import Decimal

from cpapacket.reconciliation.retained_earnings import (
    RetainedEarningsSourceData,
    build_retained_earnings_rollforward,
)
from cpapacket.utils.constants import RETAINED_EARNINGS_TOLERANCE


def _source(
    *,
    beginning_re: str,
    net_income: str,
    distributions: str,
    actual_ending_re: str,
) -> RetainedEarningsSourceData:
    return RetainedEarningsSourceData(
        beginning_retained_earnings=Decimal(beginning_re),
        net_income=Decimal(net_income),
        distributions=Decimal(distributions),
        actual_ending_retained_earnings=Decimal(actual_ending_re),
        gl_rows=[],
    )


def test_build_rollforward_calculates_expected_and_difference() -> None:
    rollforward = build_retained_earnings_rollforward(
        source=_source(
            beginning_re="100.00",
            net_income="250.00",
            distributions="25.00",
            actual_ending_re="320.00",
        ),
        structural_flags=[],
    )

    assert rollforward.expected_ending_re == Decimal("325.00")
    assert rollforward.difference == Decimal("5.00")
    assert rollforward.status == "Mismatch"


def test_build_rollforward_is_balanced_at_tolerance_boundary() -> None:
    expected = Decimal("325.00")
    rollforward = build_retained_earnings_rollforward(
        source=_source(
            beginning_re="100.00",
            net_income="250.00",
            distributions="25.00",
            actual_ending_re=str(expected - RETAINED_EARNINGS_TOLERANCE),
        ),
        structural_flags=[],
    )

    assert rollforward.expected_ending_re == expected
    assert rollforward.difference == RETAINED_EARNINGS_TOLERANCE
    assert rollforward.status == "Balanced"


def test_build_rollforward_is_mismatch_above_tolerance() -> None:
    expected = Decimal("325.00")
    rollforward = build_retained_earnings_rollforward(
        source=_source(
            beginning_re="100.00",
            net_income="250.00",
            distributions="25.00",
            actual_ending_re=str(expected - RETAINED_EARNINGS_TOLERANCE - Decimal("0.01")),
        ),
        structural_flags=[],
    )

    assert rollforward.difference == RETAINED_EARNINGS_TOLERANCE + Decimal("0.01")
    assert rollforward.status == "Mismatch"


def test_build_rollforward_zero_values_balanced() -> None:
    rollforward = build_retained_earnings_rollforward(
        source=_source(
            beginning_re="0.00",
            net_income="0.00",
            distributions="0.00",
            actual_ending_re="0.00",
        ),
        structural_flags=[],
    )

    assert rollforward.expected_ending_re == Decimal("0.00")
    assert rollforward.difference == Decimal("0.00")
    assert rollforward.status == "Balanced"


def test_build_rollforward_negative_net_income_loss() -> None:
    rollforward = build_retained_earnings_rollforward(
        source=_source(
            beginning_re="100.00",
            net_income="-50.00",
            distributions="10.00",
            actual_ending_re="40.00",
        ),
        structural_flags=[],
    )

    assert rollforward.expected_ending_re == Decimal("40.00")
    assert rollforward.difference == Decimal("0.00")
    assert rollforward.status == "Balanced"
