from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from cpapacket.data.keys import build_cache_key, canonical_json


def test_build_cache_key_is_order_independent_for_params() -> None:
    key_a = build_cache_key(
        source="qbo",
        endpoint="reports/pnl",
        params={"year": 2025, "method": "accrual", "nested": {"b": 2, "a": 1}},
        schema="v1",
        cache_version="1",
    )
    key_b = build_cache_key(
        source="qbo",
        endpoint="reports/pnl",
        params={"nested": {"a": 1, "b": 2}, "method": "accrual", "year": 2025},
        schema="v1",
        cache_version="1",
    )

    assert key_a == key_b


def test_build_cache_key_changes_on_distinct_inputs() -> None:
    base = build_cache_key(
        source="qbo",
        endpoint="reports/pnl",
        params={"year": 2025},
        schema="v1",
        cache_version="1",
    )

    changed = build_cache_key(
        source="qbo",
        endpoint="reports/pnl",
        params={"year": 2025},
        schema="v2",
        cache_version="1",
    )

    assert base != changed


def test_canonical_json_normalizes_dates_and_datetimes() -> None:
    payload = {
        "as_of": date(2025, 12, 31),
        "fetched_at": datetime(2026, 1, 1, 3, 0, tzinfo=UTC),
        "decimal_value": Decimal("12.30"),
        "start": "2025-12-31",
        "instant": "2026-01-01T03:00:00+00:00",
    }

    rendered = canonical_json(payload)

    assert '"as_of":"2025-12-31"' in rendered
    assert '"fetched_at":"2026-01-01T03:00:00Z"' in rendered
    assert '"decimal_value":"12.30"' in rendered
    assert '"start":"2025-12-31"' in rendered
    assert '"instant":"2026-01-01T03:00:00Z"' in rendered


def test_build_cache_key_rejects_float_inputs() -> None:
    with pytest.raises(TypeError, match="float values are not allowed"):
        build_cache_key(
            source="qbo",
            endpoint="reports/pnl",
            params={"year": 2025, "ratio": 0.5},
            schema="v1",
            cache_version="1",
        )
