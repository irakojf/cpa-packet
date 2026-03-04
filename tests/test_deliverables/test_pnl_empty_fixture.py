from __future__ import annotations

import json
from pathlib import Path


def test_empty_pnl_fixture_has_expected_shape() -> None:
    fixture_path = Path("tests/fixtures/qbo/pnl_empty_2025.json")
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))

    assert payload["Header"]["ReportName"] == "ProfitAndLoss"
    assert payload["Header"]["StartPeriod"] == "2025-01-01"
    assert payload["Header"]["EndPeriod"] == "2025-12-31"
    assert payload["Rows"]["Row"] == []
