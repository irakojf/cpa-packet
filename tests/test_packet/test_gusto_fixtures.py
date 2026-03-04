from __future__ import annotations

import json
from pathlib import Path


def test_gusto_payroll_fixture_has_expected_shape() -> None:
    fixture_path = Path("tests/fixtures/gusto/payroll_2025.json")
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))

    assert payload["year"] == 2025
    assert isinstance(payload["payrolls"], list)
    assert len(payload["payrolls"]) >= 2
    first = payload["payrolls"][0]
    assert "uuid" in first
    assert "employee_compensations" in first
    assert isinstance(first["employee_compensations"], list)


def test_gusto_employees_fixture_has_expected_shape() -> None:
    fixture_path = Path("tests/fixtures/gusto/employees_2025.json")
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))

    assert isinstance(payload["employees"], list)
    assert len(payload["employees"]) >= 3
    first = payload["employees"][0]
    assert {"uuid", "first_name", "last_name", "status"} <= set(first)
