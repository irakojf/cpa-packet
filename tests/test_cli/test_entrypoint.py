from __future__ import annotations

import importlib
import tomllib
from pathlib import Path
from typing import Any

MAIN_MODULE = importlib.import_module("cpapacket.cli.main")


def _project_scripts() -> dict[str, str]:
    pyproject_path = Path(__file__).resolve().parents[2] / "pyproject.toml"
    payload = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    project = payload.get("project", {})
    scripts = project.get("scripts", {})
    return dict(scripts)


def _project_table() -> dict[str, Any]:
    pyproject_path = Path(__file__).resolve().parents[2] / "pyproject.toml"
    payload = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    project = payload.get("project", {})
    return dict(project)


def test_cpapacket_console_script_points_to_main_function() -> None:
    scripts = _project_scripts()
    assert scripts["cpapacket"] == "cpapacket.cli.main:main"


def test_cpapacket_console_script_target_is_importable() -> None:
    target = _project_scripts()["cpapacket"]
    module_name, symbol = target.split(":")
    module = importlib.import_module(module_name)
    assert callable(getattr(module, symbol))


def test_main_delegates_to_click_group_main(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}

    def _fake_cli_main(*, args: list[str] | None, standalone_mode: bool) -> int:
        captured["args"] = args
        captured["standalone_mode"] = standalone_mode
        return 42

    monkeypatch.setattr(MAIN_MODULE.cli, "main", _fake_cli_main)

    result = MAIN_MODULE.main(["--help"])

    assert result == 42
    assert captured["args"] == ["--help"]
    assert captured["standalone_mode"] is False


def test_pyproject_includes_distribution_metadata() -> None:
    project = _project_table()

    assert project["requires-python"] == ">=3.11"
    assert project["license"]["text"] == "Proprietary"

    classifiers = project["classifiers"]
    assert "Programming Language :: Python :: 3.11" in classifiers
    assert "Environment :: Console" in classifiers

    urls = project["urls"]
    assert urls["Homepage"].startswith("https://")
    assert urls["Repository"].startswith("https://")
    assert urls["Issues"].startswith("https://")
