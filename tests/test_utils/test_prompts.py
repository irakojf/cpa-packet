from __future__ import annotations

from pathlib import Path

import pytest

from cpapacket.utils.prompts import resolve_output_path


def test_resolve_output_path_returns_original_when_missing(tmp_path: Path) -> None:
    target = tmp_path / "missing.csv"
    resolved = resolve_output_path(target)
    assert resolved == target


def test_resolve_output_path_overwrite_and_copy_modes(tmp_path: Path) -> None:
    target = tmp_path / "exists.csv"
    target.write_text("x", encoding="utf-8")

    overwrite = resolve_output_path(target, on_conflict="overwrite")
    assert overwrite == target

    copy = resolve_output_path(target, on_conflict="copy")
    assert copy != target
    assert copy.name.startswith("exists__copy_")
    assert copy.suffix == ".csv"


def test_resolve_output_path_abort_modes(tmp_path: Path) -> None:
    target = tmp_path / "exists.csv"
    target.write_text("x", encoding="utf-8")

    with pytest.raises(FileExistsError):
        resolve_output_path(target, on_conflict="abort")

    with pytest.raises(FileExistsError):
        resolve_output_path(target, non_interactive=True)


def test_resolve_output_path_rejects_invalid_policy(tmp_path: Path) -> None:
    target = tmp_path / "exists.csv"
    target.write_text("x", encoding="utf-8")

    with pytest.raises(ValueError, match="on_conflict must be one of"):
        resolve_output_path(target, on_conflict="replace")


def test_resolve_output_path_interactive_choice_and_retry(tmp_path: Path) -> None:
    target = tmp_path / "exists.csv"
    target.write_text("x", encoding="utf-8")

    responses = iter(["bad", "c"])
    copy_path = resolve_output_path(target, input_fn=lambda _prompt: next(responses))
    assert copy_path.name.startswith("exists__copy_")

    abort_path_responses = iter([""])
    with pytest.raises(FileExistsError):
        resolve_output_path(target, input_fn=lambda _prompt: next(abort_path_responses))
