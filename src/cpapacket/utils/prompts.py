"""Reusable prompt and conflict-resolution helpers for output paths."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from time import gmtime, strftime
from typing import Literal

ConflictAction = Literal["overwrite", "copy", "abort"]

_VALID_ACTIONS = {"overwrite", "copy", "abort"}


def resolve_output_path(
    path: str | Path,
    *,
    on_conflict: str | None = None,
    non_interactive: bool = False,
    input_fn: Callable[[str], str] = input,
) -> Path:
    """Resolve output target path according to overwrite/copy/abort policy."""
    target = Path(path)
    if not target.exists():
        return target

    action = _select_conflict_action(
        on_conflict=on_conflict,
        non_interactive=non_interactive,
        input_fn=input_fn,
    )
    if action == "overwrite":
        return target
    if action == "copy":
        suffix = strftime("%Y%m%d_%H%M%S", gmtime())
        return target.with_name(f"{target.stem}__copy_{suffix}{target.suffix}")
    raise FileExistsError(f"{target} already exists and conflict action is abort")


def _select_conflict_action(
    *,
    on_conflict: str | None,
    non_interactive: bool,
    input_fn: Callable[[str], str],
) -> ConflictAction:
    if on_conflict is not None:
        normalized = on_conflict.strip().lower()
        if normalized not in _VALID_ACTIONS:
            raise ValueError("on_conflict must be one of: overwrite, copy, abort")
        return normalized  # type: ignore[return-value]

    if non_interactive:
        return "abort"

    prompt = "Output exists. Choose [o]verwrite, [c]opy, or [a]bort (default: a): "
    while True:
        choice = input_fn(prompt).strip().lower()
        if choice in {"", "a", "abort"}:
            return "abort"
        if choice in {"o", "overwrite"}:
            return "overwrite"
        if choice in {"c", "copy"}:
            return "copy"
