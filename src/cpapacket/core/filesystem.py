"""Filesystem helpers with atomic write semantics."""

from __future__ import annotations

import os
import re
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import IO

_UNSAFE_FS_CHARS_PATTERN = re.compile(r"[\\/:*?\"<>|]+")
_WHITESPACE_PATTERN = re.compile(r"\s+")
_MULTI_UNDERSCORE_PATTERN = re.compile(r"_+")


@contextmanager
def atomic_write(
    path: str | Path,
    *,
    mode: str = "w",
    encoding: str = "utf-8",
    newline: str | None = None,
) -> Iterator[IO[str] | IO[bytes]]:
    """Write to a temporary sibling file and atomically replace destination on success."""
    if "r" in mode or "+" in mode or "a" in mode:
        raise ValueError("atomic_write supports write/xb modes only")

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)

    fd, tmp_name = tempfile.mkstemp(
        dir=str(target.parent),
        prefix=f".{target.name}.",
        suffix=".tmp",
    )
    os.close(fd)

    replaced = False
    try:
        if "b" in mode:
            with open(tmp_name, mode) as handle:
                yield handle
                handle.flush()
                os.fsync(handle.fileno())
        else:
            with open(tmp_name, mode, encoding=encoding, newline=newline) as handle:
                yield handle
                handle.flush()
                os.fsync(handle.fileno())

        os.replace(tmp_name, target)
        replaced = True
    finally:
        if not replaced:
            with suppress(FileNotFoundError):
                os.unlink(tmp_name)


def sanitize_filesystem_name(value: str) -> str:
    """Convert arbitrary text into a filesystem-safe directory name."""
    normalized = _WHITESPACE_PATTERN.sub("_", value.strip())
    normalized = _UNSAFE_FS_CHARS_PATTERN.sub("_", normalized)
    normalized = _MULTI_UNDERSCORE_PATTERN.sub("_", normalized).strip("_")
    return normalized or "untitled"


def ensure_directory(path: str | Path) -> Path:
    """Create directory and parents if missing, returning normalized Path."""
    target = Path(path)
    target.mkdir(parents=True, exist_ok=True)
    return target
