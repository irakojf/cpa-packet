"""Filesystem helpers with atomic write semantics."""

from __future__ import annotations

import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import IO


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
            try:
                os.unlink(tmp_name)
            except FileNotFoundError:
                pass
