"""Logging configuration helpers for cpapacket CLI and runners."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import TextIO

try:
    from rich.logging import RichHandler
except Exception:  # pragma: no cover - exercised only when rich is missing
    RichHandler = None  # type: ignore[assignment]


def configure_logging(
    *,
    verbose: bool = False,
    quiet: bool = False,
    plain: bool = False,
    meta_dir: Path = Path("_meta"),
    stream: TextIO | None = None,
) -> logging.Logger:
    """Configure cpapacket logger with console + file handlers."""
    logger = logging.getLogger("cpapacket")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    logger.handlers.clear()

    console_level = _console_level(verbose=verbose, quiet=quiet)
    logger.addHandler(
        _build_console_handler(
            level=console_level,
            plain=plain,
            stream=stream,
        )
    )

    meta_dir.mkdir(parents=True, exist_ok=True)
    log_file = meta_dir / "cpapacket.log"
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
    )
    logger.addHandler(file_handler)
    return logger


def _console_level(*, verbose: bool, quiet: bool) -> int:
    if quiet:
        return logging.WARNING
    if verbose:
        return logging.DEBUG
    return logging.INFO


def _build_console_handler(
    *,
    level: int,
    plain: bool,
    stream: TextIO | None,
) -> logging.Handler:
    no_color = bool(os.getenv("NO_COLOR"))
    if plain or no_color or RichHandler is None:
        handler = logging.StreamHandler(stream)
        handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    else:
        handler = RichHandler(
            show_time=False,
            show_path=False,
            rich_tracebacks=True,
            markup=False,
            log_time_format="[%X]",
            console=None,
        )
        handler.setFormatter(logging.Formatter("%(message)s"))

    handler.setLevel(level)
    return handler
