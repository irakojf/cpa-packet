"""Dual logging configuration: rich console + persistent file handler.

Per PLAN.md ADR-8:
- Console: RichHandler at INFO (DEBUG with --verbose, WARNING with --quiet)
- File: FileHandler to _meta/private/cpapacket.log at DEBUG always
- Respects NO_COLOR env var and --plain flag
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import TextIO

try:
    from rich.logging import RichHandler as _RichHandler
except Exception:  # pragma: no cover - exercised only when rich is missing
    _RichHandler = None  # type: ignore[misc,assignment]

LOGGER_NAME = "cpapacket"


def configure_logging(
    *,
    verbose: bool = False,
    quiet: bool = False,
    plain: bool = False,
    log_dir: Path | None = None,
    stream: TextIO | None = None,
) -> logging.Logger:
    """Configure cpapacket logger with console + file handlers.

    Parameters
    ----------
    verbose:
        Set console level to DEBUG. Takes precedence over *quiet*.
    quiet:
        Set console level to WARNING. Ignored when *verbose* is True.
    plain:
        Disable rich formatting; use a plain StreamHandler instead.
    log_dir:
        Directory for the persistent log file (``cpapacket.log``).
        When ``None`` the file handler is not attached.
    stream:
        Override stderr for the console handler (useful for tests).
    """
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    _clear_handlers(logger)

    console_level = _console_level(verbose=verbose, quiet=quiet)
    logger.addHandler(
        _build_console_handler(
            level=console_level,
            plain=plain,
            stream=stream,
        )
    )

    if log_dir is not None:
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "cpapacket.log"
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
        )
        logger.addHandler(file_handler)

    return logger


def get_logger(name: str | None = None) -> logging.Logger:
    """Return a child logger under the ``cpapacket`` namespace.

    Usage::

        from cpapacket.utils.logging import get_logger
        logger = get_logger(__name__)
        logger.info("Fetching P&L report")
    """
    if name is None:
        return logging.getLogger(LOGGER_NAME)
    return logging.getLogger(f"{LOGGER_NAME}.{name}")


def reset_logging() -> None:
    """Remove all handlers from the cpapacket logger. Intended for tests."""
    logger = logging.getLogger(LOGGER_NAME)
    _clear_handlers(logger)


def _clear_handlers(logger: logging.Logger) -> None:
    """Close and remove all handlers from *logger*."""
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)


def _console_level(*, verbose: bool, quiet: bool) -> int:
    if verbose:
        return logging.DEBUG
    if quiet:
        return logging.WARNING
    return logging.INFO


def _build_console_handler(
    *,
    level: int,
    plain: bool,
    stream: TextIO | None,
) -> logging.Handler:
    no_color = bool(os.getenv("NO_COLOR"))
    use_plain = plain or no_color or _RichHandler is None
    handler: logging.Handler
    if use_plain:
        handler = logging.StreamHandler(stream)
        handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    else:
        handler = _RichHandler(
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
