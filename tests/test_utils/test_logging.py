from __future__ import annotations

import io
import logging
from pathlib import Path

import pytest

from cpapacket.utils.logging import configure_logging


def test_configure_logging_creates_dual_handlers(tmp_path: Path) -> None:
    logger = configure_logging(meta_dir=tmp_path, plain=True, stream=io.StringIO())
    handlers = logger.handlers

    assert len(handlers) == 2
    assert any(isinstance(handler, logging.FileHandler) for handler in handlers)
    assert any(isinstance(handler, logging.StreamHandler) for handler in handlers)

    file_handler = next(
        handler for handler in handlers if isinstance(handler, logging.FileHandler)
    )
    assert file_handler.level == logging.DEBUG
    assert (tmp_path / "cpapacket.log").exists()


def test_console_level_flags(tmp_path: Path) -> None:
    debug_logger = configure_logging(
        verbose=True,
        plain=True,
        meta_dir=tmp_path / "debug",
        stream=io.StringIO(),
    )
    warning_logger = configure_logging(
        quiet=True,
        plain=True,
        meta_dir=tmp_path / "warning",
        stream=io.StringIO(),
    )
    default_logger = configure_logging(
        plain=True,
        meta_dir=tmp_path / "default",
        stream=io.StringIO(),
    )

    def first_console_level(logger: logging.Logger) -> int:
        return next(
            handler.level
            for handler in logger.handlers
            if not isinstance(handler, logging.FileHandler)
        )

    assert first_console_level(debug_logger) == logging.DEBUG
    assert first_console_level(warning_logger) == logging.WARNING
    assert first_console_level(default_logger) == logging.INFO


def test_no_color_forces_plain_handler(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("NO_COLOR", "1")
    logger = configure_logging(meta_dir=tmp_path, plain=False, stream=io.StringIO())
    console_handler = next(
        handler
        for handler in logger.handlers
        if not isinstance(handler, logging.FileHandler)
    )

    assert type(console_handler).__name__ != "RichHandler"
