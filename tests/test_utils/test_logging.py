from __future__ import annotations

import io
import logging
from pathlib import Path

import pytest

from cpapacket.utils.logging import (
    LOGGER_NAME,
    configure_logging,
    get_logger,
    reset_logging,
)


@pytest.fixture(autouse=True)
def _clean_logger() -> None:  # type: ignore[misc]
    """Ensure a clean logger state before each test."""
    reset_logging()
    yield  # type: ignore[misc]
    reset_logging()


def test_configure_logging_creates_dual_handlers(tmp_path: Path) -> None:
    logger = configure_logging(log_dir=tmp_path, plain=True, stream=io.StringIO())
    handlers = logger.handlers

    assert len(handlers) == 2
    assert any(isinstance(h, logging.FileHandler) for h in handlers)
    assert any(isinstance(h, logging.StreamHandler) for h in handlers)

    file_handler = next(h for h in handlers if isinstance(h, logging.FileHandler))
    assert file_handler.level == logging.DEBUG
    assert (tmp_path / "cpapacket.log").exists()


def test_configure_logging_console_only_when_no_log_dir() -> None:
    logger = configure_logging(plain=True, stream=io.StringIO())
    assert len(logger.handlers) == 1
    assert isinstance(logger.handlers[0], logging.StreamHandler)


def test_console_level_default(tmp_path: Path) -> None:
    logger = configure_logging(plain=True, log_dir=tmp_path, stream=io.StringIO())
    console = next(h for h in logger.handlers if not isinstance(h, logging.FileHandler))
    assert console.level == logging.INFO


def test_console_level_verbose(tmp_path: Path) -> None:
    logger = configure_logging(verbose=True, plain=True, log_dir=tmp_path, stream=io.StringIO())
    console = next(h for h in logger.handlers if not isinstance(h, logging.FileHandler))
    assert console.level == logging.DEBUG


def test_console_level_quiet(tmp_path: Path) -> None:
    logger = configure_logging(quiet=True, plain=True, log_dir=tmp_path, stream=io.StringIO())
    console = next(h for h in logger.handlers if not isinstance(h, logging.FileHandler))
    assert console.level == logging.WARNING


def test_verbose_overrides_quiet(tmp_path: Path) -> None:
    logger = configure_logging(
        verbose=True, quiet=True, plain=True, log_dir=tmp_path, stream=io.StringIO()
    )
    console = next(h for h in logger.handlers if not isinstance(h, logging.FileHandler))
    assert console.level == logging.DEBUG


def test_no_color_forces_plain_handler(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("NO_COLOR", "1")
    logger = configure_logging(log_dir=tmp_path, plain=False, stream=io.StringIO())
    console = next(h for h in logger.handlers if not isinstance(h, logging.FileHandler))
    assert type(console).__name__ != "RichHandler"


def test_file_handler_writes_debug_entries(tmp_path: Path) -> None:
    logger = configure_logging(log_dir=tmp_path, plain=True, stream=io.StringIO())
    logger.debug("test-debug-entry")

    # Flush file handler
    for h in logger.handlers:
        h.flush()

    log_content = (tmp_path / "cpapacket.log").read_text()
    assert "test-debug-entry" in log_content
    assert "DEBUG" in log_content


def test_console_handler_filters_debug_at_info_level() -> None:
    buf = io.StringIO()
    logger = configure_logging(plain=True, stream=buf)
    logger.debug("should-not-appear")
    logger.info("should-appear")
    output = buf.getvalue()
    assert "should-not-appear" not in output
    assert "should-appear" in output


def test_get_logger_returns_child() -> None:
    child = get_logger("test.module")
    assert child.name == f"{LOGGER_NAME}.test.module"


def test_get_logger_none_returns_root() -> None:
    root = get_logger(None)
    assert root.name == LOGGER_NAME


def test_log_dir_created_if_missing(tmp_path: Path) -> None:
    nested = tmp_path / "deep" / "nested" / "private"
    configure_logging(log_dir=nested, plain=True, stream=io.StringIO())
    assert nested.exists()
    assert (nested / "cpapacket.log").exists()


def test_reset_logging_removes_handlers() -> None:
    configure_logging(plain=True, stream=io.StringIO())
    logger = logging.getLogger(LOGGER_NAME)
    assert len(logger.handlers) > 0
    reset_logging()
    assert len(logger.handlers) == 0
