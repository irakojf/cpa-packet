from __future__ import annotations

from pathlib import Path

import pytest

from cpapacket.core.filesystem import atomic_write, ensure_directory, sanitize_filesystem_name


def test_atomic_write_text_success(tmp_path: Path) -> None:
    destination = tmp_path / "report.txt"

    with atomic_write(destination) as handle:
        assert isinstance(handle, object)
        handle.write("hello world")

    assert destination.read_text(encoding="utf-8") == "hello world"
    assert not list(tmp_path.glob("*.tmp"))


def test_atomic_write_binary_success(tmp_path: Path) -> None:
    destination = tmp_path / "raw.bin"
    payload = b"\x00\x01\xff"

    with atomic_write(destination, mode="wb") as handle:
        handle.write(payload)

    assert destination.read_bytes() == payload
    assert not list(tmp_path.glob("*.tmp"))


def test_atomic_write_cleans_up_on_error(tmp_path: Path) -> None:
    destination = tmp_path / "should_not_exist.txt"

    with pytest.raises(RuntimeError, match="boom"):
        with atomic_write(destination) as handle:
            handle.write("partial")
            raise RuntimeError("boom")

    assert not destination.exists()
    assert not list(tmp_path.glob("*.tmp"))


def test_atomic_write_overwrites_existing_file(tmp_path: Path) -> None:
    destination = tmp_path / "existing.txt"
    destination.write_text("old", encoding="utf-8")

    with atomic_write(destination) as handle:
        handle.write("new")

    assert destination.read_text(encoding="utf-8") == "new"


def test_atomic_write_rejects_unsupported_modes(tmp_path: Path) -> None:
    destination = tmp_path / "unsupported.txt"

    with pytest.raises(ValueError, match="supports write/xb modes only"):
        with atomic_write(destination, mode="r"):
            pass

    with pytest.raises(ValueError, match="supports write/xb modes only"):
        with atomic_write(destination, mode="a"):
            pass


def test_sanitize_filesystem_name_replaces_unsafe_chars_and_collapses_underscores() -> None:
    raw = '  ACME / North-East: "Holdings"  <2025>?  '
    sanitized = sanitize_filesystem_name(raw)
    assert sanitized == "ACME_North-East_Holdings_2025"


def test_sanitize_filesystem_name_defaults_for_blank_values() -> None:
    assert sanitize_filesystem_name("   ") == "untitled"
    assert sanitize_filesystem_name("////") == "untitled"


def test_ensure_directory_creates_parents(tmp_path: Path) -> None:
    target = tmp_path / "a" / "b" / "c"
    resolved = ensure_directory(target)
    assert resolved == target
    assert target.exists()
    assert target.is_dir()
