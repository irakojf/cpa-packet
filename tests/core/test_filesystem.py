from __future__ import annotations

from pathlib import Path

import pytest

from cpapacket.core.filesystem import atomic_write


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
