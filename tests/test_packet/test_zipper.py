from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from cpapacket.packet.zipper import create_packet_zip


def _make_packet_tree(tmp_path: Path) -> Path:
    packet_root = tmp_path / "Acme_2025_CPA_Packet"
    (packet_root / "01_Year-End_Profit_and_Loss").mkdir(parents=True, exist_ok=True)
    (packet_root / "_meta" / "public").mkdir(parents=True, exist_ok=True)
    (packet_root / "_meta" / "private").mkdir(parents=True, exist_ok=True)

    (packet_root / "01_Year-End_Profit_and_Loss" / "Profit_and_Loss_2025.csv").write_text(
        "section,amount\nIncome,100.00\n",
        encoding="utf-8",
    )
    (packet_root / "_meta" / "public" / "validation_report.txt").write_text(
        "ok\n",
        encoding="utf-8",
    )
    (packet_root / "_meta" / "private" / "cpapacket.log").write_text(
        "debug\n",
        encoding="utf-8",
    )
    (packet_root / "_meta" / "private" / "cache.json.gz").write_text(
        "cache\n",
        encoding="utf-8",
    )
    return packet_root


def test_create_packet_zip_excludes_private_meta_by_default(tmp_path: Path) -> None:
    packet_root = _make_packet_tree(tmp_path)

    zip_path = create_packet_zip(
        packet_root=packet_root,
        on_conflict="abort",
        non_interactive=True,
    )

    assert zip_path == tmp_path / "Acme_2025_CPA_Packet.zip"
    assert zip_path.exists()
    assert not list(tmp_path.glob("*.tmp"))

    with zipfile.ZipFile(zip_path, mode="r") as archive:
        names = set(archive.namelist())

    assert "Acme_2025_CPA_Packet/01_Year-End_Profit_and_Loss/Profit_and_Loss_2025.csv" in names
    assert "Acme_2025_CPA_Packet/_meta/public/validation_report.txt" in names
    assert "Acme_2025_CPA_Packet/_meta/private/cpapacket.log" not in names


def test_create_packet_zip_can_include_debug_log_only(tmp_path: Path) -> None:
    packet_root = _make_packet_tree(tmp_path)

    zip_path = create_packet_zip(
        packet_root=packet_root,
        on_conflict="abort",
        non_interactive=True,
        include_debug_log=True,
    )

    with zipfile.ZipFile(zip_path, mode="r") as archive:
        names = set(archive.namelist())

    assert "Acme_2025_CPA_Packet/_meta/private/cpapacket.log" in names
    assert "Acme_2025_CPA_Packet/_meta/private/cache.json.gz" not in names


def test_create_packet_zip_abort_conflict_raises(tmp_path: Path) -> None:
    packet_root = _make_packet_tree(tmp_path)
    target_zip = tmp_path / "Acme_2025_CPA_Packet.zip"
    target_zip.write_text("existing", encoding="utf-8")

    with pytest.raises(FileExistsError):
        create_packet_zip(
            packet_root=packet_root,
            on_conflict="abort",
            non_interactive=True,
        )


def test_create_packet_zip_copy_conflict_keeps_existing_and_creates_copy(tmp_path: Path) -> None:
    packet_root = _make_packet_tree(tmp_path)
    target_zip = tmp_path / "Acme_2025_CPA_Packet.zip"
    target_zip.write_text("existing", encoding="utf-8")

    copy_zip = create_packet_zip(
        packet_root=packet_root,
        on_conflict="copy",
        non_interactive=True,
    )

    assert target_zip.read_text(encoding="utf-8") == "existing"
    assert copy_zip != target_zip
    assert copy_zip.name.startswith("Acme_2025_CPA_Packet__copy_")
    assert copy_zip.suffix == ".zip"
    with zipfile.ZipFile(copy_zip, mode="r") as archive:
        assert (
            "Acme_2025_CPA_Packet/_meta/public/validation_report.txt"
            in set(archive.namelist())
        )


def test_create_packet_zip_overwrite_conflict_replaces_existing(tmp_path: Path) -> None:
    packet_root = _make_packet_tree(tmp_path)
    target_zip = tmp_path / "Acme_2025_CPA_Packet.zip"
    target_zip.write_text("existing", encoding="utf-8")

    zip_path = create_packet_zip(
        packet_root=packet_root,
        on_conflict="overwrite",
        non_interactive=True,
    )

    assert zip_path == target_zip
    with zipfile.ZipFile(zip_path, mode="r") as archive:
        assert (
            "Acme_2025_CPA_Packet/01_Year-End_Profit_and_Loss/Profit_and_Loss_2025.csv"
            in set(archive.namelist())
        )


def test_create_packet_zip_never_deletes_source_folder(tmp_path: Path) -> None:
    packet_root = _make_packet_tree(tmp_path)

    _ = create_packet_zip(
        packet_root=packet_root,
        on_conflict="abort",
        non_interactive=True,
    )

    assert packet_root.exists()
    assert (packet_root / "_meta" / "public" / "validation_report.txt").exists()


def test_create_packet_zip_handles_many_files(tmp_path: Path) -> None:
    packet_root = _make_packet_tree(tmp_path)
    bulk_dir = packet_root / "03_Full-Year_General_Ledger"
    bulk_dir.mkdir(parents=True, exist_ok=True)
    for index in range(120):
        (bulk_dir / f"ledger_{index:03d}.csv").write_text(
            f"line,{index}\n",
            encoding="utf-8",
        )

    zip_path = create_packet_zip(
        packet_root=packet_root,
        on_conflict="abort",
        non_interactive=True,
    )

    with zipfile.ZipFile(zip_path, mode="r") as archive:
        names = set(archive.namelist())

    assert "Acme_2025_CPA_Packet/03_Full-Year_General_Ledger/ledger_000.csv" in names
    assert "Acme_2025_CPA_Packet/03_Full-Year_General_Ledger/ledger_119.csv" in names
