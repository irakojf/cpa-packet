from __future__ import annotations

import json
from pathlib import Path

from cpapacket.packet.structure import PacketStructureManager


def test_packet_dir_for_company_sanitizes_name() -> None:
    manager = PacketStructureManager(output_root=Path("/tmp/out"))

    packet_dir = manager.packet_dir_for_company(
        company_name=' ACME / North-East: "Holdings" ',
        year=2025,
    )

    assert packet_dir == Path("/tmp/out/ACME_North-East_Holdings_2025_CPA_Packet")


def test_packet_dir_from_company_info_uses_company_name(tmp_path: Path) -> None:
    manager = PacketStructureManager(output_root=tmp_path)
    payload = {"CompanyInfo": {"CompanyName": "Example Co"}}

    packet_dir = manager.packet_dir_from_company_info(company_info=payload, year=2024)

    assert packet_dir == tmp_path / "Example_Co_2024_CPA_Packet"


def test_packet_dir_from_company_info_uses_fixture_payload(tmp_path: Path) -> None:
    manager = PacketStructureManager(output_root=tmp_path)
    fixture_path = Path("tests/fixtures/qbo/company_info.json")
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))

    packet_dir = manager.packet_dir_from_company_info(company_info=payload, year=2025)

    assert packet_dir == tmp_path / "Northwind_Orchard_Holdings_LLC_2025_CPA_Packet"


def test_packet_dir_from_company_info_falls_back_to_untitled(tmp_path: Path) -> None:
    manager = PacketStructureManager(output_root=tmp_path)

    packet_dir = manager.packet_dir_from_company_info(company_info={}, year=2024)

    assert packet_dir == tmp_path / "untitled_2024_CPA_Packet"


def test_ensure_meta_directories_creates_public_and_private(tmp_path: Path) -> None:
    packet_root = tmp_path / "Example_2025_CPA_Packet"

    public_dir, private_dir = PacketStructureManager.ensure_meta_directories(packet_root)

    assert public_dir == packet_root / "_meta" / "public"
    assert private_dir == packet_root / "_meta" / "private"
    assert public_dir.exists()
    assert private_dir.exists()


def test_resolve_deliverable_dir_is_lazy_by_default(tmp_path: Path) -> None:
    packet_root = tmp_path / "Example_2025_CPA_Packet"

    target = PacketStructureManager.resolve_deliverable_dir(
        packet_root,
        deliverable_key="pnl",
        create=False,
    )

    assert target == packet_root / "01_Year-End_Profit_and_Loss"
    assert not target.exists()


def test_resolve_deliverable_dir_creates_when_requested(tmp_path: Path) -> None:
    packet_root = tmp_path / "Example_2025_CPA_Packet"

    target = PacketStructureManager.resolve_deliverable_dir(
        packet_root,
        deliverable_key="pnl",
        create=True,
    )

    assert target.exists()
    assert target.is_dir()
