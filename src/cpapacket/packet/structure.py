"""Packet directory naming and structure helpers."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from cpapacket.core.filesystem import ensure_directory, sanitize_filesystem_name


class PacketStructureManager:
    """Derive and materialize packet directory paths."""

    def __init__(self, *, output_root: Path | str) -> None:
        self._output_root = Path(output_root)

    def packet_dir_for_company(self, *, company_name: str, year: int) -> Path:
        """Return packet root path for company/year without creating it."""
        safe_name = sanitize_filesystem_name(company_name)
        return self._output_root / f"{safe_name}_{year}_CPA_Packet"

    def packet_dir_from_company_info(self, *, company_info: Mapping[str, Any], year: int) -> Path:
        """Return packet root path using a QBO CompanyInfo payload."""
        company_name = _extract_company_name(company_info)
        return self.packet_dir_for_company(company_name=company_name, year=year)

    @staticmethod
    def ensure_meta_directories(packet_root: Path | str) -> tuple[Path, Path]:
        """Ensure `_meta/public` and `_meta/private` directories exist."""
        root = Path(packet_root)
        public_dir = ensure_directory(root / "_meta" / "public")
        private_dir = ensure_directory(root / "_meta" / "private")
        return public_dir, private_dir


def _extract_company_name(company_info: Mapping[str, Any]) -> str:
    payload = company_info.get("CompanyInfo")
    if isinstance(payload, Mapping):
        company_name = payload.get("CompanyName")
        if isinstance(company_name, str) and company_name.strip():
            return company_name.strip()
    return "untitled"
