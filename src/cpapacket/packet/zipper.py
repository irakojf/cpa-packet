"""Packet zip archive creation helpers."""

from __future__ import annotations

import os
import uuid
import zipfile
from pathlib import Path

from cpapacket.utils.prompts import resolve_output_path


def create_packet_zip(
    *,
    packet_root: str | Path,
    on_conflict: str = "abort",
    non_interactive: bool = True,
    include_debug_log: bool = False,
) -> Path:
    """Create `<base_dir>/<PacketName>.zip` and return the written archive path."""
    root = Path(packet_root)
    if not root.exists():
        raise FileNotFoundError(f"packet root not found: {root}")
    if not root.is_dir():
        raise NotADirectoryError(f"packet root must be a directory: {root}")

    zip_target = root.parent / f"{root.name}.zip"
    normalized_conflict = None if on_conflict == "prompt" else on_conflict
    resolved_target = resolve_output_path(
        zip_target,
        on_conflict=normalized_conflict,
        non_interactive=non_interactive,
    )
    resolved_target.parent.mkdir(parents=True, exist_ok=True)

    tmp_target = resolved_target.with_name(f".{resolved_target.name}.{uuid.uuid4().hex}.tmp")
    try:
        with zipfile.ZipFile(
            tmp_target,
            mode="w",
            compression=zipfile.ZIP_DEFLATED,
        ) as archive:
            for file_path in sorted(root.rglob("*")):
                if not file_path.is_file():
                    continue
                relative_path = file_path.relative_to(root).as_posix()
                if relative_path.endswith(".tmp"):
                    continue
                if relative_path.startswith("_meta/private/"):
                    if not include_debug_log:
                        continue
                    if relative_path != "_meta/private/cpapacket.log":
                        continue
                archive_name = f"{root.name}/{relative_path}"
                archive.write(file_path, arcname=archive_name)

        os.replace(tmp_target, resolved_target)
    finally:
        if tmp_target.exists():
            tmp_target.unlink()

    return resolved_target
