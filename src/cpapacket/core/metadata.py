"""Per-deliverable metadata helpers and deterministic input fingerprinting."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import IO, Any, cast

from pydantic import BaseModel, ConfigDict, Field

from cpapacket.core.filesystem import atomic_write, ensure_directory


class DeliverableMetadata(BaseModel):
    """Structured metadata persisted for each generated deliverable."""

    model_config = ConfigDict(frozen=True)

    deliverable: str
    generated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    inputs: dict[str, Any]
    input_fingerprint: str
    schema_versions: dict[str, str]
    artifacts: list[str]
    warnings: list[str] = Field(default_factory=list)
    data_sources: dict[str, str] = Field(default_factory=dict)


def canonicalize_inputs(inputs: Mapping[str, Any]) -> str:
    """Serialize input mapping to a stable canonical JSON string."""
    return json.dumps(
        _canonicalize_value(dict(inputs)),
        sort_keys=True,
        separators=(",", ":"),
    )


def compute_input_fingerprint(inputs: Mapping[str, Any]) -> str:
    """Compute sha256 fingerprint for canonicalized deliverable inputs."""
    canonical = canonicalize_inputs(inputs)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def default_metadata_path(*, output_root: Path | str, deliverable_key: str) -> Path:
    """Return canonical private metadata location for a deliverable key."""
    root = Path(output_root)
    return root / "_meta" / "private" / "deliverables" / f"{deliverable_key}_metadata.json"


def write_deliverable_metadata(path: Path | str, metadata: DeliverableMetadata) -> None:
    """Persist metadata atomically as pretty JSON."""
    destination = Path(path)
    ensure_directory(destination.parent)
    payload = f"{metadata.model_dump_json(indent=2)}\n".encode()
    with atomic_write(destination, mode="wb") as handle:
        cast(IO[bytes], handle).write(payload)


def read_deliverable_metadata(path: Path | str) -> DeliverableMetadata:
    """Load and validate metadata payload from disk."""
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return DeliverableMetadata.model_validate(payload)


def _canonicalize_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _canonicalize_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_canonicalize_value(item) for item in value]
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return value
