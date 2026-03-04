"""JSON payload writer with optional redaction and atomic output semantics."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cpapacket.core.filesystem import atomic_write

REDACTED_VALUE = "[REDACTED]"
SENSITIVE_JSON_KEYS = {
    "access_token",
    "refresh_token",
    "token",
    "secret",
    "api_key",
    "ssn",
    "ein",
}


@dataclass(frozen=True)
class JsonWriterConfig:
    """Configuration for JSON serialization output."""

    indent: int = 2
    ensure_ascii: bool = False


class JsonWriter:
    """Write raw JSON payloads with optional redaction and skip mode."""

    def __init__(self, *, config: JsonWriterConfig | None = None) -> None:
        self._config = config or JsonWriterConfig()

    def write_payload(
        self,
        output_path: str | Path,
        *,
        payload: Any,
        no_raw: bool = False,
        redact: bool = False,
    ) -> Path | None:
        if no_raw:
            return None

        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        serializable_payload = redact_json_payload(payload) if redact else payload

        with atomic_write(output, mode="w", encoding="utf-8", newline="\n") as handle:
            json.dump(
                serializable_payload,
                handle,
                indent=self._config.indent,
                ensure_ascii=self._config.ensure_ascii,
            )
            handle.write("\n")
        return output


def redact_json_payload(payload: Any) -> Any:
    """Recursively redact known sensitive key names within a payload."""
    return _redact_value(payload)


def _redact_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        redacted: dict[str, Any] = {}
        for key, nested in value.items():
            key_name = str(key)
            if key_name.strip().lower() in SENSITIVE_JSON_KEYS:
                redacted[key_name] = REDACTED_VALUE
                continue
            redacted[key_name] = _redact_value(nested)
        return redacted
    if isinstance(value, list):
        return [_redact_value(item) for item in value]
    return value
