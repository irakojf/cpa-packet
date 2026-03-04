"""Cache key canonicalization and hashing helpers."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal
from hashlib import sha256
from typing import Any


def canonical_json(payload: dict[str, Any]) -> str:
    """Serialize payload to deterministic JSON for cache-key hashing."""
    normalized = _normalize_value(payload)
    return json.dumps(normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def build_cache_key(
    *,
    source: str,
    endpoint: str,
    params: dict[str, Any],
    schema: str,
    cache_version: str,
) -> str:
    """Return sha256 cache key from canonicalized cache-key inputs."""
    canonical = canonical_json(
        {
            "source": source,
            "endpoint": endpoint,
            "params": params,
            "schema": schema,
            "cache_version": cache_version,
        }
    )
    return sha256(canonical.encode("utf-8")).hexdigest()


def _normalize_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _normalize_value(value[key]) for key in sorted(value)}

    if isinstance(value, (list, tuple)):
        return [_normalize_value(item) for item in value]

    if isinstance(value, datetime):
        dt = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, Decimal):
        return format(value, "f")

    if isinstance(value, float):
        raise TypeError("float values are not allowed in cache key inputs")

    if isinstance(value, str):
        return _normalize_date_string(value)

    return value


def _normalize_date_string(value: str) -> str:
    stripped = value.strip()

    if len(stripped) == 10:
        try:
            return date.fromisoformat(stripped).isoformat()
        except ValueError:
            return stripped

    try:
        dt = datetime.fromisoformat(stripped.replace("Z", "+00:00"))
    except ValueError:
        return stripped

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
