"""Session-scoped data store with in-memory and optional disk-backed cache."""

from __future__ import annotations

import gzip
import json
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event, Lock
from typing import IO, Generic, TypeVar, cast

from cpapacket.core.filesystem import atomic_write, ensure_directory
from cpapacket.utils.constants import CACHE_TTL_HOURS

T = TypeVar("T")


@dataclass(slots=True)
class _InFlight(Generic[T]):
    event: Event = field(default_factory=Event)
    value: T | None = None
    error: BaseException | None = None


class SessionDataStore:
    """Store request payloads for a single run and avoid duplicate fetch calls."""

    def __init__(
        self,
        *,
        cache_dir: Path | str | None = None,
        ttl_hours: int = CACHE_TTL_HOURS,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        self._cache: dict[str, object] = {}
        self._inflight: dict[str, _InFlight[object]] = {}
        self._lock = Lock()
        self._ttl = timedelta(hours=ttl_hours)
        self._now = now_provider or (lambda: datetime.now(UTC))
        self._cache_dir = Path(cache_dir) if cache_dir is not None else None

        if self._cache_dir is not None:
            ensure_directory(self._cache_dir)
            self._warm_memory_cache_from_disk()

    def has(self, cache_key: str) -> bool:
        """Return True when cache_key is already present in memory cache."""
        with self._lock:
            return cache_key in self._cache

    def get(self, cache_key: str) -> object | None:
        """Return cached payload for key, or None when absent."""
        with self._lock:
            cached = self._cache.get(cache_key)
            if cached is not None:
                return cached

        loaded = self._load_from_disk(cache_key)
        if loaded is None:
            return None

        with self._lock:
            self._cache.setdefault(cache_key, loaded)
            return self._cache[cache_key]

    def set(self, cache_key: str, payload: object) -> None:
        """Insert or overwrite payload for key."""
        with self._lock:
            self._cache[cache_key] = payload
        self._persist_to_disk(cache_key, payload)

    def clear(self) -> None:
        """Clear memory cache and any stale in-flight entries."""
        with self._lock:
            self._cache.clear()
            self._inflight.clear()

    def get_or_fetch(self, cache_key: str, fetcher: Callable[[], T]) -> tuple[T, str]:
        """Get cached payload, otherwise fetch exactly once per key across threads.

        Returns tuple `(payload, source)` where `source` is `"cache"` or `"api"`.
        """
        cached = self.get(cache_key)
        if cached is not None:
            return cast(T, cached), "cache"

        with self._lock:
            cached = self._cache.get(cache_key)
            if cached is not None:
                return cast(T, cached), "cache"
            inflight = self._inflight.get(cache_key)
            if inflight is None:
                inflight = _InFlight[object]()
                self._inflight[cache_key] = inflight
                owner = True
            else:
                owner = False

        if owner:
            try:
                payload = fetcher()
            except BaseException as exc:  # pragma: no cover - re-raised for callers
                with self._lock:
                    inflight.error = exc
                    self._inflight.pop(cache_key, None)
                    inflight.event.set()
                raise

            with self._lock:
                inflight.value = payload
                self._inflight.pop(cache_key, None)
                inflight.event.set()
            self.set(cache_key, payload)
            return payload, "api"

        inflight.event.wait()
        if inflight.error is not None:
            raise inflight.error

        with self._lock:
            cached = self._cache.get(cache_key)
            if cached is None:
                raise RuntimeError(f"cache entry missing after inflight completion: {cache_key}")
            return cast(T, cached), "cache"

    def _warm_memory_cache_from_disk(self) -> None:
        cache_dir = self._cache_dir
        if cache_dir is None:
            return

        for meta_path in cache_dir.glob("*.meta.json"):
            cache_key = meta_path.name[: -len(".meta.json")]
            payload = self._load_from_disk(cache_key)
            if payload is None:
                continue
            self._cache[cache_key] = payload

    def _persist_to_disk(self, cache_key: str, payload: object) -> None:
        cache_dir = self._cache_dir
        if cache_dir is None:
            return

        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        payload_path = cache_dir / f"{cache_key}.json.gz"
        with atomic_write(payload_path, mode="wb") as handle:
            gzip_handle = gzip.GzipFile(
                fileobj=cast(IO[bytes], handle),
                mode="wb",
                mtime=0,
            )
            with gzip_handle as compressed:
                compressed.write(encoded)

        cached_at = self._now()
        metadata = {
            "cache_key": cache_key,
            "cached_at": cached_at.isoformat(),
            "expires_at": (cached_at + self._ttl).isoformat(),
            "ttl_seconds": int(self._ttl.total_seconds()),
        }
        meta_path = cache_dir / f"{cache_key}.meta.json"
        with atomic_write(meta_path) as handle:
            cast(IO[str], handle).write(json.dumps(metadata, indent=2, sort_keys=True))
            cast(IO[str], handle).write("\n")

    def _load_from_disk(self, cache_key: str) -> object | None:
        cache_dir = self._cache_dir
        if cache_dir is None:
            return None

        payload_path = cache_dir / f"{cache_key}.json.gz"
        meta_path = cache_dir / f"{cache_key}.meta.json"
        if not payload_path.exists() or not meta_path.exists():
            return None

        try:
            metadata = json.loads(meta_path.read_text(encoding="utf-8"))
            expires_at_raw = metadata.get("expires_at")
            if not isinstance(expires_at_raw, str):
                return None
            expires_at = datetime.fromisoformat(expires_at_raw)
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=UTC)
            if expires_at < self._now():
                return None

            with gzip.open(payload_path, mode="rt", encoding="utf-8") as handle:
                return cast(object, json.load(handle))
        except (OSError, ValueError, json.JSONDecodeError):
            return None
