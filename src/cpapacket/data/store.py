"""Session-scoped in-memory data store with thread-safe request coalescing."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from threading import Event, Lock
from typing import Generic, TypeVar, cast

T = TypeVar("T")


@dataclass(slots=True)
class _InFlight(Generic[T]):
    event: Event = field(default_factory=Event)
    value: T | None = None
    error: BaseException | None = None


class SessionDataStore:
    """Store request payloads for a single run and avoid duplicate fetch calls."""

    def __init__(self) -> None:
        self._cache: dict[str, object] = {}
        self._inflight: dict[str, _InFlight[object]] = {}
        self._lock = Lock()

    def has(self, cache_key: str) -> bool:
        """Return True when cache_key is already present in memory cache."""
        with self._lock:
            return cache_key in self._cache

    def get(self, cache_key: str) -> object | None:
        """Return cached payload for key, or None when absent."""
        with self._lock:
            return self._cache.get(cache_key)

    def set(self, cache_key: str, payload: object) -> None:
        """Insert or overwrite payload for key."""
        with self._lock:
            self._cache[cache_key] = payload

    def clear(self) -> None:
        """Clear memory cache and any stale in-flight entries."""
        with self._lock:
            self._cache.clear()
            self._inflight.clear()

    def get_or_fetch(self, cache_key: str, fetcher: Callable[[], T]) -> tuple[T, str]:
        """Get cached payload, otherwise fetch exactly once per key across threads.

        Returns tuple `(payload, source)` where `source` is `"cache"` or `"api"`.
        """
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
                self._cache[cache_key] = payload
                inflight.value = payload
                self._inflight.pop(cache_key, None)
                inflight.event.set()
            return payload, "api"

        inflight.event.wait()
        if inflight.error is not None:
            raise inflight.error

        with self._lock:
            cached = self._cache.get(cache_key)
            if cached is None:
                raise RuntimeError(f"cache entry missing after inflight completion: {cache_key}")
            return cast(T, cached), "cache"
