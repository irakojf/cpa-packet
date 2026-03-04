"""Per-service concurrency limiter utilities."""

from __future__ import annotations

import threading
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator, Literal

from cpapacket.utils.constants import GUSTO_MAX_CONCURRENCY, QBO_MAX_CONCURRENCY

ServiceName = Literal["qbo", "gusto"]


@dataclass(frozen=True)
class LimiterConfig:
    """Concurrency limits for supported upstream services."""

    qbo_max: int = QBO_MAX_CONCURRENCY
    gusto_max: int = GUSTO_MAX_CONCURRENCY


class ServiceLimiter:
    """Bound concurrent API activity by service."""

    def __init__(self, *, config: LimiterConfig | None = None) -> None:
        self._config = config or LimiterConfig()
        if self._config.qbo_max < 1:
            raise ValueError("qbo_max must be >= 1")
        if self._config.gusto_max < 1:
            raise ValueError("gusto_max must be >= 1")

        self._limits: dict[str, int] = {
            "qbo": self._config.qbo_max,
            "gusto": self._config.gusto_max,
        }
        self._semaphores: dict[str, threading.BoundedSemaphore] = {
            name: threading.BoundedSemaphore(value=limit)
            for name, limit in self._limits.items()
        }

    def limit_for(self, service: ServiceName) -> int:
        return self._limits[service]

    @contextmanager
    def acquire(self, service: ServiceName, *, timeout: float | None = None) -> Iterator[None]:
        semaphore = self._semaphores.get(service)
        if semaphore is None:
            raise ValueError(f"unsupported service: {service}")

        acquired = semaphore.acquire(timeout=timeout)
        if not acquired:
            raise TimeoutError(f"timed out acquiring limiter for service={service}")

        try:
            yield
        finally:
            semaphore.release()
