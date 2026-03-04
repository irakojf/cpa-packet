from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from cpapacket.core.limiter import LimiterConfig, ServiceLimiter


def test_limiter_rejects_invalid_limits() -> None:
    with pytest.raises(ValueError, match="qbo_max"):
        ServiceLimiter(config=LimiterConfig(qbo_max=0, gusto_max=1))
    with pytest.raises(ValueError, match="gusto_max"):
        ServiceLimiter(config=LimiterConfig(qbo_max=1, gusto_max=0))


def test_limiter_rejects_unknown_service() -> None:
    limiter = ServiceLimiter(config=LimiterConfig(qbo_max=1, gusto_max=1))
    with pytest.raises(ValueError, match="unsupported service"):
        with limiter.acquire("stripe"):  # type: ignore[arg-type]
            pass


def test_limiter_timeout_when_capacity_exhausted() -> None:
    limiter = ServiceLimiter(config=LimiterConfig(qbo_max=1, gusto_max=1))

    with limiter.acquire("qbo"):
        with pytest.raises(TimeoutError, match="timed out"):
            with limiter.acquire("qbo", timeout=0.01):
                pass


def test_limiter_enforces_qbo_concurrency_bound() -> None:
    limiter = ServiceLimiter(config=LimiterConfig(qbo_max=2, gusto_max=1))

    active = 0
    peak = 0
    lock = threading.Lock()

    def worker() -> None:
        nonlocal active, peak
        with limiter.acquire("qbo"):
            with lock:
                active += 1
                peak = max(peak, active)
            time.sleep(0.01)
            with lock:
                active -= 1

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(worker) for _ in range(20)]
        for future in futures:
            future.result()

    assert peak <= 2
