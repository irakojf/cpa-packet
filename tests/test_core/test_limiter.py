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
    with (
        pytest.raises(ValueError, match="unsupported service"),
        limiter.acquire("stripe"),  # type: ignore[arg-type]
    ):
        pass


def test_limiter_timeout_when_capacity_exhausted() -> None:
    limiter = ServiceLimiter(config=LimiterConfig(qbo_max=1, gusto_max=1))

    with (
        limiter.acquire("qbo"),
        pytest.raises(TimeoutError, match="timed out"),
        limiter.acquire("qbo", timeout=0.01),
    ):
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


def test_limiter_enforces_service_specific_limits() -> None:
    limiter = ServiceLimiter(config=LimiterConfig(qbo_max=3, gusto_max=1))

    with (
        limiter.acquire("gusto"),
        pytest.raises(TimeoutError, match="timed out"),
        limiter.acquire("gusto", timeout=0.01),
    ):
        pass

    # QBO should still allow up to its own configured bound independently.
    with limiter.acquire("qbo"), limiter.acquire("qbo"), limiter.acquire("qbo"):
        assert True


def test_limiter_releases_slot_when_exception_occurs() -> None:
    limiter = ServiceLimiter(config=LimiterConfig(qbo_max=1, gusto_max=1))

    with pytest.raises(RuntimeError, match="boom"), limiter.acquire("qbo"):
        raise RuntimeError("boom")

    # If the slot was not released in __exit__, this acquire would time out.
    with limiter.acquire("qbo", timeout=0.05):
        assert True


def test_limiter_enforces_gusto_concurrency_bound() -> None:
    """Threaded test: gusto peak concurrency respects configured limit."""
    limiter = ServiceLimiter(config=LimiterConfig(qbo_max=4, gusto_max=1))

    active = 0
    peak = 0
    lock = threading.Lock()

    def worker() -> None:
        nonlocal active, peak
        with limiter.acquire("gusto"):
            with lock:
                active += 1
                peak = max(peak, active)
            time.sleep(0.01)
            with lock:
                active -= 1

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(worker) for _ in range(10)]
        for future in futures:
            future.result()

    assert peak <= 1


def test_limiter_services_are_independent() -> None:
    """Holding QBO slot does not block Gusto and vice versa."""
    limiter = ServiceLimiter(config=LimiterConfig(qbo_max=1, gusto_max=1))

    with limiter.acquire("qbo"), limiter.acquire("gusto", timeout=0.1):
        pass


def test_limiter_limit_for_returns_configured_values() -> None:
    limiter = ServiceLimiter(config=LimiterConfig(qbo_max=5, gusto_max=2))
    assert limiter.limit_for("qbo") == 5
    assert limiter.limit_for("gusto") == 2


def test_limiter_default_config_uses_constants() -> None:
    from cpapacket.utils.constants import GUSTO_MAX_CONCURRENCY, QBO_MAX_CONCURRENCY

    limiter = ServiceLimiter()
    assert limiter.limit_for("qbo") == QBO_MAX_CONCURRENCY
    assert limiter.limit_for("gusto") == GUSTO_MAX_CONCURRENCY
