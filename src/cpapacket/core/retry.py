"""Retry policy utilities and decorator for API calls."""

from __future__ import annotations

import random
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any, Protocol, TypeVar

from cpapacket.utils.constants import RETRY_MAX_5XX, RETRY_MAX_429

T = TypeVar("T", bound="ResponseLike")


class ResponseLike(Protocol):
    status_code: int
    headers: Mapping[str, str]


@dataclass(frozen=True)
class RetryPolicy:
    """Retry policy knobs for HTTP-like operations."""

    max_429: int = RETRY_MAX_429
    max_5xx: int = RETRY_MAX_5XX
    base_delay_seconds: float = 0.5
    jitter_ratio: float = 0.2


class RetryExhaustedError(RuntimeError):
    """Raised when retries are exhausted for retryable responses."""

    def __init__(self, status_code: int, attempts: int) -> None:
        msg = f"retry budget exhausted for status={status_code} after {attempts} attempts"
        super().__init__(msg)
        self.status_code = status_code
        self.attempts = attempts


def parse_retry_after(header_value: str | None, *, now: datetime | None = None) -> float | None:
    """Parse Retry-After header value into seconds, when valid."""
    if header_value is None:
        return None

    value = header_value.strip()
    if not value:
        return None

    try:
        seconds = float(value)
    except ValueError:
        seconds = -1.0

    if seconds >= 0:
        return seconds

    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError):
        return None

    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)

    reference = now or datetime.now(UTC)
    delta = (when - reference).total_seconds()
    return max(delta, 0.0)


def compute_backoff_delay(
    *,
    attempt: int,
    base_delay_seconds: float,
    jitter_ratio: float,
    rand: Callable[[], float] = random.random,
) -> float:
    """Compute exponential backoff delay with bounded symmetric jitter."""
    if attempt < 1:
        raise ValueError("attempt must be >= 1")
    if base_delay_seconds <= 0:
        raise ValueError("base_delay_seconds must be > 0")
    if jitter_ratio < 0:
        raise ValueError("jitter_ratio must be >= 0")

    base_delay = base_delay_seconds * (2 ** (attempt - 1))
    jitter_span = base_delay * jitter_ratio
    jitter_offset = (rand() * 2.0 - 1.0) * jitter_span
    return max(base_delay + jitter_offset, 0.0)


def should_retry(status_code: int) -> bool:
    """Return True when status code is covered by retry policy."""
    return status_code == 429 or 500 <= status_code <= 599


def retry_request(
    *,
    policy: RetryPolicy | None = None,
    sleep: Callable[[float], None] = time.sleep,
    rand: Callable[[], float] = random.random,
    ensure_token_fresh: Callable[[], None] | None = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator to retry API calls according to retry policy."""
    effective_policy = policy or RetryPolicy()

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        def wrapped(*args: Any, **kwargs: Any) -> T:
            retry_429 = 0
            retry_5xx = 0
            attempts = 0

            while True:
                if ensure_token_fresh is not None:
                    ensure_token_fresh()

                attempts += 1
                response = func(*args, **kwargs)
                status = response.status_code

                if status == 429:
                    if retry_429 >= effective_policy.max_429:
                        raise RetryExhaustedError(status_code=status, attempts=attempts)
                    retry_429 += 1
                    delay = parse_retry_after(response.headers.get("Retry-After"))
                    if delay is None:
                        delay = compute_backoff_delay(
                            attempt=retry_429,
                            base_delay_seconds=effective_policy.base_delay_seconds,
                            jitter_ratio=effective_policy.jitter_ratio,
                            rand=rand,
                        )
                    sleep(delay)
                    continue

                if 500 <= status <= 599:
                    if retry_5xx >= effective_policy.max_5xx:
                        raise RetryExhaustedError(status_code=status, attempts=attempts)
                    retry_5xx += 1
                    delay = compute_backoff_delay(
                        attempt=retry_5xx,
                        base_delay_seconds=effective_policy.base_delay_seconds,
                        jitter_ratio=effective_policy.jitter_ratio,
                        rand=rand,
                    )
                    sleep(delay)
                    continue

                if 400 <= status <= 499:
                    # Non-429 client errors are explicit fail-fast path.
                    raise RetryExhaustedError(status_code=status, attempts=attempts)

                return response

        return wrapped

    return decorator
