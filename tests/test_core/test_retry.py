from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest

from cpapacket.core.retry import (
    RetryExhaustedError,
    RetryPolicy,
    compute_backoff_delay,
    parse_retry_after,
    retry_request,
    should_retry,
)


@dataclass(frozen=True)
class FakeResponse:
    status_code: int
    headers: dict[str, str]


def test_parse_retry_after_seconds_and_http_date() -> None:
    assert parse_retry_after("7") == 7.0

    now = datetime(2026, 3, 4, 0, 0, 0, tzinfo=UTC)
    retry_at = now + timedelta(seconds=5)
    header = retry_at.strftime("%a, %d %b %Y %H:%M:%S GMT")

    parsed = parse_retry_after(header, now=now)
    assert parsed is not None
    assert 4.0 <= parsed <= 5.0


def test_compute_backoff_delay_bounds() -> None:
    low = compute_backoff_delay(
        attempt=2, base_delay_seconds=1.0, jitter_ratio=0.25, rand=lambda: 0.0
    )
    high = compute_backoff_delay(
        attempt=2, base_delay_seconds=1.0, jitter_ratio=0.25, rand=lambda: 1.0
    )

    assert low == 1.5
    assert high == 2.5


def test_should_retry_only_429_and_5xx() -> None:
    assert should_retry(429)
    assert should_retry(500)
    assert should_retry(503)
    assert not should_retry(200)
    assert not should_retry(404)


def test_retry_request_retries_429_then_succeeds() -> None:
    calls = {"n": 0}
    slept: list[float] = []

    @retry_request(
        policy=RetryPolicy(max_429=2, max_5xx=0, base_delay_seconds=1.0, jitter_ratio=0.0),
        sleep=slept.append,
        rand=lambda: 0.5,
    )
    def operation() -> FakeResponse:
        calls["n"] += 1
        if calls["n"] == 1:
            return FakeResponse(status_code=429, headers={"Retry-After": "3"})
        return FakeResponse(status_code=200, headers={})

    result = operation()
    assert result.status_code == 200
    assert calls["n"] == 2
    assert slept == [3.0]


def test_retry_request_retries_5xx_until_success() -> None:
    calls = {"n": 0}
    slept: list[float] = []

    @retry_request(
        policy=RetryPolicy(max_429=0, max_5xx=2, base_delay_seconds=0.5, jitter_ratio=0.0),
        sleep=slept.append,
        rand=lambda: 0.5,
    )
    def operation() -> FakeResponse:
        calls["n"] += 1
        if calls["n"] <= 2:
            return FakeResponse(status_code=503, headers={})
        return FakeResponse(status_code=204, headers={})

    result = operation()
    assert result.status_code == 204
    assert calls["n"] == 3
    assert slept == [0.5, 1.0]


def test_retry_request_fail_fast_on_non_429_4xx() -> None:
    @retry_request(policy=RetryPolicy(max_429=2, max_5xx=2), sleep=lambda _: None)
    def operation() -> FakeResponse:
        return FakeResponse(status_code=401, headers={})

    with pytest.raises(RetryExhaustedError, match="status=401"):
        operation()


def test_retry_request_calls_token_check_before_each_attempt() -> None:
    checks = {"n": 0}
    calls = {"n": 0}

    def ensure_token_fresh() -> None:
        checks["n"] += 1

    @retry_request(
        policy=RetryPolicy(max_429=1, max_5xx=0, base_delay_seconds=1.0, jitter_ratio=0.0),
        sleep=lambda _: None,
        ensure_token_fresh=ensure_token_fresh,
    )
    def operation() -> FakeResponse:
        calls["n"] += 1
        if calls["n"] == 1:
            return FakeResponse(status_code=429, headers={})
        return FakeResponse(status_code=200, headers={})

    operation()
    assert checks["n"] == 2


# --- parse_retry_after edge cases ---


def test_parse_retry_after_none_returns_none() -> None:
    assert parse_retry_after(None) is None


def test_parse_retry_after_empty_string_returns_none() -> None:
    assert parse_retry_after("") is None
    assert parse_retry_after("   ") is None


def test_parse_retry_after_invalid_string_returns_none() -> None:
    assert parse_retry_after("not-a-date-or-number") is None


def test_parse_retry_after_zero_is_valid() -> None:
    assert parse_retry_after("0") == 0.0


def test_parse_retry_after_float_seconds() -> None:
    assert parse_retry_after("2.5") == 2.5


def test_parse_retry_after_past_http_date_returns_zero() -> None:
    now = datetime(2026, 3, 4, 12, 0, 0, tzinfo=UTC)
    past = now - timedelta(hours=1)
    header = past.strftime("%a, %d %b %Y %H:%M:%S GMT")
    result = parse_retry_after(header, now=now)
    assert result is not None
    assert result == 0.0


# --- compute_backoff_delay validation and progression ---


def test_compute_backoff_delay_rejects_invalid_attempt() -> None:
    with pytest.raises(ValueError, match="attempt must be >= 1"):
        compute_backoff_delay(attempt=0, base_delay_seconds=1.0, jitter_ratio=0.0)


def test_compute_backoff_delay_rejects_negative_base_delay() -> None:
    with pytest.raises(ValueError, match="base_delay_seconds must be > 0"):
        compute_backoff_delay(attempt=1, base_delay_seconds=-1.0, jitter_ratio=0.0)


def test_compute_backoff_delay_rejects_negative_jitter() -> None:
    with pytest.raises(ValueError, match="jitter_ratio must be >= 0"):
        compute_backoff_delay(attempt=1, base_delay_seconds=1.0, jitter_ratio=-0.1)


def test_compute_backoff_delay_exponential_progression() -> None:
    """Verify base delay doubles each attempt (zero jitter)."""
    delays = [
        compute_backoff_delay(attempt=a, base_delay_seconds=0.5, jitter_ratio=0.0, rand=lambda: 0.5)
        for a in range(1, 6)
    ]
    assert delays == [0.5, 1.0, 2.0, 4.0, 8.0]


def test_compute_backoff_delay_jitter_within_bounds() -> None:
    """Jitter output must stay within [base*(1-ratio), base*(1+ratio)]."""
    for attempt in range(1, 4):
        base = 1.0 * (2 ** (attempt - 1))
        ratio = 0.3
        for rand_val in [0.0, 0.25, 0.5, 0.75, 1.0]:
            delay = compute_backoff_delay(
                attempt=attempt,
                base_delay_seconds=1.0,
                jitter_ratio=ratio,
                rand=lambda r=rand_val: r,
            )
            assert base * (1 - ratio) <= delay <= base * (1 + ratio), (
                f"attempt={attempt} rand={rand_val} delay={delay}"
            )


def test_compute_backoff_delay_zero_jitter_is_deterministic() -> None:
    d1 = compute_backoff_delay(
        attempt=3, base_delay_seconds=2.0, jitter_ratio=0.0, rand=lambda: 0.0
    )
    d2 = compute_backoff_delay(
        attempt=3, base_delay_seconds=2.0, jitter_ratio=0.0, rand=lambda: 1.0
    )
    assert d1 == d2 == 8.0


# --- should_retry comprehensive ---


def test_should_retry_boundary_status_codes() -> None:
    assert not should_retry(428)
    assert should_retry(429)
    assert not should_retry(430)
    assert not should_retry(499)
    assert should_retry(500)
    assert should_retry(599)
    assert not should_retry(600)


# --- retry_request max exhaustion ---


def test_retry_429_exhaustion_raises() -> None:
    calls = {"n": 0}

    @retry_request(
        policy=RetryPolicy(max_429=2, max_5xx=5, base_delay_seconds=0.1, jitter_ratio=0.0),
        sleep=lambda _: None,
    )
    def operation() -> FakeResponse:
        calls["n"] += 1
        return FakeResponse(status_code=429, headers={})

    with pytest.raises(RetryExhaustedError, match="status=429") as exc_info:
        operation()
    assert exc_info.value.status_code == 429
    assert exc_info.value.attempts == 3  # 1 initial + 2 retries
    assert calls["n"] == 3


def test_retry_5xx_exhaustion_raises() -> None:
    calls = {"n": 0}

    @retry_request(
        policy=RetryPolicy(max_429=5, max_5xx=2, base_delay_seconds=0.1, jitter_ratio=0.0),
        sleep=lambda _: None,
    )
    def operation() -> FakeResponse:
        calls["n"] += 1
        return FakeResponse(status_code=502, headers={})

    with pytest.raises(RetryExhaustedError, match="status=502") as exc_info:
        operation()
    assert exc_info.value.status_code == 502
    assert exc_info.value.attempts == 3  # 1 initial + 2 retries
    assert calls["n"] == 3


def test_retry_429_without_retry_after_uses_backoff() -> None:
    """When no Retry-After header, 429 falls back to computed backoff."""
    calls = {"n": 0}
    slept: list[float] = []

    @retry_request(
        policy=RetryPolicy(max_429=2, max_5xx=0, base_delay_seconds=1.0, jitter_ratio=0.0),
        sleep=slept.append,
        rand=lambda: 0.5,
    )
    def operation() -> FakeResponse:
        calls["n"] += 1
        if calls["n"] <= 2:
            return FakeResponse(status_code=429, headers={})  # no Retry-After
        return FakeResponse(status_code=200, headers={})

    result = operation()
    assert result.status_code == 200
    assert slept == [1.0, 2.0]  # exponential backoff without jitter


def test_retry_mixed_429_then_5xx() -> None:
    """Separate budgets for 429 and 5xx."""
    calls = {"n": 0}
    slept: list[float] = []

    @retry_request(
        policy=RetryPolicy(max_429=1, max_5xx=1, base_delay_seconds=0.5, jitter_ratio=0.0),
        sleep=slept.append,
        rand=lambda: 0.5,
    )
    def operation() -> FakeResponse:
        calls["n"] += 1
        if calls["n"] == 1:
            return FakeResponse(status_code=429, headers={})
        if calls["n"] == 2:
            return FakeResponse(status_code=500, headers={})
        return FakeResponse(status_code=200, headers={})

    result = operation()
    assert result.status_code == 200
    assert calls["n"] == 3


def test_retry_returns_immediately_on_success() -> None:
    slept: list[float] = []

    @retry_request(
        policy=RetryPolicy(max_429=3, max_5xx=3),
        sleep=slept.append,
    )
    def operation() -> FakeResponse:
        return FakeResponse(status_code=200, headers={})

    result = operation()
    assert result.status_code == 200
    assert slept == []


def test_retry_default_policy() -> None:
    """retry_request works with default policy (no explicit policy argument)."""

    @retry_request(sleep=lambda _: None)
    def operation() -> FakeResponse:
        return FakeResponse(status_code=200, headers={})

    result = operation()
    assert result.status_code == 200
