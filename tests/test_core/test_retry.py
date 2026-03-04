from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

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

    now = datetime(2026, 3, 4, 0, 0, 0, tzinfo=timezone.utc)
    retry_at = now + timedelta(seconds=5)
    header = retry_at.strftime("%a, %d %b %Y %H:%M:%S GMT")

    parsed = parse_retry_after(header, now=now)
    assert parsed is not None
    assert 4.0 <= parsed <= 5.0


def test_compute_backoff_delay_bounds() -> None:
    low = compute_backoff_delay(attempt=2, base_delay_seconds=1.0, jitter_ratio=0.25, rand=lambda: 0.0)
    high = compute_backoff_delay(attempt=2, base_delay_seconds=1.0, jitter_ratio=0.25, rand=lambda: 1.0)

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
