"""Unit tests for broker pacing rate limiter."""

from __future__ import annotations

from app.broker.rate_limiter import PacingRateLimiter
from app.domain.errors import PacingLimitError


class _Clock:
    def __init__(self, start: float = 0.0) -> None:
        self.value = start

    def __call__(self) -> float:
        return self.value


def test_rate_limiter_blocks_when_window_capacity_exceeded() -> None:
    clock = _Clock(start=10.0)
    limiter = PacingRateLimiter(max_requests=3, window_seconds=1.0, clock=clock)

    limiter.acquire()
    limiter.acquire()
    limiter.acquire()

    try:
        limiter.acquire()
        assert False, "expected pacing limit error"
    except PacingLimitError:
        pass


def test_rate_limiter_recovers_after_window_rolls() -> None:
    clock = _Clock(start=5.0)
    limiter = PacingRateLimiter(max_requests=1, window_seconds=2.0, clock=clock)

    limiter.acquire()
    clock.value = 7.01
    limiter.acquire()

    assert limiter.remaining_capacity() == 0
