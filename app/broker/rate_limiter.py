"""Simple deterministic rate limiter for broker pacing protection."""

from __future__ import annotations

from collections import deque
from collections.abc import Callable
import time

from app.domain.errors import PacingLimitError


class PacingRateLimiter:
    """Fixed-window request limiter used to avoid IB pacing violations."""

    def __init__(
        self,
        *,
        max_requests: int,
        window_seconds: float,
        clock: Callable[[], float] | None = None,
    ) -> None:
        if max_requests <= 0:
            raise ValueError("max_requests must be > 0")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be > 0")

        self._max_requests = max_requests
        self._window_seconds = window_seconds
        self._clock = clock or time.monotonic
        self._timestamps: deque[float] = deque()

    def acquire(self, tokens: int = 1) -> None:
        """Consume pacing tokens or raise if the request would exceed limits."""
        if tokens <= 0:
            raise ValueError("tokens must be > 0")
        if tokens > self._max_requests:
            raise PacingLimitError("requested tokens exceed max pacing bucket size")

        now = self._clock()
        self._prune(now)

        if len(self._timestamps) + tokens > self._max_requests:
            raise PacingLimitError("request pacing limit exceeded")

        for _ in range(tokens):
            self._timestamps.append(now)

    def remaining_capacity(self) -> int:
        """Return remaining capacity in the current window."""
        now = self._clock()
        self._prune(now)
        return self._max_requests - len(self._timestamps)

    def _prune(self, now: float) -> None:
        cutoff = now - self._window_seconds
        while self._timestamps and self._timestamps[0] <= cutoff:
            self._timestamps.popleft()
