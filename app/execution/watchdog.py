"""Operational watchdogs for market-data freshness."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from app.data.calendar import is_us_equity_market_hours


def _utc_now() -> datetime:
    return datetime.now(tz=UTC)


def _to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


@dataclass(frozen=True)
class WatchdogStatus:
    """State snapshot for market-data freshness checks."""

    stale: bool
    last_bar_at: datetime | None
    checked_at: datetime
    stale_after_seconds: float


class MarketDataWatchdog:
    """Detect missing bars during market hours and trigger reconnect callback."""

    def __init__(
        self,
        *,
        stale_after_seconds: float,
        on_stale: Callable[[WatchdogStatus], None],
        now_provider: Callable[[], datetime] | None = None,
        market_hours_checker: Callable[[datetime], bool] = is_us_equity_market_hours,
    ) -> None:
        if stale_after_seconds <= 0:
            raise ValueError("stale_after_seconds must be > 0")

        self._stale_after = stale_after_seconds
        self._on_stale = on_stale
        self._now_provider = now_provider or _utc_now
        self._market_hours_checker = market_hours_checker
        self._last_bar_at: datetime | None = None
        self._alerted_for_current_stale_window = False

    def observe_bar(self, bar_timestamp: datetime) -> None:
        """Record the timestamp of a newly observed market data bar."""
        self._last_bar_at = _to_utc(bar_timestamp)
        self._alerted_for_current_stale_window = False

    def check(self, *, now: datetime | None = None) -> WatchdogStatus:
        """Evaluate freshness and trigger callback once per stale window."""
        checked_at = _to_utc(now or self._now_provider())

        if not self._market_hours_checker(checked_at):
            self._alerted_for_current_stale_window = False
            return WatchdogStatus(
                stale=False,
                last_bar_at=self._last_bar_at,
                checked_at=checked_at,
                stale_after_seconds=self._stale_after,
            )

        stale = self._is_stale(checked_at)
        status = WatchdogStatus(
            stale=stale,
            last_bar_at=self._last_bar_at,
            checked_at=checked_at,
            stale_after_seconds=self._stale_after,
        )

        if stale and not self._alerted_for_current_stale_window:
            self._on_stale(status)
            self._alerted_for_current_stale_window = True
        elif not stale:
            self._alerted_for_current_stale_window = False

        return status

    def _is_stale(self, now: datetime) -> bool:
        if self._last_bar_at is None:
            return True
        return now - self._last_bar_at > timedelta(seconds=self._stale_after)
