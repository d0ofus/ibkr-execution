"""Unit tests for market data watchdog behavior."""

from __future__ import annotations

from datetime import UTC, datetime

from app.execution.watchdog import MarketDataWatchdog, WatchdogStatus


def test_watchdog_triggers_once_when_stale_during_market_hours() -> None:
    triggered: list[WatchdogStatus] = []
    watchdog = MarketDataWatchdog(
        stale_after_seconds=30,
        on_stale=lambda status: triggered.append(status),
    )

    check_time = datetime(2026, 1, 5, 15, 0, 0, tzinfo=UTC)
    status_1 = watchdog.check(now=check_time)
    status_2 = watchdog.check(now=check_time)

    assert status_1.stale is True
    assert status_2.stale is True
    assert len(triggered) == 1


def test_watchdog_resets_alert_after_new_bar_observed() -> None:
    triggered: list[WatchdogStatus] = []
    watchdog = MarketDataWatchdog(
        stale_after_seconds=30,
        on_stale=lambda status: triggered.append(status),
    )

    stale_time = datetime(2026, 1, 5, 15, 0, 0, tzinfo=UTC)
    _ = watchdog.check(now=stale_time)
    assert len(triggered) == 1

    watchdog.observe_bar(datetime(2026, 1, 5, 15, 0, 10, tzinfo=UTC))
    healthy_status = watchdog.check(now=datetime(2026, 1, 5, 15, 0, 20, tzinfo=UTC))
    assert healthy_status.stale is False

    stale_again = watchdog.check(now=datetime(2026, 1, 5, 15, 1, 0, tzinfo=UTC))
    assert stale_again.stale is True
    assert len(triggered) == 2


def test_watchdog_no_alert_outside_market_hours() -> None:
    triggered: list[WatchdogStatus] = []
    watchdog = MarketDataWatchdog(
        stale_after_seconds=30,
        on_stale=lambda status: triggered.append(status),
    )

    overnight = datetime(2026, 1, 5, 2, 0, 0, tzinfo=UTC)
    status = watchdog.check(now=overnight)

    assert status.stale is False
    assert triggered == []
