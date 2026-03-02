"""Unit tests for US equity market-hours helper."""

from __future__ import annotations

from datetime import UTC, datetime

from app.data.calendar import is_us_equity_market_hours


def test_market_hours_weekday_open_close_bounds() -> None:
    monday_open = datetime(2026, 1, 5, 14, 30, 0, tzinfo=UTC)
    monday_mid = datetime(2026, 1, 5, 17, 0, 0, tzinfo=UTC)
    monday_close = datetime(2026, 1, 5, 21, 0, 0, tzinfo=UTC)

    assert is_us_equity_market_hours(monday_open) is True
    assert is_us_equity_market_hours(monday_mid) is True
    assert is_us_equity_market_hours(monday_close) is False


def test_market_hours_excludes_weekend() -> None:
    saturday = datetime(2026, 1, 3, 15, 0, 0, tzinfo=UTC)
    assert is_us_equity_market_hours(saturday) is False
