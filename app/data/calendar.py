"""US market session calendar helpers."""

from __future__ import annotations

from datetime import UTC, datetime, time


def is_us_equity_market_hours(timestamp: datetime) -> bool:
    """Return whether timestamp falls in regular US equity market hours (UTC-based)."""
    value = timestamp if timestamp.tzinfo is not None else timestamp.replace(tzinfo=UTC)
    utc_value = value.astimezone(UTC)

    if utc_value.weekday() >= 5:
        return False

    market_open_utc = time(14, 30)
    market_close_utc = time(21, 0)
    current_time = utc_value.time()
    return market_open_utc <= current_time < market_close_utc
