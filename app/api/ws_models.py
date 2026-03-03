"""Typed websocket event envelopes for workspace streaming."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel


class WorkspaceRowSnapshot(BaseModel):
    """Serializable market-data snapshot for a workspace row."""

    row_id: str
    symbol: str
    sec_type: str
    exchange: str
    currency: str
    volume: int | None = None
    avg_volume: int | None = None
    avg_volume_at_time: int | None = None
    last: Decimal | None = None
    day_high: Decimal | None = None
    day_low: Decimal | None = None
    prev_day_low: Decimal | None = None
    close: Decimal | None = None
    change_pct: Decimal | None = None
    bid: Decimal | None = None
    ask: Decimal | None = None
    ask_below_high_pct: Decimal | None = None
    delayed: bool = False
    stale: bool = True
    updated_at: datetime | None = None


class WorkspaceInitialStatePayload(BaseModel):
    """Initial websocket state payload."""

    workspace_key: str
    rows: list[WorkspaceRowSnapshot]
    columns: list[str]
    connected: bool
    feed_healthy: bool


class MarketDataStatusPayload(BaseModel):
    """Feed status update payload."""

    connected: bool
    feed_healthy: bool
    reason: str | None = None


class ExecutionStatusPayload(BaseModel):
    """Execution/strategy status message payload."""

    scope: Literal["orb", "order"]
    row_id: str | None = None
    strategy_id: str | None = None
    state: str
    detail: str | None = None
    trade_id: str | None = None


class ErrorPayload(BaseModel):
    """Error event payload."""

    message: str


class WorkspaceEventEnvelope(BaseModel):
    """Generic websocket event wrapper."""

    event: Literal[
        "workspace.initial_state",
        "market_data.snapshot",
        "market_data.status",
        "execution.status",
        "error",
    ]
    payload: dict[str, Any]
