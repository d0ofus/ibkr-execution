"""Normalized broker event models and IBKR callback mapping."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import IntEnum, StrEnum


class BrokerEventType(StrEnum):
    """Normalized broker event types emitted by connector plumbing."""

    CONNECTION_OPENED = "connection_opened"
    CONNECTION_CLOSED = "connection_closed"
    HEARTBEAT = "heartbeat"
    HEARTBEAT_TIMEOUT = "heartbeat_timeout"
    MARKET_DATA_STALE = "market_data_stale"
    IB_ERROR = "ib_error"
    CONNECTIVITY_LOST = "connectivity_lost"
    CONNECTIVITY_RESTORED_DATA_LOST = "connectivity_restored_data_lost"
    CONNECTIVITY_RESTORED_DATA_MAINTAINED = "connectivity_restored_data_maintained"
    SOCKET_PORT_RESET = "socket_port_reset"
    UNKNOWN = "unknown"


class IbConnectivityCode(IntEnum):
    """IB connectivity-related error codes handled by reconnect logic."""

    CONNECTIVITY_LOST = 1100
    CONNECTIVITY_RESTORED_DATA_LOST = 1101
    CONNECTIVITY_RESTORED_DATA_MAINTAINED = 1102
    SOCKET_PORT_RESET = 1300


@dataclass(frozen=True)
class BrokerEvent:
    """Normalized broker event payload."""

    event_type: BrokerEventType
    timestamp: datetime
    payload: dict[str, object] = field(default_factory=dict)


class IbkrEventAdapter:
    """Translate raw IB callbacks into normalized broker events."""

    @staticmethod
    def _now() -> datetime:
        return datetime.now(tz=UTC)

    def normalize(self, event_type: BrokerEventType, payload: dict[str, object]) -> BrokerEvent:
        """Normalize an event payload with a consistent timestamp envelope."""
        return BrokerEvent(event_type=event_type, timestamp=self._now(), payload=payload)

    def from_ib_error(self, code: int, message: str) -> BrokerEvent:
        """Map IB error code into a normalized connectivity or generic error event."""
        mapped = self._map_error_code(code)
        payload: dict[str, object] = {"code": code, "message": message}
        return self.normalize(event_type=mapped, payload=payload)

    @staticmethod
    def _map_error_code(code: int) -> BrokerEventType:
        if code == IbConnectivityCode.CONNECTIVITY_LOST:
            return BrokerEventType.CONNECTIVITY_LOST
        if code == IbConnectivityCode.CONNECTIVITY_RESTORED_DATA_LOST:
            return BrokerEventType.CONNECTIVITY_RESTORED_DATA_LOST
        if code == IbConnectivityCode.CONNECTIVITY_RESTORED_DATA_MAINTAINED:
            return BrokerEventType.CONNECTIVITY_RESTORED_DATA_MAINTAINED
        if code == IbConnectivityCode.SOCKET_PORT_RESET:
            return BrokerEventType.SOCKET_PORT_RESET
        return BrokerEventType.IB_ERROR
