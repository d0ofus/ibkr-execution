"""Reconnect supervision policy, backoff, and connectivity event handling."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
import random

from app.broker.ibkr_events import IbConnectivityCode


class ReconnectState(StrEnum):
    """Broker connectivity state tracked by reconnect supervisor."""

    DISCONNECTED = "disconnected"
    RECONNECTING = "reconnecting"
    HEALTHY = "healthy"


class ReconnectAction(StrEnum):
    """Suggested action after a connectivity transition."""

    NONE = "none"
    RECONNECT = "reconnect"
    RECONCILE = "reconcile"
    RESUBSCRIBE_AND_RECONCILE = "resubscribe_and_reconcile"


@dataclass(frozen=True)
class ReconnectPolicy:
    """Backoff policy used by reconnect supervisor."""

    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 30.0
    jitter_seconds: float = 0.25


@dataclass(frozen=True)
class ReconnectDecision:
    """Reconnect decision emitted after handling a connectivity event."""

    state: ReconnectState
    action: ReconnectAction
    delay_seconds: float | None
    reason_code: int | None = None


class ReconnectSupervisor:
    """Track connectivity transitions and reconnect attempts."""

    def __init__(self, policy: ReconnectPolicy, *, rng: random.Random | None = None) -> None:
        self._policy = policy
        self._rng = rng or random.Random()
        self._state = ReconnectState.DISCONNECTED
        self._attempt_count = 0

    @property
    def state(self) -> ReconnectState:
        """Current reconnect supervisor state."""
        return self._state

    @property
    def attempt_count(self) -> int:
        """Number of reconnect attempts since last healthy state."""
        return self._attempt_count

    def on_disconnect(self, reason_code: int | None = None) -> ReconnectDecision:
        """Record broker disconnect and return reconnect action."""
        self._state = ReconnectState.DISCONNECTED
        self._attempt_count += 1
        return ReconnectDecision(
            state=self._state,
            action=ReconnectAction.RECONNECT,
            delay_seconds=self.next_delay_seconds(),
            reason_code=reason_code,
        )

    def on_reconnect(self, *, data_lost: bool, reason_code: int | None = None) -> ReconnectDecision:
        """Record successful reconnect and return reconciliation action."""
        self._state = ReconnectState.HEALTHY
        self._attempt_count = 0
        action = (
            ReconnectAction.RESUBSCRIBE_AND_RECONCILE
            if data_lost
            else ReconnectAction.RECONCILE
        )
        return ReconnectDecision(
            state=self._state,
            action=action,
            delay_seconds=None,
            reason_code=reason_code,
        )

    def on_reconnect_attempt_started(self) -> None:
        """Mark that reconnect work has started."""
        self._state = ReconnectState.RECONNECTING

    def next_delay_seconds(self) -> float:
        """Return the next reconnect delay with bounded jitter."""
        exponent = max(self._attempt_count - 1, 0)
        base = min(self._policy.max_delay_seconds, self._policy.base_delay_seconds * (2**exponent))
        unit_random = self._rng.random()
        jitter = (unit_random * 2.0 - 1.0) * self._policy.jitter_seconds
        return float(max(0.0, min(self._policy.max_delay_seconds, base + jitter)))

    def handle_ib_error_code(self, code: int) -> ReconnectDecision:
        """Handle IB connectivity error code and return reconnect decision."""
        if code == IbConnectivityCode.CONNECTIVITY_LOST:
            return self.on_disconnect(reason_code=code)
        if code == IbConnectivityCode.CONNECTIVITY_RESTORED_DATA_LOST:
            return self.on_reconnect(data_lost=True, reason_code=code)
        if code == IbConnectivityCode.CONNECTIVITY_RESTORED_DATA_MAINTAINED:
            return self.on_reconnect(data_lost=False, reason_code=code)
        if code == IbConnectivityCode.SOCKET_PORT_RESET:
            return self.on_disconnect(reason_code=code)

        return ReconnectDecision(
            state=self._state,
            action=ReconnectAction.NONE,
            delay_seconds=None,
            reason_code=code,
        )
