"""IBKR connector abstraction with heartbeat, reconnect, and event plumbing."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from app.broker.ibkr_events import BrokerEvent, BrokerEventType, IbkrEventAdapter
from app.broker.rate_limiter import PacingRateLimiter
from app.broker.reconnect import ReconnectAction, ReconnectDecision, ReconnectSupervisor
from app.domain.errors import BrokerConnectivityError
from app.domain.models import BrokerOrderSpec, ContractRef, MarketBar


class IbkrTransport(Protocol):
    """Transport interface for socket operations against IBKR."""

    def connect(self) -> None:
        """Connect socket transport to IBKR."""

    def disconnect(self) -> None:
        """Disconnect socket transport from IBKR."""

    def is_connected(self) -> bool:
        """Return transport-level connection status."""

    def place_orders(self, contract: ContractRef, orders: list[BrokerOrderSpec]) -> list[str]:
        """Transmit one or more orders to the broker."""

    def cancel_order(self, broker_order_id: str) -> None:
        """Cancel an existing broker order."""

    def request_open_orders(self) -> None:
        """Request open orders for reconciliation."""

    def request_positions(self) -> None:
        """Request positions for reconciliation."""

    def request_executions(self, since: datetime | None = None) -> None:
        """Request executions for reconciliation."""

    def subscribe_bars(self, contract: ContractRef, bar_size: str) -> None:
        """Subscribe to bar market data."""

    def subscribe_quote(self, contract: ContractRef) -> str:
        """Subscribe quote stream and return broker subscription identifier."""

    def unsubscribe_quote(self, contract_or_sub_id: ContractRef | str) -> None:
        """Unsubscribe quote stream for the given contract or subscription ID."""

    def request_historical_daily(self, contract: ContractRef, *, sessions: int) -> list[MarketBar]:
        """Fetch historical daily bars for the contract."""

    def request_historical_intraday(
        self,
        contract: ContractRef,
        *,
        sessions: int,
        bar_size: str,
    ) -> list[MarketBar]:
        """Fetch historical intraday bars for the contract."""

    def set_connection_params(self, *, host: str, port: int, client_id: int) -> None:
        """Update transport connection params for the next connect attempt."""


class BrokerClient(Protocol):
    """Protocol for broker client interactions used by OrderManager."""

    def connect(self) -> None:
        """Open broker connection."""

    def disconnect(self) -> None:
        """Close broker connection."""

    def is_connected(self) -> bool:
        """Return whether broker connection is healthy."""

    def place_orders(self, contract: ContractRef, orders: list[BrokerOrderSpec]) -> list[str]:
        """Transmit one or more orders and return broker order IDs."""

    def cancel_order(self, broker_order_id: str) -> None:
        """Cancel an existing broker order."""

    def request_open_orders(self) -> None:
        """Request current open orders for reconciliation."""

    def request_positions(self) -> None:
        """Request current positions for reconciliation."""

    def request_executions(self, since: datetime | None = None) -> None:
        """Request executions since optional timestamp for reconciliation."""

    def subscribe_bars(self, contract: ContractRef, bar_size: str) -> None:
        """Subscribe to bar market data for a qualified contract."""

    def subscribe_quote(self, contract: ContractRef) -> str:
        """Subscribe quote stream and return broker subscription identifier."""

    def unsubscribe_quote(self, contract_or_sub_id: ContractRef | str) -> None:
        """Unsubscribe quote stream for the given contract or subscription ID."""

    def request_historical_daily(self, contract: ContractRef, *, sessions: int) -> list[MarketBar]:
        """Fetch historical daily bars for the contract."""

    def request_historical_intraday(
        self,
        contract: ContractRef,
        *,
        sessions: int,
        bar_size: str,
    ) -> list[MarketBar]:
        """Fetch historical intraday bars for the contract."""

    def switch_connection_profile(self, *, host: str, port: int, client_id: int) -> None:
        """Reconnect using a different socket connection profile."""


@dataclass(frozen=True)
class HeartbeatStatus:
    """Current heartbeat status of broker connectivity."""

    is_healthy: bool
    last_heartbeat_at: datetime | None
    timeout_seconds: float


class IbkrClient:
    """Socket-backed IBKR client with reconnect and reconciliation plumbing."""

    def __init__(
        self,
        transport: IbkrTransport,
        reconnect_supervisor: ReconnectSupervisor,
        *,
        event_adapter: IbkrEventAdapter | None = None,
        heartbeat_timeout_seconds: float = 30.0,
        rate_limiter: PacingRateLimiter | None = None,
    ) -> None:
        self._transport = transport
        self._reconnect_supervisor = reconnect_supervisor
        self._event_adapter = event_adapter or IbkrEventAdapter()
        self._heartbeat_timeout_seconds = heartbeat_timeout_seconds
        self._rate_limiter = rate_limiter
        self._last_heartbeat_at: datetime | None = None
        self._event_handlers: list[Callable[[BrokerEvent], None]] = []
        self._bar_subscriptions: dict[int, str] = {}

    @staticmethod
    def _now() -> datetime:
        return datetime.now(tz=UTC)

    @property
    def last_heartbeat_at(self) -> datetime | None:
        """Return timestamp of most recent heartbeat update."""
        return self._last_heartbeat_at

    def register_event_handler(self, handler: Callable[[BrokerEvent], None]) -> None:
        """Register event callback for normalized connector events."""
        self._event_handlers.append(handler)

    def connect(self) -> None:
        """Open broker connection and initialize heartbeat state."""
        self._reconnect_supervisor.on_reconnect_attempt_started()
        self._transport.connect()
        self._last_heartbeat_at = self._now()
        self._emit_event(BrokerEventType.CONNECTION_OPENED, {})

    def disconnect(self) -> None:
        """Close broker connection and clear heartbeat state."""
        self._transport.disconnect()
        self._last_heartbeat_at = None
        self._emit_event(BrokerEventType.CONNECTION_CLOSED, {})

    def is_connected(self) -> bool:
        """Return whether broker connection is healthy."""
        return self._transport.is_connected()

    def place_orders(self, contract: ContractRef, orders: list[BrokerOrderSpec]) -> list[str]:
        """Transmit one or more orders and return broker order IDs."""
        if not self.is_connected():
            raise BrokerConnectivityError("Cannot place orders while broker is disconnected.")
        self._acquire_pacing(tokens=max(1, len(orders)))
        return self._transport.place_orders(contract, orders)

    def cancel_order(self, broker_order_id: str) -> None:
        """Cancel an existing broker order."""
        self._acquire_pacing(tokens=1)
        self._transport.cancel_order(broker_order_id)

    def request_open_orders(self) -> None:
        """Request current open orders for reconciliation."""
        self._acquire_pacing(tokens=1)
        self._transport.request_open_orders()

    def request_positions(self) -> None:
        """Request current positions for reconciliation."""
        self._acquire_pacing(tokens=1)
        self._transport.request_positions()

    def request_executions(self, since: datetime | None = None) -> None:
        """Request executions since optional timestamp for reconciliation."""
        self._acquire_pacing(tokens=1)
        self._transport.request_executions(since=since)

    def subscribe_bars(self, contract: ContractRef, bar_size: str = "1 min") -> None:
        """Subscribe to bar market data for a qualified contract."""
        normalized = bar_size.strip()
        if not normalized:
            raise ValueError("bar_size cannot be empty")
        self._acquire_pacing(tokens=1)
        self._bar_subscriptions[contract.con_id] = normalized
        self._transport.subscribe_bars(contract, normalized)

    def subscribe_quote(self, contract: ContractRef) -> str:
        """Subscribe quote market data for a qualified contract."""
        self._acquire_pacing(tokens=1)
        return self._transport.subscribe_quote(contract)

    def unsubscribe_quote(self, contract_or_sub_id: ContractRef | str) -> None:
        """Unsubscribe quote market data for a contract or subscription ID."""
        self._acquire_pacing(tokens=1)
        self._transport.unsubscribe_quote(contract_or_sub_id)

    def request_historical_daily(self, contract: ContractRef, *, sessions: int) -> list[MarketBar]:
        """Request historical daily bars for bootstrap calculations."""
        if sessions <= 0:
            raise ValueError("sessions must be positive")
        self._acquire_pacing(tokens=1)
        return self._transport.request_historical_daily(contract, sessions=sessions)

    def request_historical_intraday(
        self,
        contract: ContractRef,
        *,
        sessions: int,
        bar_size: str = "1 min",
    ) -> list[MarketBar]:
        """Request historical intraday bars for bootstrap calculations."""
        if sessions <= 0:
            raise ValueError("sessions must be positive")
        normalized_bar_size = bar_size.strip()
        if not normalized_bar_size:
            raise ValueError("bar_size cannot be empty")
        self._acquire_pacing(tokens=1)
        return self._transport.request_historical_intraday(
            contract,
            sessions=sessions,
            bar_size=normalized_bar_size,
        )

    def switch_connection_profile(self, *, host: str, port: int, client_id: int) -> None:
        """Reconnect using a different socket connection profile."""
        was_connected = self.is_connected()
        if was_connected:
            self.disconnect()
        self._transport.set_connection_params(host=host, port=port, client_id=client_id)
        self.connect()

    def heartbeat(self, *, timestamp: datetime | None = None) -> None:
        """Record broker heartbeat signal."""
        self._last_heartbeat_at = timestamp or self._now()
        self._emit_event(
            BrokerEventType.HEARTBEAT,
            {"last_heartbeat_at": self._last_heartbeat_at.isoformat()},
        )

    def heartbeat_status(self, *, now: datetime | None = None) -> HeartbeatStatus:
        """Return current heartbeat health projection."""
        current = now or self._now()
        if self._last_heartbeat_at is None:
            return HeartbeatStatus(
                is_healthy=False,
                last_heartbeat_at=None,
                timeout_seconds=self._heartbeat_timeout_seconds,
            )

        delta = current - self._last_heartbeat_at
        is_healthy = delta <= timedelta(seconds=self._heartbeat_timeout_seconds)
        return HeartbeatStatus(
            is_healthy=is_healthy,
            last_heartbeat_at=self._last_heartbeat_at,
            timeout_seconds=self._heartbeat_timeout_seconds,
        )

    def check_heartbeat(self, *, now: datetime | None = None) -> bool:
        """Validate heartbeat freshness and emit timeout event when stale."""
        status = self.heartbeat_status(now=now)
        if status.is_healthy:
            return True

        if self.is_connected():
            decision = self._reconnect_supervisor.on_disconnect()
            self._emit_event(
                BrokerEventType.HEARTBEAT_TIMEOUT,
                {"delay_seconds": decision.delay_seconds},
            )
            self.disconnect()

        return False

    def handle_market_data_stale(
        self,
        *,
        checked_at: datetime,
        last_bar_at: datetime | None,
    ) -> ReconnectDecision:
        """Handle watchdog stale-market-data signal by triggering reconnect path."""
        decision = self._reconnect_supervisor.on_disconnect()
        self._emit_event(
            BrokerEventType.MARKET_DATA_STALE,
            {
                "checked_at": checked_at.isoformat(),
                "last_bar_at": last_bar_at.isoformat() if last_bar_at is not None else None,
                "delay_seconds": decision.delay_seconds,
            },
        )
        if self.is_connected():
            self.disconnect()
        return decision

    def handle_error_code(self, code: int, message: str) -> ReconnectDecision:
        """Handle IB error code and apply reconnect/reconciliation hooks."""
        error_event = self._event_adapter.from_ib_error(code=code, message=message)
        self._dispatch_event(error_event)

        decision = self._reconnect_supervisor.handle_ib_error_code(code)

        if decision.action == ReconnectAction.RECONNECT:
            if self.is_connected():
                self.disconnect()
        elif decision.action in {
            ReconnectAction.RECONCILE,
            ReconnectAction.RESUBSCRIBE_AND_RECONCILE,
        }:
            self._run_reconciliation_hooks()

        return decision

    def _run_reconciliation_hooks(self) -> None:
        self.request_open_orders()
        self.request_positions()
        self.request_executions(since=None)

    def _emit_event(self, event_type: BrokerEventType, payload: dict[str, object]) -> None:
        event = self._event_adapter.normalize(event_type=event_type, payload=payload)
        self._dispatch_event(event)

    def _dispatch_event(self, event: BrokerEvent) -> None:
        for handler in self._event_handlers:
            handler(event)

    def _acquire_pacing(self, *, tokens: int) -> None:
        if self._rate_limiter is None:
            return
        self._rate_limiter.acquire(tokens=tokens)
