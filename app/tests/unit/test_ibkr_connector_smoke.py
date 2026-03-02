"""Smoke tests for IBKR connector, reconnect supervisor, and event plumbing."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import random

import pytest

from app.broker.ibkr_client import IbkrClient
from app.broker.ibkr_events import BrokerEventType, IbkrEventAdapter
from app.broker.rate_limiter import PacingRateLimiter
from app.broker.reconnect import ReconnectAction, ReconnectPolicy, ReconnectState, ReconnectSupervisor
from app.domain.errors import PacingLimitError
from app.domain.models import BrokerOrderSpec, ContractRef


class FakeTransport:
    """In-memory transport mock for connector smoke tests."""

    def __init__(self) -> None:
        self.connected = False
        self.request_open_orders_calls = 0
        self.request_positions_calls = 0
        self.request_executions_calls = 0
        self.subscriptions: list[tuple[ContractRef, str]] = []

    def connect(self) -> None:
        self.connected = True

    def disconnect(self) -> None:
        self.connected = False

    def is_connected(self) -> bool:
        return self.connected

    def place_orders(self, contract: ContractRef, orders: list[BrokerOrderSpec]) -> list[str]:
        _ = (contract, orders)
        return ["broker-order-1"]

    def cancel_order(self, broker_order_id: str) -> None:
        _ = broker_order_id

    def request_open_orders(self) -> None:
        self.request_open_orders_calls += 1

    def request_positions(self) -> None:
        self.request_positions_calls += 1

    def request_executions(self, since: datetime | None = None) -> None:
        _ = since
        self.request_executions_calls += 1

    def subscribe_bars(self, contract: ContractRef, bar_size: str) -> None:
        self.subscriptions.append((contract, bar_size))


def _build_client(transport: FakeTransport | None = None) -> tuple[IbkrClient, FakeTransport, ReconnectSupervisor]:
    fake = transport or FakeTransport()
    supervisor = ReconnectSupervisor(
        ReconnectPolicy(base_delay_seconds=1.0, max_delay_seconds=10.0, jitter_seconds=0.0),
        rng=random.Random(7),
    )
    client = IbkrClient(
        transport=fake,
        reconnect_supervisor=supervisor,
        event_adapter=IbkrEventAdapter(),
        heartbeat_timeout_seconds=2.0,
    )
    return client, fake, supervisor


def test_event_adapter_maps_connectivity_codes() -> None:
    adapter = IbkrEventAdapter()

    assert adapter.from_ib_error(1100, "lost").event_type == BrokerEventType.CONNECTIVITY_LOST
    assert (
        adapter.from_ib_error(1101, "restored data lost").event_type
        == BrokerEventType.CONNECTIVITY_RESTORED_DATA_LOST
    )
    assert (
        adapter.from_ib_error(1102, "restored").event_type
        == BrokerEventType.CONNECTIVITY_RESTORED_DATA_MAINTAINED
    )
    assert adapter.from_ib_error(1300, "socket reset").event_type == BrokerEventType.SOCKET_PORT_RESET


def test_reconnect_supervisor_handles_connectivity_error_codes() -> None:
    supervisor = ReconnectSupervisor(
        ReconnectPolicy(base_delay_seconds=1.0, max_delay_seconds=10.0, jitter_seconds=0.0),
        rng=random.Random(1),
    )

    decision_1100 = supervisor.handle_ib_error_code(1100)
    assert decision_1100.action == ReconnectAction.RECONNECT
    assert decision_1100.delay_seconds == 1.0
    assert supervisor.state == ReconnectState.DISCONNECTED

    supervisor.on_reconnect_attempt_started()
    assert supervisor.state == ReconnectState.RECONNECTING

    decision_1101 = supervisor.handle_ib_error_code(1101)
    assert decision_1101.action == ReconnectAction.RESUBSCRIBE_AND_RECONCILE
    assert decision_1101.delay_seconds is None
    assert supervisor.state == ReconnectState.HEALTHY

    decision_1300 = supervisor.handle_ib_error_code(1300)
    assert decision_1300.action == ReconnectAction.RECONNECT
    assert supervisor.state == ReconnectState.DISCONNECTED


def test_ibkr_client_reconciliation_hooks_on_1101_and_1102() -> None:
    client, transport, _ = _build_client()
    client.connect()

    decision_1101 = client.handle_error_code(1101, "Connectivity restored, data lost")
    assert decision_1101.action == ReconnectAction.RESUBSCRIBE_AND_RECONCILE

    decision_1102 = client.handle_error_code(1102, "Connectivity restored")
    assert decision_1102.action == ReconnectAction.RECONCILE

    assert transport.request_open_orders_calls == 2
    assert transport.request_positions_calls == 2
    assert transport.request_executions_calls == 2


def test_ibkr_client_disconnects_on_1100_and_1300() -> None:
    client, transport, _ = _build_client()
    client.connect()
    assert client.is_connected() is True

    decision_1100 = client.handle_error_code(1100, "Connectivity lost")
    assert decision_1100.action == ReconnectAction.RECONNECT
    assert transport.connected is False

    client.connect()
    assert transport.connected is True

    decision_1300 = client.handle_error_code(1300, "Socket port reset")
    assert decision_1300.action == ReconnectAction.RECONNECT
    assert transport.connected is False


def test_ibkr_client_heartbeat_timeout_triggers_disconnect_and_event() -> None:
    client, transport, _ = _build_client()
    events: list[BrokerEventType] = []
    client.register_event_handler(lambda event: events.append(event.event_type))

    client.connect()
    client.heartbeat(timestamp=datetime.now(tz=UTC))

    future = datetime.now(tz=UTC) + timedelta(seconds=3)
    healthy = client.check_heartbeat(now=future)

    assert healthy is False
    assert transport.connected is False
    assert BrokerEventType.HEARTBEAT_TIMEOUT in events


def test_subscribe_bars_records_bar_subscription_on_transport() -> None:
    client, transport, _ = _build_client()
    contract = ContractRef(symbol="AAPL", con_id=265598)

    client.subscribe_bars(contract=contract, bar_size="5 secs")

    assert len(transport.subscriptions) == 1
    saved_contract, saved_size = transport.subscriptions[0]
    assert saved_contract.con_id == 265598
    assert saved_size == "5 secs"


def test_ibkr_client_handles_market_data_stale_with_reconnect_decision() -> None:
    client, transport, _ = _build_client()
    events: list[BrokerEventType] = []
    client.register_event_handler(lambda event: events.append(event.event_type))
    client.connect()

    decision = client.handle_market_data_stale(
        checked_at=datetime.now(tz=UTC),
        last_bar_at=None,
    )

    assert decision.action == ReconnectAction.RECONNECT
    assert transport.connected is False
    assert BrokerEventType.MARKET_DATA_STALE in events


def test_ibkr_client_applies_pacing_rate_limit() -> None:
    transport = FakeTransport()
    supervisor = ReconnectSupervisor(
        ReconnectPolicy(base_delay_seconds=1.0, max_delay_seconds=10.0, jitter_seconds=0.0),
        rng=random.Random(7),
    )
    limiter = PacingRateLimiter(max_requests=1, window_seconds=10.0, clock=lambda: 100.0)
    client = IbkrClient(
        transport=transport,
        reconnect_supervisor=supervisor,
        rate_limiter=limiter,
    )

    client.request_open_orders()
    with pytest.raises(PacingLimitError):
        client.request_positions()
