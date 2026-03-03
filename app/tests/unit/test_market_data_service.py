"""Unit tests for MarketDataService subscription lifecycle and streaming behavior."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal

from app.data.market_data_service import MarketDataService, WorkspaceRow
from app.domain.models import ContractRef, MarketBar, MarketQuote


@dataclass
class FakeBrokerClient:
    connected: bool = True
    subscribe_calls: list[ContractRef] = field(default_factory=list)
    unsubscribe_calls: list[str] = field(default_factory=list)

    def is_connected(self) -> bool:
        return self.connected

    def subscribe_quote(self, contract: ContractRef) -> str:
        self.subscribe_calls.append(contract)
        return f"sub-{len(self.subscribe_calls)}"

    def unsubscribe_quote(self, contract_or_sub_id: ContractRef | str) -> None:
        if isinstance(contract_or_sub_id, ContractRef):
            self.unsubscribe_calls.append(str(contract_or_sub_id.con_id))
            return
        self.unsubscribe_calls.append(contract_or_sub_id)

    def request_historical_daily(self, contract: ContractRef, *, sessions: int) -> list[MarketBar]:
        _ = (contract, sessions)
        return [
            MarketBar(
                symbol="AAPL",
                timestamp=datetime(2026, 1, 1, tzinfo=UTC),
                open=Decimal("100"),
                high=Decimal("101"),
                low=Decimal("95"),
                close=Decimal("100"),
                volume=1000,
            ),
            MarketBar(
                symbol="AAPL",
                timestamp=datetime(2026, 1, 2, tzinfo=UTC),
                open=Decimal("100"),
                high=Decimal("102"),
                low=Decimal("96"),
                close=Decimal("101"),
                volume=1200,
            ),
        ]

    def request_historical_intraday(
        self,
        contract: ContractRef,
        *,
        sessions: int,
        bar_size: str,
    ) -> list[MarketBar]:
        _ = (contract, sessions, bar_size)
        return [
            MarketBar(
                symbol="AAPL",
                timestamp=datetime(2026, 1, 2, 13, 30, tzinfo=UTC),
                open=Decimal("100"),
                high=Decimal("101"),
                low=Decimal("99"),
                close=Decimal("100"),
                volume=500,
            )
        ]


def test_subscription_lifecycle_deduplicates_and_unsubscribes_at_zero_refs() -> None:
    broker = FakeBrokerClient()
    service = MarketDataService(broker_client=broker, stale_after_seconds=30.0)
    key = "local:paper"

    service.set_rows(
        key,
        [
            WorkspaceRow(row_id="A", symbol="AAPL", sec_type="STK", exchange="SMART", currency="USD"),
            WorkspaceRow(row_id="B", symbol="AAPL", sec_type="STK", exchange="SMART", currency="USD"),
        ],
    )

    assert len(broker.subscribe_calls) == 1

    service.delete_row(key, "A")
    assert broker.unsubscribe_calls == []

    service.delete_row(key, "B")
    assert len(broker.unsubscribe_calls) == 1


def test_websocket_queue_receives_initial_and_snapshot_events() -> None:
    async def _run() -> None:
        broker = FakeBrokerClient()
        service = MarketDataService(broker_client=broker, stale_after_seconds=30.0)
        workspace = "local:paper"
        service.set_connectivity(connected=True)
        service.set_rows(
            workspace,
            [WorkspaceRow(row_id="AAPL-1", symbol="AAPL", sec_type="STK", exchange="SMART", currency="USD")],
        )

        queue = service.register_session(session_id="session-1", workspace_key=workspace)
        initial = await asyncio.wait_for(queue.get(), timeout=1.0)
        assert initial.event == "workspace.initial_state"

        service.on_quote_update(
            MarketQuote(
                symbol="AAPL",
                sec_type="STK",
                exchange="SMART",
                currency="USD",
                last=Decimal("200"),
                day_high=Decimal("201"),
                day_low=Decimal("198"),
                close=Decimal("199"),
                bid=Decimal("199.9"),
                ask=Decimal("200.1"),
                volume=100,
                updated_at=datetime.now(tz=UTC),
            )
        )

        snapshot_event = await asyncio.wait_for(queue.get(), timeout=1.0)
        assert snapshot_event.event == "market_data.snapshot"
        assert snapshot_event.payload["row_id"] == "AAPL-1"
        assert snapshot_event.payload["data_mode"] == "live"

        status_event = await asyncio.wait_for(queue.get(), timeout=1.0)
        assert status_event.event == "market_data.status"

    asyncio.run(_run())


def test_disconnect_emits_market_data_status_event() -> None:
    async def _run() -> None:
        broker = FakeBrokerClient()
        service = MarketDataService(broker_client=broker, stale_after_seconds=30.0)
        workspace = "local:paper"
        queue = service.register_session(session_id="session-2", workspace_key=workspace)

        _ = await asyncio.wait_for(queue.get(), timeout=1.0)
        service.set_connectivity(connected=False, reason="connection_closed")
        status_event = await asyncio.wait_for(queue.get(), timeout=1.0)

        assert status_event.event == "market_data.status"
        assert status_event.payload["connected"] is False

    asyncio.run(_run())


def test_seed_quote_from_historical_populates_initial_snapshot_fields() -> None:
    broker = FakeBrokerClient()
    service = MarketDataService(broker_client=broker, stale_after_seconds=30.0)
    workspace = "local:paper"
    service.set_rows(
        workspace,
        [WorkspaceRow(row_id="AAPL-1", symbol="AAPL", sec_type="STK", exchange="SMART", currency="USD")],
    )

    snapshot = service.snapshot_row(workspace, "AAPL-1")
    assert snapshot is not None
    assert snapshot.last == Decimal("100")
    assert snapshot.day_high == Decimal("101")
    assert snapshot.day_low == Decimal("99")
    assert snapshot.close == Decimal("101")
    assert snapshot.avg_volume == 1100
    assert snapshot.avg_volume_at_time == 500
    assert snapshot.prev_day_low == Decimal("95")
    assert snapshot.bid == Decimal("100")
    assert snapshot.ask == Decimal("100")
    assert snapshot.data_mode == "stale"
