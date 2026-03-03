"""Unit tests for workspace ORB runner state transitions."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from app.api.ws_models import WorkspaceRowSnapshot
from app.data.market_data_service import WorkspaceRow
from app.execution.orb_runner import OrbParameters, OrbRunner


@dataclass
class FakeOrderManager:
    entries: list[tuple[str, Decimal, Decimal, int]] = field(default_factory=list)
    be_moves: list[str] = field(default_factory=list)
    partials: list[tuple[str, int, Decimal]] = field(default_factory=list)

    def submit_fixed_qty_entry(
        self,
        *,
        symbol: str,
        side,
        entry_price: Decimal,
        stop_price: Decimal,
        quantity: int,
        strategy_id: str,
        intent_id: str | None = None,
    ) -> str:
        _ = (side, strategy_id, intent_id)
        self.entries.append((symbol, entry_price, stop_price, quantity))
        return "trade-1"

    def move_stop_to_breakeven(self, trade_id: str) -> bool:
        self.be_moves.append(trade_id)
        return True

    def take_profit_partial(self, trade_id: str, *, qty: int, limit_price: Decimal) -> bool:
        self.partials.append((trade_id, qty, limit_price))
        return True


@dataclass
class FakeMarketDataService:
    rows: dict[str, WorkspaceRow]
    feed_healthy: bool = True
    execution_events: list[dict[str, str]] = field(default_factory=list)

    def get_row(self, workspace_key: str, row_id: str) -> WorkspaceRow | None:
        _ = workspace_key
        return self.rows.get(row_id)

    def is_feed_healthy(self) -> bool:
        return self.feed_healthy

    def on_execution_status(self, payload) -> None:  # type: ignore[no-untyped-def]
        self.execution_events.append(payload.model_dump(mode="json"))


@dataclass
class FakeBrokerClient:
    connected: bool = True

    def is_connected(self) -> bool:
        return self.connected


@dataclass
class FakeRuntime:
    kill_switch: bool = False


def _snapshot(
    *,
    row_id: str,
    last: str,
    day_high: str,
    day_low: str,
    stale: bool = False,
) -> WorkspaceRowSnapshot:
    return WorkspaceRowSnapshot(
        row_id=row_id,
        symbol="AAPL",
        sec_type="STK",
        exchange="SMART",
        currency="USD",
        last=Decimal(last),
        day_high=Decimal(day_high),
        day_low=Decimal(day_low),
        stale=stale,
    )


def test_orb_runner_transitions_entry_be_and_partial_take_profit() -> None:
    row = WorkspaceRow(row_id="row-1", symbol="AAPL", sec_type="STK", exchange="SMART", currency="USD")
    service = FakeMarketDataService(rows={"row-1": row})
    manager = FakeOrderManager()
    runner = OrbRunner(
        order_manager=manager,
        market_data_service=service,  # type: ignore[arg-type]
        broker_client=FakeBrokerClient(),
        runtime=FakeRuntime(),
    )

    _ = runner.start(
        workspace_key="local:paper",
        row_ids=["row-1"],
        params=OrbParameters(qty=90, x1=Decimal("102"), x2=Decimal("105")),
    )

    runner.on_market_snapshot("local:paper", _snapshot(row_id="row-1", last="100", day_high="101", day_low="99"))
    runner.on_market_snapshot("local:paper", _snapshot(row_id="row-1", last="101", day_high="101", day_low="99"))
    runner.on_market_snapshot("local:paper", _snapshot(row_id="row-1", last="103", day_high="101", day_low="99"))
    runner.on_market_snapshot("local:paper", _snapshot(row_id="row-1", last="106", day_high="101", day_low="99"))

    assert len(manager.entries) == 1
    assert len(manager.be_moves) == 1
    assert len(manager.partials) == 1
    assert manager.partials[0][1] == 30

    status = runner.list_status(workspace_key="local:paper")
    assert status[0].state == "PARTIAL_TP_DONE"


def test_orb_runner_pauses_when_stale_and_resumes_when_healthy() -> None:
    row = WorkspaceRow(row_id="row-2", symbol="AAPL", sec_type="STK", exchange="SMART", currency="USD")
    service = FakeMarketDataService(rows={"row-2": row})
    manager = FakeOrderManager()
    runner = OrbRunner(
        order_manager=manager,
        market_data_service=service,  # type: ignore[arg-type]
        broker_client=FakeBrokerClient(),
        runtime=FakeRuntime(),
    )

    _ = runner.start(
        workspace_key="local:paper",
        row_ids=["row-2"],
        params=OrbParameters(qty=30, x1=Decimal("101"), x2=Decimal("102")),
    )

    runner.on_market_snapshot("local:paper", _snapshot(row_id="row-2", last="101", day_high="101", day_low="99", stale=True))
    assert runner.list_status(workspace_key="local:paper")[0].state == "PAUSED_STALE"
    assert len(manager.entries) == 0

    runner.on_market_snapshot("local:paper", _snapshot(row_id="row-2", last="101", day_high="101", day_low="99", stale=False))
    assert len(manager.entries) == 1
