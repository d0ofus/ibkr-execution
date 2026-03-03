"""Hard-coded ORB strategy runner for workspace execution panel."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from threading import RLock
from typing import Protocol

from app.api.ws_models import ExecutionStatusPayload, WorkspaceRowSnapshot
from app.domain.enums import Side
from app.domain.errors import ValidationError
from app.data.market_data_service import MarketDataService


class OrderManagerForOrb(Protocol):
    """Execution methods used by ORB runner."""

    def submit_fixed_qty_entry(
        self,
        *,
        symbol: str,
        side: Side,
        entry_price: Decimal,
        stop_price: Decimal,
        quantity: int,
        strategy_id: str,
        intent_id: str | None = None,
    ) -> str:
        """Submit fixed-quantity entry bracket."""

    def move_stop_to_breakeven(self, trade_id: str) -> bool:
        """Move stop to breakeven once."""

    def take_profit_partial(self, trade_id: str, *, qty: int, limit_price: Decimal) -> bool:
        """Submit partial take profit and resize stop."""


class RuntimeLike(Protocol):
    """Runtime safety gates consumed by ORB runner."""

    kill_switch: bool


class BrokerConnectionLike(Protocol):
    """Broker connectivity protocol used for safety checks."""

    def is_connected(self) -> bool:
        """Return whether broker is connected."""


@dataclass(frozen=True)
class OrbParameters:
    """Runtime parameters for ORB state machine."""

    qty: int
    x1: Decimal
    x2: Decimal


@dataclass
class OrbRowState:
    """State machine snapshot for one workspace row."""

    workspace_key: str
    row_id: str
    symbol: str
    qty: int
    x1: Decimal
    x2: Decimal
    state: str
    previous_state: str | None = None
    previous_last: Decimal | None = None
    trade_id: str | None = None
    detail: str | None = None


class OrbRunner:
    """Executes ORB state transitions from live row snapshots."""

    def __init__(
        self,
        *,
        order_manager: OrderManagerForOrb,
        market_data_service: MarketDataService,
        broker_client: BrokerConnectionLike,
        runtime: RuntimeLike,
    ) -> None:
        self._order_manager = order_manager
        self._market_data_service = market_data_service
        self._broker_client = broker_client
        self._runtime = runtime
        self._lock = RLock()
        self._states: dict[tuple[str, str], OrbRowState] = {}

    def start(self, *, workspace_key: str, row_ids: list[str], params: OrbParameters) -> list[OrbRowState]:
        """Start ORB runtime for selected rows."""
        self._validate_params(params)
        updated: list[OrbRowState] = []
        with self._lock:
            for row_id in row_ids:
                row = self._market_data_service.get_row(workspace_key, row_id)
                if row is None:
                    raise ValidationError(f"row_id not found: {row_id}")
                if row.sec_type.upper() != "STK" or row.currency.upper() != "USD":
                    raise ValidationError(f"row_id {row_id} is not executable under STK/USD policy")

                state = OrbRowState(
                    workspace_key=workspace_key,
                    row_id=row_id,
                    symbol=row.symbol,
                    qty=params.qty,
                    x1=params.x1,
                    x2=params.x2,
                    state="ARMED",
                )
                self._states[(workspace_key, row_id)] = state
                updated.append(state)
                self._emit(state, detail="armed")
        return updated

    def stop(
        self,
        *,
        workspace_key: str,
        row_ids: list[str],
        stop_all: bool,
    ) -> list[OrbRowState]:
        """Stop ORB for specific rows or all rows in workspace."""
        stopped: list[OrbRowState] = []
        with self._lock:
            keys = [
                key for key in self._states if key[0] == workspace_key and (stop_all or key[1] in row_ids)
            ]
            for key in keys:
                item = self._states[key]
                item.state = "IDLE"
                item.detail = "stopped"
                stopped.append(item)
                self._emit(item, detail=item.detail)
        return stopped

    def list_status(self, *, workspace_key: str) -> list[OrbRowState]:
        """Return ORB status for a workspace."""
        with self._lock:
            return [value for key, value in self._states.items() if key[0] == workspace_key]

    def on_market_snapshot(self, workspace_key: str, snapshot: WorkspaceRowSnapshot) -> None:
        """Consume workspace snapshot update and evaluate state transitions."""
        key = (workspace_key, snapshot.row_id)
        with self._lock:
            state = self._states.get(key)
            if state is None:
                return

            if not self._is_execution_ready(snapshot):
                if state.state not in {"IDLE", "ERROR", "COMPLETED", "PAUSED_STALE"}:
                    state.previous_state = state.state
                    state.state = "PAUSED_STALE"
                    state.detail = "feed_or_connectivity_unhealthy"
                    self._emit(state, detail=state.detail)
                return

            if state.state == "PAUSED_STALE":
                state.state = state.previous_state or "ARMED"
                state.detail = "resumed"
                self._emit(state, detail=state.detail)

            if snapshot.last is None:
                return

            try:
                if state.state == "ARMED":
                    self._maybe_submit_entry(state=state, snapshot=snapshot)
                elif state.state in {"ENTRY_SUBMITTED", "ENTERED"}:
                    self._maybe_move_to_breakeven(state=state, snapshot=snapshot)
                elif state.state == "STOP_MOVED_BE":
                    self._maybe_take_partial_profit(state=state, snapshot=snapshot)
            except Exception as exc:
                state.state = "ERROR"
                state.detail = str(exc)
                self._emit(state, detail=state.detail)
            finally:
                state.previous_last = snapshot.last

    def _validate_params(self, params: OrbParameters) -> None:
        if params.qty <= 0:
            raise ValidationError("qty must be positive")
        if params.x1 <= Decimal("0"):
            raise ValidationError("x1 must be positive")
        if params.x2 <= params.x1:
            raise ValidationError("x2 must be greater than x1")

    def _is_execution_ready(self, snapshot: WorkspaceRowSnapshot) -> bool:
        if self._runtime.kill_switch:
            return False
        if not self._broker_client.is_connected():
            return False
        if not self._market_data_service.is_feed_healthy():
            return False
        if snapshot.stale:
            return False
        return True

    def _maybe_submit_entry(self, *, state: OrbRowState, snapshot: WorkspaceRowSnapshot) -> None:
        if snapshot.day_high is None or snapshot.day_low is None or snapshot.last is None:
            return
        crossed = snapshot.last >= snapshot.day_high and (
            state.previous_last is None or state.previous_last < snapshot.day_high
        )
        if not crossed:
            return

        trade_id = self._order_manager.submit_fixed_qty_entry(
            symbol=state.symbol,
            side=Side.BUY,
            entry_price=snapshot.last,
            stop_price=snapshot.day_low,
            quantity=state.qty,
            strategy_id=f"workspace_orb:{state.row_id}",
            intent_id=f"workspace_orb:{state.workspace_key}:{state.row_id}",
        )
        state.trade_id = trade_id
        state.state = "ENTRY_SUBMITTED"
        self._emit(state, detail="entry_submitted")

        state.state = "ENTERED"
        self._emit(state, detail="entered")

    def _maybe_move_to_breakeven(self, *, state: OrbRowState, snapshot: WorkspaceRowSnapshot) -> None:
        if state.trade_id is None or snapshot.last is None:
            return
        if snapshot.last < state.x1:
            return

        moved = self._order_manager.move_stop_to_breakeven(state.trade_id)
        if moved:
            state.state = "STOP_MOVED_BE"
            self._emit(state, detail="stop_moved_to_breakeven")

    def _maybe_take_partial_profit(self, *, state: OrbRowState, snapshot: WorkspaceRowSnapshot) -> None:
        if state.trade_id is None or snapshot.last is None:
            return
        if snapshot.last < state.x2:
            return

        partial_qty = max(1, state.qty // 3)
        if partial_qty >= state.qty:
            partial_qty = state.qty - 1
        if partial_qty <= 0:
            raise ValidationError("qty too small for 1/3 partial take-profit")

        done = self._order_manager.take_profit_partial(
            state.trade_id,
            qty=partial_qty,
            limit_price=snapshot.last,
        )
        if done:
            state.state = "PARTIAL_TP_DONE"
            self._emit(state, detail="partial_take_profit_done")

    def _emit(self, state: OrbRowState, *, detail: str | None) -> None:
        payload = ExecutionStatusPayload(
            scope="orb",
            row_id=state.row_id,
            strategy_id="workspace_orb",
            state=state.state,
            detail=detail,
            trade_id=state.trade_id,
        )
        self._market_data_service.on_execution_status(payload)
