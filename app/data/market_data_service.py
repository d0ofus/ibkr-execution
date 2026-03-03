"""Workspace market-data subscription management and websocket fanout."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import logging
from threading import RLock
from typing import Protocol

from app.api.ws_models import (
    ExecutionStatusPayload,
    MarketDataStatusPayload,
    WorkspaceEventEnvelope,
    WorkspaceInitialStatePayload,
    WorkspaceRowSnapshot,
)
from app.domain.models import ContractRef, MarketBar, MarketQuote


class MarketDataBrokerClient(Protocol):
    """Broker-client methods consumed by MarketDataService."""

    def is_connected(self) -> bool:
        """Return whether the broker transport is connected."""

    def subscribe_quote(self, contract: ContractRef) -> str:
        """Subscribe quote stream for a contract and return broker subscription ID."""

    def unsubscribe_quote(self, contract_or_sub_id: ContractRef | str) -> None:
        """Cancel quote stream for contract or broker subscription ID."""

    def request_historical_daily(self, contract: ContractRef, *, sessions: int) -> list[MarketBar]:
        """Return historical daily bars."""

    def request_historical_intraday(
        self,
        contract: ContractRef,
        *,
        sessions: int,
        bar_size: str,
    ) -> list[MarketBar]:
        """Return historical intraday bars."""


@dataclass(frozen=True)
class WorkspaceRow:
    """User-configured instrument row in workspace grid."""

    row_id: str
    symbol: str
    sec_type: str = "STK"
    exchange: str = "SMART"
    currency: str = "USD"
    con_id: int | None = None
    primary_exchange: str | None = None


@dataclass(frozen=True)
class InstrumentKey:
    """Stable key for de-duplicated market-data subscriptions."""

    symbol: str
    sec_type: str
    exchange: str
    currency: str
    con_id: int | None


@dataclass
class _WorkspaceState:
    rows: dict[str, WorkspaceRow] = field(default_factory=dict)
    columns: list[str] = field(default_factory=list)
    sessions: set[str] = field(default_factory=set)


@dataclass
class _SessionStream:
    workspace_key: str
    queue: asyncio.Queue[WorkspaceEventEnvelope]
    loop: asyncio.AbstractEventLoop


_DEFAULT_COLUMNS: list[str] = [
    "volume",
    "avg_volume",
    "avg_volume_at_time",
    "last",
    "day_high",
    "day_low",
    "prev_day_low",
    "close",
    "change_pct",
    "bid",
    "ask",
    "ask_below_high_pct",
]


class MarketDataService:
    """Tracks subscriptions, snapshots, and websocket fanout for workspace rows."""

    def __init__(
        self,
        *,
        broker_client: MarketDataBrokerClient,
        stale_after_seconds: float,
    ) -> None:
        self._broker_client = broker_client
        self._stale_after = stale_after_seconds
        self._logger = logging.getLogger("ibkr_exec.market_data_service")
        self._lock = RLock()

        self._workspaces: dict[str, _WorkspaceState] = {}
        self._sessions: dict[str, _SessionStream] = {}
        self._row_by_instrument: dict[InstrumentKey, set[tuple[str, str]]] = defaultdict(set)
        self._instrument_ref_count: dict[InstrumentKey, int] = defaultdict(int)
        self._instrument_subscription_id: dict[InstrumentKey, str] = {}
        self._instrument_metrics: dict[InstrumentKey, tuple[int | None, int | None, Decimal | None]] = {}

        self._row_quotes: dict[tuple[str, str], MarketQuote] = {}
        self._row_stale: dict[tuple[str, str], bool] = {}
        self._connected = False
        self._feed_healthy = False
        self._snapshot_handlers: list[Callable[[str, WorkspaceRowSnapshot], None]] = []

    def ensure_workspace(self, workspace_key: str) -> None:
        """Ensure in-memory workspace state exists."""
        normalized = self._normalize_workspace_key(workspace_key)
        with self._lock:
            self._workspaces.setdefault(normalized, _WorkspaceState(columns=list(_DEFAULT_COLUMNS)))

    def get_columns(self, workspace_key: str) -> list[str]:
        """Return configured columns for workspace."""
        normalized = self._normalize_workspace_key(workspace_key)
        with self._lock:
            state = self._workspaces.setdefault(normalized, _WorkspaceState(columns=list(_DEFAULT_COLUMNS)))
            return list(state.columns)

    def set_columns(self, workspace_key: str, columns: list[str]) -> list[str]:
        """Replace workspace column ordering."""
        normalized = self._normalize_workspace_key(workspace_key)
        cleaned = [item.strip() for item in columns if item.strip()]
        if not cleaned:
            cleaned = list(_DEFAULT_COLUMNS)
        deduped: list[str] = []
        for column in cleaned:
            if column not in deduped:
                deduped.append(column)

        with self._lock:
            state = self._workspaces.setdefault(normalized, _WorkspaceState(columns=list(_DEFAULT_COLUMNS)))
            state.columns = deduped
            self._broadcast_to_workspace_locked(
                normalized,
                "workspace.initial_state",
                self._initial_state_payload_locked(normalized).model_dump(mode="json"),
            )
        return deduped

    def get_rows(self, workspace_key: str) -> list[WorkspaceRow]:
        """Return current workspace rows."""
        normalized = self._normalize_workspace_key(workspace_key)
        with self._lock:
            state = self._workspaces.setdefault(normalized, _WorkspaceState(columns=list(_DEFAULT_COLUMNS)))
            return list(state.rows.values())

    def get_row(self, workspace_key: str, row_id: str) -> WorkspaceRow | None:
        """Return one workspace row by ID."""
        normalized = self._normalize_workspace_key(workspace_key)
        with self._lock:
            state = self._workspaces.setdefault(normalized, _WorkspaceState(columns=list(_DEFAULT_COLUMNS)))
            return state.rows.get(row_id)

    def set_rows(self, workspace_key: str, rows: list[WorkspaceRow]) -> list[WorkspaceRow]:
        """Replace workspace rows and reconcile market-data subscriptions."""
        normalized = self._normalize_workspace_key(workspace_key)
        normalized_rows = [self._normalize_row(row) for row in rows]

        with self._lock:
            state = self._workspaces.setdefault(normalized, _WorkspaceState(columns=list(_DEFAULT_COLUMNS)))
            previous_rows = dict(state.rows)
            state.rows = {row.row_id: row for row in normalized_rows}

            removed_ids = set(previous_rows) - set(state.rows)
            added_ids = set(state.rows) - set(previous_rows)
            possibly_changed_ids = set(state.rows).intersection(previous_rows)

            for row_id in removed_ids:
                self._remove_row_subscription_locked(normalized, previous_rows[row_id])
                self._row_quotes.pop((normalized, row_id), None)
                self._row_stale.pop((normalized, row_id), None)

            for row_id in added_ids:
                self._add_row_subscription_locked(normalized, state.rows[row_id])

            for row_id in possibly_changed_ids:
                old_row = previous_rows[row_id]
                new_row = state.rows[row_id]
                if self._instrument_key(old_row) != self._instrument_key(new_row):
                    self._remove_row_subscription_locked(normalized, old_row)
                    self._add_row_subscription_locked(normalized, new_row)

            self._broadcast_to_workspace_locked(
                normalized,
                "workspace.initial_state",
                self._initial_state_payload_locked(normalized).model_dump(mode="json"),
            )
            return list(state.rows.values())

    def delete_row(self, workspace_key: str, row_id: str) -> bool:
        """Delete one row and reconcile subscriptions."""
        normalized = self._normalize_workspace_key(workspace_key)
        with self._lock:
            state = self._workspaces.setdefault(normalized, _WorkspaceState(columns=list(_DEFAULT_COLUMNS)))
            row = state.rows.pop(row_id, None)
            if row is None:
                return False
            self._remove_row_subscription_locked(normalized, row)
            self._row_quotes.pop((normalized, row_id), None)
            self._row_stale.pop((normalized, row_id), None)
            self._broadcast_to_workspace_locked(
                normalized,
                "workspace.initial_state",
                self._initial_state_payload_locked(normalized).model_dump(mode="json"),
            )
            return True

    def register_session(self, *, session_id: str, workspace_key: str) -> asyncio.Queue[WorkspaceEventEnvelope]:
        """Register websocket session and return its event queue."""
        normalized_workspace = self._normalize_workspace_key(workspace_key)
        normalized_session = session_id.strip()
        if not normalized_session:
            raise ValueError("session_id cannot be empty")

        queue: asyncio.Queue[WorkspaceEventEnvelope] = asyncio.Queue(maxsize=2048)
        loop = asyncio.get_running_loop()

        with self._lock:
            state = self._workspaces.setdefault(
                normalized_workspace,
                _WorkspaceState(columns=list(_DEFAULT_COLUMNS)),
            )
            state.sessions.add(normalized_session)
            self._sessions[normalized_session] = _SessionStream(
                workspace_key=normalized_workspace,
                queue=queue,
                loop=loop,
            )
            self._enqueue_locked(
                normalized_session,
                WorkspaceEventEnvelope(
                    event="workspace.initial_state",
                    payload=self._initial_state_payload_locked(normalized_workspace).model_dump(mode="json"),
                ),
            )
        return queue

    def unregister_session(self, session_id: str) -> None:
        """Unregister websocket session and clean up workspace linkage."""
        with self._lock:
            stream = self._sessions.pop(session_id, None)
            if stream is None:
                return
            state = self._workspaces.get(stream.workspace_key)
            if state is not None:
                state.sessions.discard(session_id)

    def set_connectivity(self, *, connected: bool, reason: str | None = None) -> None:
        """Update broker/feed status and broadcast to active sessions."""
        with self._lock:
            self._connected = connected
            if not connected:
                self._feed_healthy = False
                for key in list(self._row_stale.keys()):
                    self._row_stale[key] = True
            status_payload = MarketDataStatusPayload(
                connected=self._connected,
                feed_healthy=self._feed_healthy,
                reason=reason,
            ).model_dump(mode="json")
            for workspace_key in self._workspaces:
                self._broadcast_to_workspace_locked(
                    workspace_key,
                    "market_data.status",
                    status_payload,
                )

            if connected:
                self._resubscribe_all_locked()

    def on_quote_update(self, quote: MarketQuote) -> None:
        """Merge incoming quote update and emit row snapshots."""
        normalized_quote = self._normalize_quote(quote)
        key = self._instrument_key_from_quote(normalized_quote)
        snapshot_events: list[tuple[str, WorkspaceRowSnapshot]] = []

        with self._lock:
            row_refs = list(self._row_by_instrument.get(key, set()))
            if not row_refs:
                return

            self._feed_healthy = self._connected
            for workspace_key, row_id in row_refs:
                row_key = (workspace_key, row_id)
                merged = self._merge_quote_locked(row_key=row_key, quote=normalized_quote)
                self._row_quotes[row_key] = merged
                self._row_stale[row_key] = False

                snapshot = self._build_row_snapshot_locked(
                    workspace_key=workspace_key,
                    row_id=row_id,
                    row=self._workspaces[workspace_key].rows[row_id],
                )
                self._broadcast_to_workspace_locked(
                    workspace_key,
                    "market_data.snapshot",
                    snapshot.model_dump(mode="json"),
                )
                snapshot_events.append((workspace_key, snapshot))

            self._broadcast_feed_status_locked(reason=None)

        for workspace_key, snapshot in snapshot_events:
            for handler in self._snapshot_handlers:
                handler(workspace_key, snapshot)

    def on_execution_status(self, payload: ExecutionStatusPayload) -> None:
        """Broadcast execution status to all workspaces."""
        with self._lock:
            for workspace_key in self._workspaces:
                self._broadcast_to_workspace_locked(
                    workspace_key,
                    "execution.status",
                    payload.model_dump(mode="json"),
                )

    def is_feed_healthy(self) -> bool:
        """Return whether broker connection and feed are healthy."""
        with self._lock:
            return self._connected and self._feed_healthy

    def is_connected(self) -> bool:
        """Return whether broker transport is currently connected."""
        with self._lock:
            return self._connected

    def register_snapshot_handler(self, handler: Callable[[str, WorkspaceRowSnapshot], None]) -> None:
        """Register callback invoked for each row snapshot update."""
        self._snapshot_handlers.append(handler)

    def snapshot_rows(self, workspace_key: str) -> list[WorkspaceRowSnapshot]:
        """Return computed snapshots for all rows in a workspace."""
        normalized = self._normalize_workspace_key(workspace_key)
        with self._lock:
            state = self._workspaces.setdefault(normalized, _WorkspaceState(columns=list(_DEFAULT_COLUMNS)))
            return [
                self._build_row_snapshot_locked(workspace_key=normalized, row_id=row_id, row=row)
                for row_id, row in state.rows.items()
            ]

    def snapshot_row(self, workspace_key: str, row_id: str) -> WorkspaceRowSnapshot | None:
        """Return one row snapshot from a workspace, if row exists."""
        normalized = self._normalize_workspace_key(workspace_key)
        with self._lock:
            state = self._workspaces.setdefault(normalized, _WorkspaceState(columns=list(_DEFAULT_COLUMNS)))
            row = state.rows.get(row_id)
            if row is None:
                return None
            return self._build_row_snapshot_locked(workspace_key=normalized, row_id=row_id, row=row)

    def _normalize_workspace_key(self, workspace_key: str) -> str:
        normalized = workspace_key.strip()
        if not normalized:
            raise ValueError("workspace_key cannot be empty")
        return normalized

    def _normalize_row(self, row: WorkspaceRow) -> WorkspaceRow:
        symbol = row.symbol.strip().upper()
        if not symbol:
            raise ValueError("row.symbol cannot be empty")
        row_id = row.row_id.strip()
        if not row_id:
            raise ValueError("row_id cannot be empty")
        sec_type = row.sec_type.strip().upper() or "STK"
        exchange = row.exchange.strip().upper() or "SMART"
        currency = row.currency.strip().upper() or "USD"
        primary_exchange = row.primary_exchange.strip().upper() if row.primary_exchange else None
        return WorkspaceRow(
            row_id=row_id,
            symbol=symbol,
            sec_type=sec_type,
            exchange=exchange,
            currency=currency,
            con_id=row.con_id,
            primary_exchange=primary_exchange,
        )

    def _normalize_quote(self, quote: MarketQuote) -> MarketQuote:
        symbol = quote.symbol.strip().upper()
        sec_type = quote.sec_type.strip().upper() or "STK"
        exchange = quote.exchange.strip().upper() or "SMART"
        currency = quote.currency.strip().upper() or "USD"
        primary_exchange = quote.primary_exchange.strip().upper() if quote.primary_exchange else None
        updated_at = quote.updated_at.astimezone(UTC) if quote.updated_at.tzinfo else quote.updated_at.replace(tzinfo=UTC)
        return MarketQuote(
            symbol=symbol,
            sec_type=sec_type,
            exchange=exchange,
            currency=currency,
            con_id=quote.con_id,
            primary_exchange=primary_exchange,
            bid=quote.bid,
            ask=quote.ask,
            last=quote.last,
            close=quote.close,
            day_high=quote.day_high,
            day_low=quote.day_low,
            prev_day_low=quote.prev_day_low,
            volume=quote.volume,
            avg_volume=quote.avg_volume,
            avg_volume_at_time=quote.avg_volume_at_time,
            delayed=quote.delayed,
            updated_at=updated_at,
        )

    def _instrument_key(self, row: WorkspaceRow) -> InstrumentKey:
        return InstrumentKey(
            symbol=row.symbol,
            sec_type=row.sec_type,
            exchange=row.exchange,
            currency=row.currency,
            con_id=row.con_id,
        )

    def _instrument_key_from_quote(self, quote: MarketQuote) -> InstrumentKey:
        return InstrumentKey(
            symbol=quote.symbol,
            sec_type=quote.sec_type,
            exchange=quote.exchange,
            currency=quote.currency,
            con_id=quote.con_id,
        )

    def _contract_from_row(self, row: WorkspaceRow) -> ContractRef:
        return ContractRef(
            symbol=row.symbol,
            con_id=row.con_id or 0,
            exchange=row.exchange,
            primary_exchange=row.primary_exchange,
            sec_type=row.sec_type,
            currency=row.currency,
        )

    def _add_row_subscription_locked(self, workspace_key: str, row: WorkspaceRow) -> None:
        key = self._instrument_key(row)
        row_ref = (workspace_key, row.row_id)
        self._row_by_instrument[key].add(row_ref)
        self._instrument_ref_count[key] += 1
        self._row_stale[(workspace_key, row.row_id)] = True

        if self._instrument_ref_count[key] > 1:
            return

        contract = self._contract_from_row(row)
        self._bootstrap_metrics_locked(key=key, contract=contract)

        if not self._broker_client.is_connected():
            return

        try:
            sub_id = self._broker_client.subscribe_quote(contract)
            self._instrument_subscription_id[key] = sub_id
        except Exception:
            self._logger.exception("market_data_subscribe_failed symbol=%s", row.symbol)

    def _remove_row_subscription_locked(self, workspace_key: str, row: WorkspaceRow) -> None:
        key = self._instrument_key(row)
        row_ref = (workspace_key, row.row_id)
        members = self._row_by_instrument.get(key)
        if members is None:
            return

        members.discard(row_ref)
        self._instrument_ref_count[key] = max(0, self._instrument_ref_count[key] - 1)

        if members or self._instrument_ref_count[key] > 0:
            return

        self._row_by_instrument.pop(key, None)
        self._instrument_ref_count.pop(key, None)
        self._instrument_metrics.pop(key, None)

        subscription_id = self._instrument_subscription_id.pop(key, None)
        if subscription_id is None:
            return
        try:
            self._broker_client.unsubscribe_quote(subscription_id)
        except Exception:
            self._logger.exception("market_data_unsubscribe_failed symbol=%s", row.symbol)

    def _bootstrap_metrics_locked(self, *, key: InstrumentKey, contract: ContractRef) -> None:
        if key in self._instrument_metrics:
            return
        try:
            daily = self._broker_client.request_historical_daily(contract, sessions=20)
            intraday = self._broker_client.request_historical_intraday(
                contract,
                sessions=20,
                bar_size="1 min",
            )
            avg_daily = self._average_daily_volume(daily)
            avg_at_time = self._average_volume_at_time(intraday)
            prev_day_low = self._previous_day_low(daily)
            self._instrument_metrics[key] = (avg_daily, avg_at_time, prev_day_low)
        except Exception:
            self._logger.exception("historical_bootstrap_failed symbol=%s", contract.symbol)
            self._instrument_metrics[key] = (None, None, None)

    def _average_daily_volume(self, bars: list[MarketBar]) -> int | None:
        if not bars:
            return None
        totals = [bar.volume for bar in bars if bar.volume > 0]
        if not totals:
            return None
        return int(sum(totals) / len(totals))

    def _average_volume_at_time(self, bars: list[MarketBar]) -> int | None:
        if not bars:
            return None

        now_utc = datetime.now(tz=UTC)
        minute_of_day = now_utc.hour * 60 + now_utc.minute
        grouped: dict[str, list[MarketBar]] = defaultdict(list)
        for bar in bars:
            key = bar.timestamp.astimezone(UTC).strftime("%Y-%m-%d")
            grouped[key].append(bar)

        cumulative_values: list[int] = []
        for day_bars in grouped.values():
            ordered = sorted(day_bars, key=lambda item: item.timestamp)
            if not ordered:
                continue
            cutoff_index = min(len(ordered) - 1, minute_of_day)
            cumulative = sum(item.volume for item in ordered[: cutoff_index + 1])
            cumulative_values.append(cumulative)

        if not cumulative_values:
            return None
        return int(sum(cumulative_values) / len(cumulative_values))

    def _previous_day_low(self, bars: list[MarketBar]) -> Decimal | None:
        ordered = sorted(bars, key=lambda item: item.timestamp)
        if len(ordered) >= 2:
            return ordered[-2].low
        if ordered:
            return ordered[-1].low
        return None

    def _resubscribe_all_locked(self) -> None:
        for key, refs in self._instrument_ref_count.items():
            if refs <= 0:
                continue
            row_ref = next(iter(self._row_by_instrument.get(key, set())), None)
            if row_ref is None:
                continue
            workspace_key, row_id = row_ref
            row = self._workspaces.get(workspace_key, _WorkspaceState()).rows.get(row_id)
            if row is None:
                continue
            contract = self._contract_from_row(row)
            try:
                sub_id = self._broker_client.subscribe_quote(contract)
                self._instrument_subscription_id[key] = sub_id
            except Exception:
                self._logger.exception("market_data_resubscribe_failed symbol=%s", row.symbol)

    def _merge_quote_locked(self, *, row_key: tuple[str, str], quote: MarketQuote) -> MarketQuote:
        previous = self._row_quotes.get(row_key)
        if previous is None:
            previous = quote

        avg_volume, avg_volume_at_time, prev_day_low = self._instrument_metrics.get(
            self._instrument_key_from_quote(quote),
            (None, None, None),
        )

        return MarketQuote(
            symbol=quote.symbol,
            sec_type=quote.sec_type,
            exchange=quote.exchange,
            currency=quote.currency,
            con_id=quote.con_id,
            primary_exchange=quote.primary_exchange,
            bid=quote.bid if quote.bid is not None else previous.bid,
            ask=quote.ask if quote.ask is not None else previous.ask,
            last=quote.last if quote.last is not None else previous.last,
            close=quote.close if quote.close is not None else previous.close,
            day_high=quote.day_high if quote.day_high is not None else previous.day_high,
            day_low=quote.day_low if quote.day_low is not None else previous.day_low,
            prev_day_low=(
                quote.prev_day_low
                if quote.prev_day_low is not None
                else (previous.prev_day_low if previous.prev_day_low is not None else prev_day_low)
            ),
            volume=quote.volume if quote.volume is not None else previous.volume,
            avg_volume=quote.avg_volume if quote.avg_volume is not None else avg_volume,
            avg_volume_at_time=(
                quote.avg_volume_at_time
                if quote.avg_volume_at_time is not None
                else avg_volume_at_time
            ),
            delayed=quote.delayed,
            updated_at=quote.updated_at,
        )

    def _build_row_snapshot_locked(
        self,
        *,
        workspace_key: str,
        row_id: str,
        row: WorkspaceRow,
    ) -> WorkspaceRowSnapshot:
        row_key = (workspace_key, row_id)
        quote = self._row_quotes.get(row_key)

        if quote is None:
            return WorkspaceRowSnapshot(
                row_id=row_id,
                symbol=row.symbol,
                sec_type=row.sec_type,
                exchange=row.exchange,
                currency=row.currency,
                stale=True,
            )

        stale = self._is_row_stale_locked(row_key=row_key, updated_at=quote.updated_at)
        change_pct = self._compute_change_pct(quote.last, quote.close)
        ask_below_high_pct = self._compute_ask_below_high_pct(quote.ask, quote.day_high)

        return WorkspaceRowSnapshot(
            row_id=row_id,
            symbol=row.symbol,
            sec_type=row.sec_type,
            exchange=row.exchange,
            currency=row.currency,
            volume=quote.volume,
            avg_volume=quote.avg_volume,
            avg_volume_at_time=quote.avg_volume_at_time,
            last=quote.last,
            day_high=quote.day_high,
            day_low=quote.day_low,
            prev_day_low=quote.prev_day_low,
            close=quote.close,
            change_pct=change_pct,
            bid=quote.bid,
            ask=quote.ask,
            ask_below_high_pct=ask_below_high_pct,
            delayed=quote.delayed,
            stale=stale,
            updated_at=quote.updated_at,
        )

    def _is_row_stale_locked(self, *, row_key: tuple[str, str], updated_at: datetime) -> bool:
        explicit_stale = self._row_stale.get(row_key, True)
        if explicit_stale:
            return True
        now = datetime.now(tz=UTC)
        return now - updated_at > timedelta(seconds=self._stale_after)

    def _initial_state_payload_locked(self, workspace_key: str) -> WorkspaceInitialStatePayload:
        state = self._workspaces.setdefault(workspace_key, _WorkspaceState(columns=list(_DEFAULT_COLUMNS)))
        snapshots = [
            self._build_row_snapshot_locked(workspace_key=workspace_key, row_id=row_id, row=row)
            for row_id, row in state.rows.items()
        ]
        return WorkspaceInitialStatePayload(
            workspace_key=workspace_key,
            rows=snapshots,
            columns=list(state.columns),
            connected=self._connected,
            feed_healthy=self._connected and self._feed_healthy,
        )

    def _broadcast_feed_status_locked(self, *, reason: str | None) -> None:
        payload = MarketDataStatusPayload(
            connected=self._connected,
            feed_healthy=self._connected and self._feed_healthy,
            reason=reason,
        ).model_dump(mode="json")
        for workspace_key in self._workspaces:
            self._broadcast_to_workspace_locked(workspace_key, "market_data.status", payload)

    def _broadcast_to_workspace_locked(
        self,
        workspace_key: str,
        event: str,
        payload: dict[str, object],
    ) -> None:
        state = self._workspaces.get(workspace_key)
        if state is None:
            return
        envelope = WorkspaceEventEnvelope(event=event, payload=payload)
        for session_id in list(state.sessions):
            self._enqueue_locked(session_id, envelope)

    def _enqueue_locked(self, session_id: str, envelope: WorkspaceEventEnvelope) -> None:
        stream = self._sessions.get(session_id)
        if stream is None:
            return

        def _put() -> None:
            try:
                stream.queue.put_nowait(envelope)
            except asyncio.QueueFull:
                self._logger.warning("workspace_ws_queue_full session=%s", session_id)

        stream.loop.call_soon_threadsafe(_put)

    @staticmethod
    def _compute_change_pct(last: Decimal | None, close: Decimal | None) -> Decimal | None:
        if last is None or close is None or close == Decimal("0"):
            return None
        return ((last - close) / close) * Decimal("100")

    @staticmethod
    def _compute_ask_below_high_pct(ask: Decimal | None, high: Decimal | None) -> Decimal | None:
        if ask is None or high is None or high == Decimal("0"):
            return None
        return ((high - ask) / high) * Decimal("100")


def build_workspace_key(user_key: str, environment: str) -> str:
    """Return stable in-memory workspace key."""
    normalized_user = user_key.strip() or "local"
    normalized_env = environment.strip().lower() or "paper"
    return f"{normalized_user}:{normalized_env}"


def default_columns() -> list[str]:
    """Return default workspace column ordering."""
    return list(_DEFAULT_COLUMNS)
