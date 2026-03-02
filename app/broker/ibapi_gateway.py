"""Concrete IB API socket gateway for contract qualification and order routing."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
import logging
import threading
from typing import Any
from collections.abc import Callable

from ibapi.client import EClient
from ibapi.common import BarData
from ibapi.contract import Contract, ContractDetails
from ibapi.execution import ExecutionFilter
from ibapi.order import Order
from ibapi.tag_value import TagValue
from ibapi.wrapper import EWrapper

from app.broker.contracts import ContractCandidate
from app.domain.errors import BrokerConnectivityError
from app.domain.models import BrokerOrderSpec, ContractRef, MarketBar


@dataclass
class _PendingContractQualification:
    """Pending synchronous contract-qualification request."""

    completed: threading.Event = field(default_factory=threading.Event)
    candidates: list[ContractCandidate] = field(default_factory=list)
    error_message: str | None = None


@dataclass
class _PendingOrderAck:
    """Pending synchronous order-acknowledgement request."""

    completed: threading.Event = field(default_factory=threading.Event)
    acknowledged: bool = False
    rejected: bool = False
    error_message: str | None = None


class IbApiGateway(EWrapper, EClient):  # type: ignore[misc]
    """Thread-safe IB API gateway implementing broker transport operations."""

    def __init__(
        self,
        *,
        host: str,
        port: int,
        client_id: int,
        connect_timeout_seconds: float = 10.0,
        request_timeout_seconds: float = 10.0,
    ) -> None:
        EWrapper.__init__(self)
        EClient.__init__(self, wrapper=self)

        self._host = host
        self._port = port
        self._client_id = client_id
        self._connect_timeout_seconds = connect_timeout_seconds
        self._request_timeout_seconds = request_timeout_seconds
        self._logger = logging.getLogger("ibkr_exec.ibapi_gateway")

        self._reader_thread: threading.Thread | None = None
        self._connected_event = threading.Event()
        self._order_id_lock = threading.Lock()
        self._next_valid_order_id: int | None = None
        self._request_id_lock = threading.Lock()
        self._next_request_id = 1
        self._pending_contract_requests: dict[int, _PendingContractQualification] = {}
        self._pending_order_acks: dict[int, _PendingOrderAck] = {}
        self._pending_order_acks_lock = threading.Lock()
        self._bar_subscription_ids: dict[int, int] = {}
        self._bar_subscription_symbols: dict[int, str] = {}
        self._bar_handlers: list[Callable[[MarketBar], None]] = []
        self._order_status_handlers: list[Callable[[int, str, int, int], None]] = []

    def connect(self) -> None:
        """Connect to IB API socket and wait for next valid order ID callback."""
        if self.is_connected():
            return

        EClient.connect(self, self._host, self._port, self._client_id)
        self._start_reader_thread()

        if not self._connected_event.wait(timeout=self._connect_timeout_seconds):
            EClient.disconnect(self)
            raise BrokerConnectivityError(
                "Timed out waiting for IB nextValidId during connect. "
                "Check IB Gateway/TWS API settings and client ID."
            )

    def disconnect(self) -> None:
        """Disconnect from IB API socket."""
        if self.isConnected():
            EClient.disconnect(self)
        self._connected_event.clear()

        if self._reader_thread is not None and self._reader_thread.is_alive():
            self._reader_thread.join(timeout=2.0)
        self._reader_thread = None

    def is_connected(self) -> bool:
        """Return current connection status."""
        return bool(self.isConnected())

    def place_orders(self, contract: ContractRef, orders: list[BrokerOrderSpec]) -> list[str]:
        """Submit one or more orders and return assigned broker order IDs."""
        if not self.is_connected():
            raise BrokerConnectivityError("IB API not connected.")
        if not orders:
            return []

        local_to_broker_id: dict[int, int] = {}
        used_order_ids: set[int] = set()
        assigned_ids: list[str] = []
        ib_contract = self._to_ib_contract(contract)

        for spec in orders:
            order_id = self._resolve_submission_order_id(
                spec=spec,
                local_to_broker_id=local_to_broker_id,
                used_order_ids=used_order_ids,
            )
            pending_ack = _PendingOrderAck()
            with self._pending_order_acks_lock:
                self._pending_order_acks[order_id] = pending_ack

            ib_order = self._to_ib_order(
                spec=spec,
                order_id=order_id,
                local_to_broker_id=local_to_broker_id,
            )

            super().placeOrder(order_id, ib_contract, ib_order)

            if not pending_ack.completed.wait(timeout=self._request_timeout_seconds):
                with self._pending_order_acks_lock:
                    self._pending_order_acks.pop(order_id, None)
                raise BrokerConnectivityError(
                    f"Timed out waiting for broker acknowledgement for order_id={order_id}."
                )
            if pending_ack.rejected:
                with self._pending_order_acks_lock:
                    self._pending_order_acks.pop(order_id, None)
                reason = pending_ack.error_message or "unknown broker rejection"
                raise BrokerConnectivityError(
                    f"Broker rejected order_id={order_id}: {reason}"
                )
            with self._pending_order_acks_lock:
                self._pending_order_acks.pop(order_id, None)

            assigned_ids.append(str(order_id))
            used_order_ids.add(order_id)

            if spec.order_id is not None:
                local_to_broker_id[spec.order_id] = order_id

        return assigned_ids

    def cancel_order(self, broker_order_id: str) -> None:
        """Cancel an active order by broker order ID."""
        order_id = int(broker_order_id)
        super().cancelOrder(order_id, "")

    def request_open_orders(self) -> None:
        """Request open orders snapshot."""
        super().reqOpenOrders()

    def request_positions(self) -> None:
        """Request positions snapshot."""
        super().reqPositions()

    def request_executions(self, since: datetime | None = None) -> None:
        """Request executions optionally filtered by timestamp."""
        request_id = self._reserve_request_id()
        execution_filter = ExecutionFilter()
        if since is not None:
            execution_filter.time = since.strftime("%Y%m%d-%H:%M:%S")
        super().reqExecutions(request_id, execution_filter)

    def subscribe_bars(self, contract: ContractRef, bar_size: str) -> None:
        """Subscribe to 5-second real-time bars for the given contract."""
        del bar_size  # IB real-time bars are fixed-size (5 seconds).
        request_id = self._reserve_request_id()
        ib_contract = self._to_ib_contract(contract)
        self._bar_subscription_ids[contract.con_id] = request_id
        self._bar_subscription_symbols[request_id] = contract.symbol.upper()
        super().reqRealTimeBars(request_id, ib_contract, 5, "TRADES", True, [])

    def register_realtime_bar_handler(self, handler: Callable[[MarketBar], None]) -> None:
        """Register callback for incoming IB real-time bars."""
        self._bar_handlers.append(handler)

    def register_order_status_handler(self, handler: Callable[[int, str, int, int], None]) -> None:
        """Register callback for normalized order status updates."""
        self._order_status_handlers.append(handler)

    def qualify(self, symbol: str) -> list[ContractCandidate]:
        """Synchronously qualify a US equity symbol against IB contract details."""
        if not self.is_connected():
            raise BrokerConnectivityError("IB API not connected.")

        request_id = self._reserve_request_id()
        pending = _PendingContractQualification()
        self._pending_contract_requests[request_id] = pending

        contract = Contract()
        contract.symbol = symbol.upper()
        contract.secType = "STK"
        contract.currency = "USD"
        contract.exchange = "SMART"

        super().reqContractDetails(request_id, contract)

        if not pending.completed.wait(timeout=self._request_timeout_seconds):
            self._pending_contract_requests.pop(request_id, None)
            raise BrokerConnectivityError(f"Contract qualification timed out for symbol {symbol}.")

        self._pending_contract_requests.pop(request_id, None)
        if pending.error_message is not None and not pending.candidates:
            raise BrokerConnectivityError(pending.error_message)

        return pending.candidates

    def nextValidId(self, orderId: int) -> None:  # noqa: N802
        """IB callback providing next valid order ID."""
        with self._order_id_lock:
            self._next_valid_order_id = orderId
        self._connected_event.set()

    def contractDetails(self, reqId: int, contractDetails: ContractDetails) -> None:  # noqa: N802
        """IB callback for contract qualification candidates."""
        pending = self._pending_contract_requests.get(reqId)
        if pending is None:
            return

        contract = contractDetails.contract
        pending.candidates.append(
            ContractCandidate(
                symbol=contract.symbol.upper(),
                con_id=int(contract.conId),
                exchange=contract.exchange.upper(),
                primary_exchange=contract.primaryExchange.upper() if contract.primaryExchange else None,
                sec_type=contract.secType.upper(),
                currency=contract.currency.upper(),
            )
        )

    def contractDetailsEnd(self, reqId: int) -> None:  # noqa: N802
        """IB callback signaling end of contract details stream."""
        pending = self._pending_contract_requests.get(reqId)
        if pending is not None:
            pending.completed.set()

    def error(  # noqa: N802
        self,
        reqId: int,
        errorCode: int,
        errorString: str,
        advancedOrderRejectJson: str = "",
    ) -> None:
        """IB callback for socket/errors; routes contract-request errors to pending waiters."""
        if reqId in self._pending_contract_requests:
            pending = self._pending_contract_requests[reqId]
            pending.error_message = f"IB error {errorCode}: {errorString}"
            if errorCode in {200, 201, 321}:
                pending.completed.set()
        with self._pending_order_acks_lock:
            pending_order_ack = self._pending_order_acks.get(reqId)
        if pending_order_ack is not None:
            pending_order_ack.rejected = True
            pending_order_ack.error_message = f"IB error {errorCode}: {errorString}"
            pending_order_ack.completed.set()

        self._logger.warning(
            "ib_error req_id=%s code=%s message=%s",
            reqId,
            errorCode,
            errorString,
        )
        _ = advancedOrderRejectJson

    def realtimeBar(  # noqa: N802
        self,
        reqId: int,
        time: int,
        open_: float,
        high: float,
        low: float,
        close: float,
        volume: int,
        wap: float,
        count: int,
    ) -> None:
        """IB callback for real-time bars (currently logged only)."""
        self._logger.debug(
            "realtime_bar req_id=%s ts=%s open=%s high=%s low=%s close=%s volume=%s",
            reqId,
            time,
            open_,
            high,
            low,
            close,
            volume,
        )
        symbol = self._bar_subscription_symbols.get(reqId)
        if symbol is not None:
            market_bar = MarketBar(
                symbol=symbol,
                timestamp=datetime.fromtimestamp(time, tz=UTC),
                open=Decimal(str(open_)),
                high=Decimal(str(high)),
                low=Decimal(str(low)),
                close=Decimal(str(close)),
                volume=int(volume),
            )
            for handler in self._bar_handlers:
                handler(market_bar)
        _ = (wap, count)

    def historicalData(self, reqId: int, bar: BarData) -> None:  # noqa: N802
        """IB callback placeholder for historical bars."""
        self._logger.debug("historical_data req_id=%s bar_date=%s", reqId, bar.date)

    def openOrder(  # noqa: N802
        self,
        orderId: int,
        contract: Contract,
        order: Order,
        orderState: Any,
    ) -> None:
        """IB callback indicating broker accepted the order envelope."""
        self._mark_order_acknowledged(order_id=orderId)
        _ = (contract, order, orderState)

    def orderStatus(  # noqa: N802
        self,
        orderId: int,
        status: str,
        filled: float,
        remaining: float,
        avgFillPrice: float,
        permId: int,
        parentId: int,
        lastFillPrice: float,
        clientId: int,
        whyHeld: str,
        mktCapPrice: float,
    ) -> None:
        """IB callback for order status transitions."""
        normalized = status.strip().lower()
        if normalized in {
            "submitted",
            "presubmitted",
            "apipending",
            "pendingcancel",
            "filled",
        }:
            self._mark_order_acknowledged(order_id=orderId)
        elif normalized in {"cancelled", "apicancelled", "inactive"}:
            self._mark_order_rejected(
                order_id=orderId,
                message=f"order status {status}",
            )
        for handler in self._order_status_handlers:
            handler(orderId, status, int(filled), int(remaining))
        _ = (
            filled,
            remaining,
            avgFillPrice,
            permId,
            parentId,
            lastFillPrice,
            clientId,
            whyHeld,
            mktCapPrice,
        )

    def _start_reader_thread(self) -> None:
        if self._reader_thread is not None and self._reader_thread.is_alive():
            return
        self._reader_thread = threading.Thread(target=self.run, name="ibapi-reader", daemon=True)
        self._reader_thread.start()

    def _reserve_request_id(self) -> int:
        with self._request_id_lock:
            request_id = self._next_request_id
            self._next_request_id += 1
        return request_id

    def _resolve_submission_order_id(
        self,
        *,
        spec: BrokerOrderSpec,
        local_to_broker_id: dict[int, int],
        used_order_ids: set[int],
    ) -> int:
        if spec.modification_of_order_id is not None:
            return local_to_broker_id.get(spec.modification_of_order_id, spec.modification_of_order_id)

        return self._reserve_next_order_id(used_order_ids=used_order_ids)

    def _reserve_next_order_id(self, *, used_order_ids: set[int]) -> int:
        with self._order_id_lock:
            if self._next_valid_order_id is None:
                raise BrokerConnectivityError("No nextValidId available from IB connection.")
            while self._next_valid_order_id in used_order_ids:
                self._next_valid_order_id += 1
            reserved = self._next_valid_order_id
            self._next_valid_order_id += 1
            return reserved

    def _mark_order_acknowledged(self, *, order_id: int) -> None:
        with self._pending_order_acks_lock:
            pending = self._pending_order_acks.get(order_id)
        if pending is None:
            return
        pending.acknowledged = True
        pending.completed.set()

    def _mark_order_rejected(self, *, order_id: int, message: str) -> None:
        with self._pending_order_acks_lock:
            pending = self._pending_order_acks.get(order_id)
        if pending is None:
            return
        pending.rejected = True
        pending.error_message = message
        pending.completed.set()

    @staticmethod
    def _to_ib_contract(contract: ContractRef) -> Contract:
        ib_contract = Contract()
        ib_contract.symbol = contract.symbol
        ib_contract.conId = contract.con_id
        ib_contract.secType = contract.sec_type
        ib_contract.exchange = contract.exchange
        ib_contract.primaryExchange = contract.primary_exchange or ""
        ib_contract.currency = contract.currency
        return ib_contract

    def _to_ib_order(
        self,
        *,
        spec: BrokerOrderSpec,
        order_id: int,
        local_to_broker_id: dict[int, int],
    ) -> Order:
        order = Order()
        order.orderId = order_id
        order.action = spec.side.value
        order.totalQuantity = spec.quantity
        order.orderType = "STP" if spec.order_type == "MODIFY_STP" else spec.order_type
        order.tif = spec.time_in_force.value
        order.transmit = spec.transmit

        if spec.limit_price is not None:
            order.lmtPrice = float(spec.limit_price)
        if spec.stop_price is not None:
            order.auxPrice = float(spec.stop_price)

        if spec.parent_order_id is not None:
            order.parentId = local_to_broker_id.get(spec.parent_order_id, spec.parent_order_id)

        if spec.algo_strategy is not None:
            order.algoStrategy = spec.algo_strategy
            order.algoParams = [TagValue(tag=key, value=value) for key, value in spec.algo_params.items()]

        # Newer IB server builds can reject these deprecated attributes unless explicitly disabled.
        if hasattr(order, "eTradeOnly"):
            order.eTradeOnly = False
        if hasattr(order, "firmQuoteOnly"):
            order.firmQuoteOnly = False

        if spec.is_adjustable and spec.trigger_price is not None and spec.adjusted_stop_price is not None:
            order.triggerPrice = float(spec.trigger_price)
            order.adjustedOrderType = "STP"
            order.adjustedStopPrice = float(spec.adjusted_stop_price)

        return order


def _unused(value: Any) -> None:
    """Explicitly mark currently-unused callback values."""
    _ = value
