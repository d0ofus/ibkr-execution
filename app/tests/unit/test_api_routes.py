"""TestClient tests for FastAPI control-plane routes with dependency overrides."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from fastapi.testclient import TestClient

from app.api.http_server import create_http_app
from app.api.routes import get_contract_service, get_order_manager, get_pinned_contract_reader
from app.config import Settings
from app.domain.enums import EnvironmentMode, Side
from app.domain.errors import ContractNotFoundError, PinnedContractRequiredError
from app.domain.models import ContractRef, OrderIntent, PinnedContract, utc_now


@dataclass(frozen=True)
class FakeCandidate:
    symbol: str
    con_id: int
    exchange: str
    primary_exchange: str | None
    sec_type: str
    currency: str


@dataclass
class FakeContractService:
    candidates_by_symbol: dict[str, list[FakeCandidate]] = field(default_factory=dict)
    pins: list[PinnedContract] = field(default_factory=list)

    def resolve_candidates(self, symbol: str) -> list[FakeCandidate]:
        key = symbol.upper()
        candidates = self.candidates_by_symbol.get(key, [])
        if not candidates:
            raise ContractNotFoundError(f"No candidates for {key}")
        return list(candidates)

    def pin_contract(self, symbol: str, environment: EnvironmentMode, selected_con_id: int) -> PinnedContract:
        for item in self.resolve_candidates(symbol):
            if item.con_id == selected_con_id:
                pin = PinnedContract(
                    symbol=item.symbol,
                    environment=environment,
                    con_id=item.con_id,
                    exchange=item.exchange,
                    primary_exchange=item.primary_exchange,
                    sec_type=item.sec_type,
                    currency=item.currency,
                    pinned_at=utc_now(),
                )
                self.pins = [
                    existing
                    for existing in self.pins
                    if not (existing.symbol == pin.symbol and existing.environment == pin.environment)
                ]
                self.pins.append(pin)
                return pin
        raise ContractNotFoundError(f"conId {selected_con_id} not found for {symbol}")

    def require_pinned_contract(self, symbol: str, environment: EnvironmentMode = EnvironmentMode.PAPER) -> ContractRef:
        key = symbol.upper()
        for item in self.pins:
            if item.symbol == key and item.environment == environment and item.is_active:
                return ContractRef(
                    symbol=item.symbol,
                    con_id=item.con_id,
                    exchange=item.exchange,
                    primary_exchange=item.primary_exchange,
                    sec_type=item.sec_type,
                    currency=item.currency,
                )
        raise PinnedContractRequiredError(f"No active pin for {key}")

    def list_active(self, environment: EnvironmentMode | None = None) -> list[PinnedContract]:
        if environment is None:
            return list(self.pins)
        return [item for item in self.pins if item.environment == environment]


@dataclass
class FakeOrderManager:
    trades: dict[str, OrderIntent] = field(default_factory=dict)

    def submit_intent(self, intent: OrderIntent) -> str:
        trade_id = f"trade-{len(self.trades) + 1}"
        self.trades[trade_id] = intent
        return trade_id

    def cancel_trade(self, trade_id: str) -> None:
        self.trades.pop(trade_id, None)

    def list_open_trades(self) -> list[str]:
        return sorted(self.trades.keys())


def _build_client(*, live_trading: bool = False, ack: str = "", dry_run: bool = False) -> tuple[TestClient, FakeContractService, FakeOrderManager]:
    settings = Settings(
        live_trading=live_trading,
        ack_live_trading=ack,
        dry_run=dry_run,
        ibkr_account="DU123456",
    )
    app = create_http_app(settings=settings)

    contract_service = FakeContractService(
        candidates_by_symbol={
            "AAPL": [FakeCandidate("AAPL", 265598, "SMART", "NASDAQ", "STK", "USD")],
            "MSFT": [FakeCandidate("MSFT", 272093, "SMART", "NASDAQ", "STK", "USD")],
        }
    )
    order_manager = FakeOrderManager()

    app.dependency_overrides[get_contract_service] = lambda: contract_service
    app.dependency_overrides[get_pinned_contract_reader] = lambda: contract_service
    app.dependency_overrides[get_order_manager] = lambda: order_manager

    return TestClient(app), contract_service, order_manager


def test_health_and_status_endpoints() -> None:
    client, _, _ = _build_client()

    health = client.get("/health")
    assert health.status_code == 200
    assert health.json()["status"] == "ok"

    status = client.get("/status")
    assert status.status_code == 200
    payload = status.json()
    assert payload["mode"] == "paper"
    assert payload["live_armed"] is False
    assert payload["account"] == "DU123456"


def test_contracts_resolve_pin_and_list() -> None:
    client, _, _ = _build_client()

    resolved = client.post("/contracts/resolve", json={"symbol": "AAPL"})
    assert resolved.status_code == 200
    assert resolved.json()["candidates"][0]["con_id"] == 265598

    pinned = client.post(
        "/contracts/pin",
        json={"symbol": "AAPL", "con_id": 265598, "environment": "paper"},
    )
    assert pinned.status_code == 200
    assert pinned.json()["symbol"] == "AAPL"

    listed = client.get("/contracts/pinned")
    assert listed.status_code == 200
    assert len(listed.json()) == 1


def test_orders_intent_requires_pin_and_then_submits() -> None:
    client, _, order_manager = _build_client()

    rejected = client.post(
        "/orders/intent",
        json={
            "symbol": "MSFT",
            "side": "BUY",
            "entry_price": "100",
            "stop_price": "99",
            "risk_dollars": "100",
        },
    )
    assert rejected.status_code == 200
    assert rejected.json()["accepted"] is False

    _ = client.post(
        "/contracts/pin",
        json={"symbol": "MSFT", "con_id": 272093, "environment": "paper"},
    )

    accepted = client.post(
        "/orders/intent",
        json={
            "symbol": "MSFT",
            "side": Side.BUY.value,
            "entry_price": "100",
            "stop_price": "99",
            "risk_dollars": "100",
        },
    )
    assert accepted.status_code == 200
    body = accepted.json()
    assert body["accepted"] is True
    assert body["trade_id"] == "trade-1"
    assert len(order_manager.trades) == 1


def test_orders_open_and_cancel() -> None:
    client, contract_service, _ = _build_client()
    _ = contract_service.pin_contract("AAPL", EnvironmentMode.PAPER, 265598)

    _ = client.post(
        "/orders/intent",
        json={
            "symbol": "AAPL",
            "side": "BUY",
            "entry_price": "100",
            "stop_price": "99",
            "risk_dollars": "100",
        },
    )

    open_before = client.get("/orders/open")
    assert open_before.status_code == 200
    assert open_before.json()["trade_ids"] == ["trade-1"]

    cancelled = client.post("/orders/cancel/trade-1")
    assert cancelled.status_code == 200
    assert cancelled.json()["cancelled"] is True

    open_after = client.get("/orders/open")
    assert open_after.status_code == 200
    assert open_after.json()["trade_ids"] == []


def test_strategies_start_stop_and_kill_switch() -> None:
    client, _, _ = _build_client()

    start = client.post("/strategies/start", json={"strategy_id": "orb-aapl"})
    assert start.status_code == 200
    assert start.json()["running"] is True

    listed = client.get("/strategies")
    assert listed.status_code == 200
    assert listed.json()["strategies"][0]["strategy_id"] == "orb-aapl"

    stop = client.post("/strategies/stop", json={"strategy_id": "orb-aapl"})
    assert stop.status_code == 200
    assert stop.json()["running"] is False

    killed = client.post("/kill")
    assert killed.status_code == 200
    assert killed.json()["kill_switch"] is True

    blocked = client.post("/strategies/start", json={"strategy_id": "orb-aapl"})
    assert blocked.status_code == 409


def test_arm_live_requires_env_gates_and_switches_status_mode() -> None:
    client_blocked, _, _ = _build_client(live_trading=False, ack="")
    blocked = client_blocked.post("/arm_live")
    assert blocked.status_code == 400

    client_live, contract_service, _ = _build_client(live_trading=True, ack="I_UNDERSTAND")
    armed = client_live.post("/arm_live")
    assert armed.status_code == 200
    assert armed.json()["armed"] is True
    assert armed.json()["mode"] == "live"

    status_payload = client_live.get("/status").json()
    assert status_payload["mode"] == "live"

    _ = contract_service.pin_contract("AAPL", EnvironmentMode.LIVE, 265598)
    order = client_live.post(
        "/orders/intent",
        json={
            "symbol": "AAPL",
            "side": "BUY",
            "entry_price": str(Decimal("150")),
            "stop_price": str(Decimal("149")),
            "risk_dollars": str(Decimal("100")),
        },
    )
    assert order.status_code == 200
    assert order.json()["accepted"] is True


def test_orders_intent_rejected_when_dry_run_enabled() -> None:
    client, contract_service, _ = _build_client(dry_run=True)
    _ = contract_service.pin_contract("AAPL", EnvironmentMode.PAPER, 265598)

    response = client.post(
        "/orders/intent",
        json={
            "symbol": "AAPL",
            "side": "BUY",
            "entry_price": "100",
            "stop_price": "99",
            "risk_dollars": "100",
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["accepted"] is False
    assert "dry_run" in payload["reason"]
