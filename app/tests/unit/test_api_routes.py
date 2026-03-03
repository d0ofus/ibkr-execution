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
from app.execution.orb_runner import OrbParameters, OrbRowState


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


@dataclass
class FakeBrokerClient:
    connected: bool = True
    profile_switches: list[tuple[str, int, int]] = field(default_factory=list)

    def is_connected(self) -> bool:
        return self.connected

    def switch_connection_profile(self, *, host: str, port: int, client_id: int) -> None:
        self.profile_switches.append((host, port, client_id))
        self.connected = True


@dataclass
class FakeWorkspaceSettings:
    user_key: str
    environment: EnvironmentMode
    settings_json: str


@dataclass
class FakeWorkspaceSettingsRepo:
    items: dict[tuple[str, EnvironmentMode], FakeWorkspaceSettings] = field(default_factory=dict)

    def get(self, *, user_key: str, environment: EnvironmentMode) -> FakeWorkspaceSettings | None:
        return self.items.get((user_key, environment))

    def upsert(self, *, user_key: str, environment: EnvironmentMode, settings_json: str) -> FakeWorkspaceSettings:
        item = FakeWorkspaceSettings(user_key=user_key, environment=environment, settings_json=settings_json)
        self.items[(user_key, environment)] = item
        return item


@dataclass
class FakeMarketDataService:
    rows: list[dict[str, str]] = field(default_factory=list)
    columns: list[str] = field(default_factory=lambda: ["last", "bid"])

    def ensure_workspace(self, workspace_key: str) -> None:
        _ = workspace_key

    def get_rows(self, workspace_key: str):  # type: ignore[no-untyped-def]
        _ = workspace_key
        return [
            type(
                "Row",
                (),
                {
                    "row_id": row["row_id"],
                    "symbol": row["symbol"],
                    "sec_type": row["sec_type"],
                    "exchange": row["exchange"],
                    "currency": row["currency"],
                    "con_id": None,
                    "primary_exchange": None,
                },
            )()
            for row in self.rows
        ]

    def set_rows(self, workspace_key: str, rows):  # type: ignore[no-untyped-def]
        _ = workspace_key
        self.rows = [
            {
                "row_id": row.row_id,
                "symbol": row.symbol,
                "sec_type": row.sec_type,
                "exchange": row.exchange,
                "currency": row.currency,
            }
            for row in rows
        ]
        return rows

    def delete_row(self, workspace_key: str, row_id: str) -> bool:
        _ = workspace_key
        before = len(self.rows)
        self.rows = [row for row in self.rows if row["row_id"] != row_id]
        return len(self.rows) < before

    def get_columns(self, workspace_key: str) -> list[str]:
        _ = workspace_key
        return list(self.columns)

    def set_columns(self, workspace_key: str, columns: list[str]) -> list[str]:
        _ = workspace_key
        self.columns = list(columns)
        return list(self.columns)

    def is_connected(self) -> bool:
        return True

    def is_feed_healthy(self) -> bool:
        return True

    def register_session(self, *, session_id: str, workspace_key: str):  # type: ignore[no-untyped-def]
        _ = (session_id, workspace_key)
        raise RuntimeError("not used in this test")

    def unregister_session(self, session_id: str) -> None:
        _ = session_id


@dataclass
class FakeOrbRunner:
    items: list[OrbRowState] = field(default_factory=list)

    def start(self, *, workspace_key: str, row_ids: list[str], params: OrbParameters) -> list[OrbRowState]:
        _ = (workspace_key, params)
        self.items = [
            OrbRowState(
                workspace_key=workspace_key,
                row_id=row_id,
                symbol="AAPL",
                qty=params.qty,
                x1=params.x1,
                x2=params.x2,
                state="ARMED",
            )
            for row_id in row_ids
        ]
        return self.items

    def stop(self, *, workspace_key: str, row_ids: list[str], stop_all: bool) -> list[OrbRowState]:
        _ = workspace_key
        if stop_all:
            for item in self.items:
                item.state = "IDLE"
            return self.items
        for item in self.items:
            if item.row_id in row_ids:
                item.state = "IDLE"
        return self.items

    def list_status(self, *, workspace_key: str) -> list[OrbRowState]:
        _ = workspace_key
        return self.items


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

    workspace_repo = FakeWorkspaceSettingsRepo()
    app.state.workspace_settings_repository = workspace_repo
    app.state.market_data_service = FakeMarketDataService()
    app.state.orb_runner = FakeOrbRunner()
    app.state.broker_client = FakeBrokerClient()
    app.state.broker_profiles = {
        EnvironmentMode.PAPER: type("Profile", (), {"host": "127.0.0.1", "port": 4002, "client_id": 1, "account": "DU123456"})(),
        EnvironmentMode.LIVE: type("Profile", (), {"host": "127.0.0.1", "port": 4001, "client_id": 2, "account": "U123456"})(),
    }

    return TestClient(app), contract_service, order_manager


def test_health_and_status_endpoints() -> None:
    client, _, _ = _build_client()

    ui_root = client.get("/")
    assert ui_root.status_code == 200
    assert "IBKR Exec Control Panel" in ui_root.text

    ui_page = client.get("/ui")
    assert ui_page.status_code == 200
    assert "Quick Submit" in ui_page.text

    strategy_list_page = client.get("/strategy-list")
    assert strategy_list_page.status_code == 200
    assert "Strategy List" in strategy_list_page.text

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


def test_strategy_definition_upsert_and_fetch() -> None:
    client, _, _ = _build_client()
    payload = {
        "source_format": "yaml",
        "source_payload": 'version: "1.0"\nstrategy_id: "orb-test"\nsymbol: "AAPL"\nrisk:\n  risk_dollars: 100\nactions:\n  - kind: enter_long\n    params:\n      initial_stop: session_low\n',
    }

    upserted = client.post("/strategies/upsert", json=payload)
    assert upserted.status_code == 200
    strategy_id = upserted.json()["strategy_id"]

    fetched = client.get(f"/strategies/definition/{strategy_id}")
    assert fetched.status_code == 200
    body = fetched.json()
    assert body["strategy_id"] == strategy_id
    assert body["source_format"] == "yaml"


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


def test_workspace_and_orb_routes() -> None:
    client, _, _ = _build_client()

    state = client.get("/workspace/state")
    assert state.status_code == 200
    assert state.json()["workspace_key"].endswith(":paper")

    updated_rows = client.put(
        "/workspace/rows",
        json={
            "rows": [
                {
                    "row_id": "row-1",
                    "symbol": "AAPL",
                    "sec_type": "STK",
                    "exchange": "SMART",
                    "currency": "USD",
                }
            ]
        },
    )
    assert updated_rows.status_code == 200
    assert len(updated_rows.json()["rows"]) == 1

    updated_columns = client.put("/workspace/columns", json={"columns": ["last", "ask"]})
    assert updated_columns.status_code == 200
    assert updated_columns.json()["columns"] == ["last", "ask"]

    started = client.post(
        "/execution/orb/start",
        json={"row_ids": ["row-1"], "params": {"qty": 90, "x1": "101.00", "x2": "102.00"}},
    )
    assert started.status_code == 200
    assert started.json()["items"][0]["state"] == "ARMED"

    status_resp = client.get("/execution/orb/status")
    assert status_resp.status_code == 200
    assert len(status_resp.json()["items"]) == 1

    stopped = client.post("/execution/orb/stop", json={"row_ids": ["row-1"], "stop_all": False})
    assert stopped.status_code == 200
    assert stopped.json()["items"][0]["state"] == "IDLE"


def test_runtime_readiness_and_profile_switch_routes() -> None:
    client, _, _ = _build_client(live_trading=True, ack="I_UNDERSTAND", dry_run=False)

    readiness = client.get("/runtime/readiness")
    assert readiness.status_code == 200
    payload = readiness.json()
    assert payload["selected_profile"] == "paper"
    assert payload["paper_profile"]["port"] == 4002
    assert payload["live_profile"]["port"] == 4001

    switched = client.put("/runtime/profile", json={"profile": "live"})
    assert switched.status_code == 200
    switched_payload = switched.json()
    assert switched_payload["selected_profile"] == "live"

    updated = client.put(
        "/runtime/profile/config",
        json={
            "profile": "live",
            "host": "127.0.0.1",
            "port": 7496,
            "client_id": 11,
            "account": "U999999",
        },
    )
    assert updated.status_code == 200
    updated_payload = updated.json()
    assert updated_payload["live_profile"]["port"] == 7496
    assert updated_payload["live_profile"]["client_id"] == 11
