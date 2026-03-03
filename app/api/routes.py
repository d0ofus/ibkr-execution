"""Route registration for the control plane HTTP API."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
import json
from pathlib import Path
from typing import Literal, Protocol, cast
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Request, WebSocket, WebSocketDisconnect, status
from fastapi.responses import HTMLResponse, StreamingResponse

from app.api.schemas import (
    ArmLiveResponse,
    CancelOrderResponse,
    ContractCandidateResponse,
    ContractPinRequest,
    ContractResolveRequest,
    ContractResolveResponse,
    HealthResponse,
    KillResponse,
    OpenOrdersResponse,
    OrbStartRequest,
    OrbStateItem,
    OrbStatusResponse,
    OrbStopRequest,
    RuntimeProfileResponse,
    RuntimeProfileSwitchRequest,
    RuntimeReadinessResponse,
    OrderIntentRequest,
    OrderIntentResponse,
    PinnedContractResponse,
    StatusResponse,
    StrategiesResponse,
    StrategyCommandRequest,
    StrategyDefinitionResponse,
    StrategyStatus,
    StrategyUpsertRequest,
    WorkspaceColumnsRequest,
    WorkspaceRowInput,
    WorkspaceRowsRequest,
    WorkspaceStateResponse,
)
from app.config import Settings
from app.data.market_data_service import MarketDataService, WorkspaceRow, build_workspace_key, default_columns
from app.domain.enums import EnvironmentMode
from app.domain.errors import ExecBotError
from app.domain.models import ContractRef, OrderIntent, PinnedContract, WorkspaceSettings
from app.execution.orb_runner import OrbParameters as OrbRuntimeParameters
from app.execution.orb_runner import OrbRowState
from app.execution.strategy_runtime import StrategyDefinitionView, StrategyRuntimeStatus


class ContractCandidateLike(Protocol):
    """Protocol for contract candidate responses."""

    symbol: str
    con_id: int
    exchange: str
    primary_exchange: str | None
    sec_type: str
    currency: str


class ContractServiceLike(Protocol):
    """Protocol for contract-related API operations."""

    def resolve_candidates(self, symbol: str) -> list[ContractCandidateLike]:
        """Return qualified candidate contracts for a symbol."""

    def pin_contract(self, symbol: str, environment: EnvironmentMode, selected_con_id: int) -> PinnedContract:
        """Persist a pinned contract selection."""

    def require_pinned_contract(
        self,
        symbol: str,
        environment: EnvironmentMode = EnvironmentMode.PAPER,
    ) -> ContractRef:
        """Validate that symbol has a pinned contract."""


class PinnedContractReaderLike(Protocol):
    """Protocol for reading active pinned contracts."""

    def list_active(self, environment: EnvironmentMode | None = None) -> list[PinnedContract]:
        """Return currently active pinned contracts."""


class OrderManagerLike(Protocol):
    """Protocol for order-manager API operations."""

    def submit_intent(self, intent: OrderIntent) -> str:
        """Submit an order intent and return trade ID."""

    def cancel_trade(self, trade_id: str) -> None:
        """Cancel an active trade."""

    def list_open_trades(self) -> list[str]:
        """List currently open trades."""

    def set_environment(self, environment: EnvironmentMode) -> None:
        """Switch execution environment context."""


class WorkspaceSettingsRepositoryLike(Protocol):
    """Protocol for workspace settings persistence."""

    def get(self, *, user_key: str, environment: EnvironmentMode) -> WorkspaceSettings | None:
        """Load settings by user/environment key."""

    def upsert(
        self,
        *,
        user_key: str,
        environment: EnvironmentMode,
        settings_json: str,
    ) -> WorkspaceSettings:
        """Persist settings by user/environment key."""


class BrokerClientLike(Protocol):
    """Broker connectivity/runtime switching operations."""

    def is_connected(self) -> bool:
        """Return broker connection state."""

    def switch_connection_profile(self, *, host: str, port: int, client_id: int) -> None:
        """Reconnect using a different host/port/client profile."""


class OrbRunnerLike(Protocol):
    """Protocol for workspace ORB runtime control."""

    def start(self, *, workspace_key: str, row_ids: list[str], params: OrbRuntimeParameters) -> list[OrbRowState]:
        """Start ORB for selected rows."""

    def stop(self, *, workspace_key: str, row_ids: list[str], stop_all: bool) -> list[OrbRowState]:
        """Stop ORB for selected rows or all rows."""

    def list_status(self, *, workspace_key: str) -> list[OrbRowState]:
        """Return ORB status snapshots."""


@dataclass
class StrategyRegistry:
    """Fallback in-memory strategy registry used when runtime orchestration is not provided."""

    running: dict[str, bool] = field(default_factory=dict)
    definitions: dict[str, StrategyDefinitionResponse] = field(default_factory=dict)

    def list(self) -> list[StrategyStatus]:
        return [
            StrategyStatus(
                strategy_id=strategy_id,
                running=is_running,
                symbol=(self.definitions[strategy_id].symbol if strategy_id in self.definitions else None),
                enabled=(self.definitions[strategy_id].enabled if strategy_id in self.definitions else None),
            )
            for strategy_id, is_running in sorted(self.running.items())
        ]

    def start(self, strategy_id: str) -> StrategyStatus:
        self.running[strategy_id] = True
        return StrategyStatus(strategy_id=strategy_id, running=True)

    def stop(self, strategy_id: str) -> StrategyStatus:
        self.running[strategy_id] = False
        return StrategyStatus(strategy_id=strategy_id, running=False)

    def stop_all(self) -> None:
        for key in list(self.running.keys()):
            self.running[key] = False

    def upsert_definition(
        self,
        *,
        source_payload: str,
        source_format: Literal["yaml", "json"],
    ) -> StrategyStatus:
        strategy_id = f"draft-{len(self.definitions) + 1}"
        self.definitions[strategy_id] = StrategyDefinitionResponse(
            strategy_id=strategy_id,
            source_format=source_format,
            source_payload=source_payload,
            symbol="UNKNOWN",
            enabled=True,
        )
        self.running.setdefault(strategy_id, False)
        return StrategyStatus(strategy_id=strategy_id, running=self.running[strategy_id])

    def get_definition(self, strategy_id: str) -> StrategyDefinitionResponse:
        item = self.definitions.get(strategy_id)
        if item is None:
            raise ValueError("strategy definition not found")
        return item


class StrategyRegistryLike(Protocol):
    """Protocol for strategy registry/runtime operations."""

    def list(self) -> list[StrategyStatus | StrategyRuntimeStatus]:
        """List strategy statuses."""

    def start(self, strategy_id: str) -> StrategyStatus | StrategyRuntimeStatus:
        """Start a strategy."""

    def stop(self, strategy_id: str) -> StrategyStatus | StrategyRuntimeStatus:
        """Stop a strategy."""

    def stop_all(self) -> None:
        """Stop all strategies."""

    def upsert_definition(
        self,
        *,
        source_payload: str,
        source_format: Literal["yaml", "json"],
    ) -> StrategyStatus | StrategyRuntimeStatus:
        """Create or update strategy definition."""

    def get_definition(self, strategy_id: str) -> StrategyDefinitionView | StrategyDefinitionResponse:
        """Get strategy definition source."""


@dataclass
class ControlPlaneRuntime:
    """Mutable runtime controls for kill/live arming gates."""

    live_armed: bool = False
    kill_switch: bool = False
    selected_profile: EnvironmentMode = EnvironmentMode.PAPER


@dataclass
class BrokerConnectionProfile:
    """Broker socket profile for one environment."""

    host: str
    port: int
    client_id: int
    account: str


def get_settings(request: Request) -> Settings:
    """Resolve app settings from request app state."""
    settings_obj = getattr(request.app.state, "settings", None)
    if not isinstance(settings_obj, Settings):
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="settings unavailable")
    return settings_obj


def get_runtime(request: Request) -> ControlPlaneRuntime:
    """Resolve control-plane runtime from request app state."""
    runtime_obj = getattr(request.app.state, "runtime", None)
    if not isinstance(runtime_obj, ControlPlaneRuntime):
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="runtime unavailable")
    return runtime_obj


def get_contract_service(request: Request) -> ContractServiceLike:
    """Resolve contract service dependency from app state."""
    service_obj = getattr(request.app.state, "contract_service", None)
    if service_obj is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="contract service unavailable",
        )
    return cast(ContractServiceLike, service_obj)


def get_pinned_contract_reader(request: Request) -> PinnedContractReaderLike:
    """Resolve pinned-contract reader dependency from app state."""
    reader_obj = getattr(request.app.state, "pinned_contract_reader", None)
    if reader_obj is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="pinned contract reader unavailable",
        )
    return cast(PinnedContractReaderLike, reader_obj)


def get_order_manager(request: Request) -> OrderManagerLike:
    """Resolve order-manager dependency from app state."""
    manager_obj = getattr(request.app.state, "order_manager", None)
    if manager_obj is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="order manager unavailable",
        )
    return cast(OrderManagerLike, manager_obj)


def get_broker_client(request: Request) -> BrokerClientLike:
    """Resolve broker client dependency from app state."""
    client_obj = getattr(request.app.state, "broker_client", None)
    if client_obj is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="broker client unavailable",
        )
    return cast(BrokerClientLike, client_obj)


def get_broker_profiles(request: Request) -> dict[EnvironmentMode, BrokerConnectionProfile]:
    """Resolve broker connection profiles from app state."""
    profiles = getattr(request.app.state, "broker_profiles", None)
    if not isinstance(profiles, dict):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="broker profiles unavailable",
        )
    return cast(dict[EnvironmentMode, BrokerConnectionProfile], profiles)


def get_strategy_registry(request: Request) -> StrategyRegistryLike:
    """Resolve strategy registry dependency from app state."""
    registry_obj = getattr(request.app.state, "strategy_registry", None)
    if registry_obj is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="strategy registry unavailable",
        )
    return cast(StrategyRegistryLike, registry_obj)


def get_market_data_service(request: Request) -> MarketDataService:
    """Resolve market-data workspace service from app state."""
    service_obj = getattr(request.app.state, "market_data_service", None)
    if service_obj is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="market data service unavailable")
    return cast(MarketDataService, service_obj)


def get_workspace_settings_repo(request: Request) -> WorkspaceSettingsRepositoryLike:
    """Resolve workspace settings repository from app state."""
    repo = getattr(request.app.state, "workspace_settings_repository", None)
    if repo is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="workspace settings repository unavailable")
    return cast(WorkspaceSettingsRepositoryLike, repo)


def get_orb_runner(request: Request) -> OrbRunnerLike:
    """Resolve ORB runner from app state."""
    runner = getattr(request.app.state, "orb_runner", None)
    if runner is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="orb runner unavailable")
    return cast(OrbRunnerLike, runner)

def build_router() -> APIRouter:
    """Construct API router with control-plane endpoints."""
    router = APIRouter()
    ui_path = Path(__file__).with_name("web_ui.html")
    workspace_ui_path = Path(__file__).with_name("workspace_ui.html")
    strategy_list_ui_path = Path(__file__).with_name("strategy_list_ui.html")

    @router.get("/", response_class=HTMLResponse, include_in_schema=False)
    def web_ui_root() -> HTMLResponse:
        return HTMLResponse(content=ui_path.read_text(encoding="utf-8"))

    @router.get("/ui", response_class=HTMLResponse, include_in_schema=False)
    def web_ui_page() -> HTMLResponse:
        return HTMLResponse(content=ui_path.read_text(encoding="utf-8"))

    @router.get("/workspace", response_class=HTMLResponse, include_in_schema=False)
    def workspace_ui_page() -> HTMLResponse:
        return HTMLResponse(content=workspace_ui_path.read_text(encoding="utf-8"))

    @router.get("/strategy-list", response_class=HTMLResponse, include_in_schema=False)
    def strategy_list_ui_page() -> HTMLResponse:
        return HTMLResponse(content=strategy_list_ui_path.read_text(encoding="utf-8"))

    @router.websocket("/ws/workspace")
    async def workspace_websocket(websocket: WebSocket) -> None:
        await websocket.accept()
        settings = getattr(websocket.app.state, "settings", None)
        runtime = getattr(websocket.app.state, "runtime", None)
        market_data_service = getattr(websocket.app.state, "market_data_service", None)
        workspace_repo = getattr(websocket.app.state, "workspace_settings_repository", None)
        if not isinstance(settings, Settings):
            raise RuntimeError("settings unavailable")
        if not isinstance(runtime, ControlPlaneRuntime):
            raise RuntimeError("runtime unavailable")
        if market_data_service is None:
            raise RuntimeError("market data service unavailable")
        if workspace_repo is None:
            raise RuntimeError("workspace settings repository unavailable")
        workspace_repo_typed = cast(WorkspaceSettingsRepositoryLike, workspace_repo)

        user_key, mode, workspace_key = _workspace_scope(settings=settings, runtime=runtime)
        _load_workspace_from_repo(
            market_data_service=market_data_service,
            workspace_repo=workspace_repo_typed,
            user_key=user_key,
            mode=mode,
            workspace_key=workspace_key,
        )

        session_id = str(uuid4())
        queue = market_data_service.register_session(session_id=session_id, workspace_key=workspace_key)

        try:
            while True:
                queue_task = asyncio.create_task(queue.get())
                receive_task = asyncio.create_task(websocket.receive_text())
                done, pending = await asyncio.wait(
                    {queue_task, receive_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if queue_task in done:
                    envelope = queue_task.result()
                    await websocket.send_json(envelope.model_dump(mode="json"))

                if receive_task in done:
                    _ = receive_task.result()

                for task in pending:
                    task.cancel()
        except (WebSocketDisconnect, RuntimeError):
            market_data_service.unregister_session(session_id)

    @router.get("/sse/workspace")
    async def workspace_sse(
        request: Request,
        settings: Settings = Depends(get_settings),
        runtime: ControlPlaneRuntime = Depends(get_runtime),
        market_data_service: MarketDataService = Depends(get_market_data_service),
        workspace_repo: WorkspaceSettingsRepositoryLike = Depends(get_workspace_settings_repo),
    ) -> StreamingResponse:
        user_key, mode, workspace_key = _workspace_scope(settings=settings, runtime=runtime)
        _load_workspace_from_repo(
            market_data_service=market_data_service,
            workspace_repo=workspace_repo,
            user_key=user_key,
            mode=mode,
            workspace_key=workspace_key,
        )

        session_id = str(uuid4())
        queue = market_data_service.register_session(session_id=session_id, workspace_key=workspace_key)

        async def _event_stream() -> AsyncIterator[str]:
            try:
                while True:
                    if await request.is_disconnected():
                        break
                    envelope = await queue.get()
                    payload = json.dumps(envelope.model_dump(mode="json"), separators=(",", ":"))
                    yield f"data: {payload}\n\n"
            finally:
                market_data_service.unregister_session(session_id)

        return StreamingResponse(
            _event_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
        )

    @router.get("/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        return HealthResponse(status="ok")

    @router.get("/status", response_model=StatusResponse)
    def status_endpoint(
        settings: Settings = Depends(get_settings),
        runtime: ControlPlaneRuntime = Depends(get_runtime),
        profiles: dict[EnvironmentMode, BrokerConnectionProfile] = Depends(get_broker_profiles),
    ) -> StatusResponse:
        mode = _current_mode(settings=settings, runtime=runtime)
        selected_account = profiles.get(runtime.selected_profile, BrokerConnectionProfile("", 0, 0, "")).account
        return StatusResponse(
            mode=mode,
            live_enabled=settings.live_trading,
            live_armed=runtime.live_armed,
            dry_run=settings.dry_run,
            kill_switch=runtime.kill_switch,
            account=selected_account or settings.ibkr_account,
        )

    @router.get("/runtime/readiness", response_model=RuntimeReadinessResponse)
    def runtime_readiness(
        settings: Settings = Depends(get_settings),
        runtime: ControlPlaneRuntime = Depends(get_runtime),
        broker_client: BrokerClientLike = Depends(get_broker_client),
        profiles: dict[EnvironmentMode, BrokerConnectionProfile] = Depends(get_broker_profiles),
    ) -> RuntimeReadinessResponse:
        paper = profiles[EnvironmentMode.PAPER]
        live = profiles[EnvironmentMode.LIVE]
        mode = _current_mode(settings=settings, runtime=runtime)
        connected = broker_client.is_connected()
        guidance = _readiness_guidance(
            settings=settings,
            runtime=runtime,
            connected=connected,
        )
        return RuntimeReadinessResponse(
            selected_profile=runtime.selected_profile,
            broker_connected=connected,
            kill_switch=runtime.kill_switch,
            dry_run=settings.dry_run,
            live_trading_enabled=settings.live_trading,
            ack_configured=(settings.ack_live_trading == "I_UNDERSTAND"),
            live_armed=runtime.live_armed,
            current_mode=mode,
            paper_profile=RuntimeProfileResponse(
                host=paper.host,
                port=paper.port,
                client_id=paper.client_id,
                account=paper.account,
            ),
            live_profile=RuntimeProfileResponse(
                host=live.host,
                port=live.port,
                client_id=live.client_id,
                account=live.account,
            ),
            guidance=guidance,
        )

    @router.put("/runtime/profile", response_model=RuntimeReadinessResponse)
    def switch_runtime_profile(
        payload: RuntimeProfileSwitchRequest,
        settings: Settings = Depends(get_settings),
        runtime: ControlPlaneRuntime = Depends(get_runtime),
        broker_client: BrokerClientLike = Depends(get_broker_client),
        profiles: dict[EnvironmentMode, BrokerConnectionProfile] = Depends(get_broker_profiles),
    ) -> RuntimeReadinessResponse:
        if payload.profile not in {EnvironmentMode.PAPER, EnvironmentMode.LIVE}:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="profile must be paper or live")

        selected = profiles[payload.profile]
        try:
            broker_client.switch_connection_profile(
                host=selected.host,
                port=selected.port,
                client_id=selected.client_id,
            )
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"failed to switch profile: {exc}",
            ) from exc

        runtime.selected_profile = payload.profile
        if payload.profile == EnvironmentMode.PAPER:
            runtime.live_armed = False
        return runtime_readiness(
            settings=settings,
            runtime=runtime,
            broker_client=broker_client,
            profiles=profiles,
        )

    @router.get("/workspace/state", response_model=WorkspaceStateResponse)
    def workspace_state(
        settings: Settings = Depends(get_settings),
        runtime: ControlPlaneRuntime = Depends(get_runtime),
        market_data_service: MarketDataService = Depends(get_market_data_service),
        workspace_repo: WorkspaceSettingsRepositoryLike = Depends(get_workspace_settings_repo),
    ) -> WorkspaceStateResponse:
        user_key, mode, workspace_key = _workspace_scope(settings=settings, runtime=runtime)
        _load_workspace_from_repo(
            market_data_service=market_data_service,
            workspace_repo=workspace_repo,
            user_key=user_key,
            mode=mode,
            workspace_key=workspace_key,
        )

        rows = [
            WorkspaceRowInput(
                row_id=row.row_id,
                symbol=row.symbol,
                sec_type=row.sec_type,
                exchange=row.exchange,
                currency=row.currency,
                con_id=row.con_id,
                primary_exchange=row.primary_exchange,
            )
            for row in market_data_service.get_rows(workspace_key)
        ]
        columns = market_data_service.get_columns(workspace_key)
        return WorkspaceStateResponse(
            workspace_key=workspace_key,
            rows=rows,
            columns=columns,
            connected=market_data_service.is_connected(),
            feed_healthy=market_data_service.is_feed_healthy(),
        )

    @router.put("/workspace/rows", response_model=WorkspaceStateResponse)
    def set_workspace_rows(
        payload: WorkspaceRowsRequest,
        settings: Settings = Depends(get_settings),
        runtime: ControlPlaneRuntime = Depends(get_runtime),
        market_data_service: MarketDataService = Depends(get_market_data_service),
        workspace_repo: WorkspaceSettingsRepositoryLike = Depends(get_workspace_settings_repo),
    ) -> WorkspaceStateResponse:
        user_key, mode, workspace_key = _workspace_scope(settings=settings, runtime=runtime)
        _load_workspace_from_repo(
            market_data_service=market_data_service,
            workspace_repo=workspace_repo,
            user_key=user_key,
            mode=mode,
            workspace_key=workspace_key,
        )

        rows = [
            WorkspaceRow(
                row_id=item.row_id,
                symbol=item.symbol,
                sec_type=item.sec_type,
                exchange=item.exchange,
                currency=item.currency,
                con_id=item.con_id,
                primary_exchange=item.primary_exchange,
            )
            for item in payload.rows
        ]
        market_data_service.set_rows(workspace_key, rows)
        columns = market_data_service.get_columns(workspace_key)
        _save_workspace_to_repo(
            workspace_repo=workspace_repo,
            user_key=user_key,
            mode=mode,
            rows=payload.rows,
            columns=columns,
        )

        return WorkspaceStateResponse(
            workspace_key=workspace_key,
            rows=payload.rows,
            columns=columns,
            connected=market_data_service.is_connected(),
            feed_healthy=market_data_service.is_feed_healthy(),
        )

    @router.delete("/workspace/rows/{row_id}", response_model=WorkspaceStateResponse)
    def delete_workspace_row(
        row_id: str,
        settings: Settings = Depends(get_settings),
        runtime: ControlPlaneRuntime = Depends(get_runtime),
        market_data_service: MarketDataService = Depends(get_market_data_service),
        workspace_repo: WorkspaceSettingsRepositoryLike = Depends(get_workspace_settings_repo),
    ) -> WorkspaceStateResponse:
        user_key, mode, workspace_key = _workspace_scope(settings=settings, runtime=runtime)
        _load_workspace_from_repo(
            market_data_service=market_data_service,
            workspace_repo=workspace_repo,
            user_key=user_key,
            mode=mode,
            workspace_key=workspace_key,
        )

        _ = market_data_service.delete_row(workspace_key, row_id)
        rows = [
            WorkspaceRowInput(
                row_id=item.row_id,
                symbol=item.symbol,
                sec_type=item.sec_type,
                exchange=item.exchange,
                currency=item.currency,
                con_id=item.con_id,
                primary_exchange=item.primary_exchange,
            )
            for item in market_data_service.get_rows(workspace_key)
        ]
        columns = market_data_service.get_columns(workspace_key)
        _save_workspace_to_repo(
            workspace_repo=workspace_repo,
            user_key=user_key,
            mode=mode,
            rows=rows,
            columns=columns,
        )

        return WorkspaceStateResponse(
            workspace_key=workspace_key,
            rows=rows,
            columns=columns,
            connected=market_data_service.is_connected(),
            feed_healthy=market_data_service.is_feed_healthy(),
        )

    @router.put("/workspace/columns", response_model=WorkspaceStateResponse)
    def set_workspace_columns(
        payload: WorkspaceColumnsRequest,
        settings: Settings = Depends(get_settings),
        runtime: ControlPlaneRuntime = Depends(get_runtime),
        market_data_service: MarketDataService = Depends(get_market_data_service),
        workspace_repo: WorkspaceSettingsRepositoryLike = Depends(get_workspace_settings_repo),
    ) -> WorkspaceStateResponse:
        user_key, mode, workspace_key = _workspace_scope(settings=settings, runtime=runtime)
        _load_workspace_from_repo(
            market_data_service=market_data_service,
            workspace_repo=workspace_repo,
            user_key=user_key,
            mode=mode,
            workspace_key=workspace_key,
        )

        columns = market_data_service.set_columns(workspace_key, payload.columns)
        rows = [
            WorkspaceRowInput(
                row_id=item.row_id,
                symbol=item.symbol,
                sec_type=item.sec_type,
                exchange=item.exchange,
                currency=item.currency,
                con_id=item.con_id,
                primary_exchange=item.primary_exchange,
            )
            for item in market_data_service.get_rows(workspace_key)
        ]
        _save_workspace_to_repo(
            workspace_repo=workspace_repo,
            user_key=user_key,
            mode=mode,
            rows=rows,
            columns=columns,
        )

        return WorkspaceStateResponse(
            workspace_key=workspace_key,
            rows=rows,
            columns=columns,
            connected=market_data_service.is_connected(),
            feed_healthy=market_data_service.is_feed_healthy(),
        )

    @router.post("/execution/orb/start", response_model=OrbStatusResponse)
    def start_workspace_orb(
        payload: OrbStartRequest,
        settings: Settings = Depends(get_settings),
        runtime: ControlPlaneRuntime = Depends(get_runtime),
        orb_runner: OrbRunnerLike = Depends(get_orb_runner),
        market_data_service: MarketDataService = Depends(get_market_data_service),
        workspace_repo: WorkspaceSettingsRepositoryLike = Depends(get_workspace_settings_repo),
    ) -> OrbStatusResponse:
        if runtime.kill_switch:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="kill switch active")

        user_key, mode, workspace_key = _workspace_scope(settings=settings, runtime=runtime)
        _load_workspace_from_repo(
            market_data_service=market_data_service,
            workspace_repo=workspace_repo,
            user_key=user_key,
            mode=mode,
            workspace_key=workspace_key,
        )

        params = OrbRuntimeParameters(qty=payload.params.qty, x1=payload.params.x1, x2=payload.params.x2)
        try:
            items = orb_runner.start(workspace_key=workspace_key, row_ids=payload.row_ids, params=params)
        except ExecBotError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

        return OrbStatusResponse(items=[_to_orb_status_item(item) for item in items])

    @router.post("/execution/orb/stop", response_model=OrbStatusResponse)
    def stop_workspace_orb(
        payload: OrbStopRequest,
        settings: Settings = Depends(get_settings),
        runtime: ControlPlaneRuntime = Depends(get_runtime),
        orb_runner: OrbRunnerLike = Depends(get_orb_runner),
        market_data_service: MarketDataService = Depends(get_market_data_service),
        workspace_repo: WorkspaceSettingsRepositoryLike = Depends(get_workspace_settings_repo),
    ) -> OrbStatusResponse:
        user_key, mode, workspace_key = _workspace_scope(settings=settings, runtime=runtime)
        _load_workspace_from_repo(
            market_data_service=market_data_service,
            workspace_repo=workspace_repo,
            user_key=user_key,
            mode=mode,
            workspace_key=workspace_key,
        )
        items = orb_runner.stop(
            workspace_key=workspace_key,
            row_ids=payload.row_ids,
            stop_all=payload.stop_all,
        )
        return OrbStatusResponse(items=[_to_orb_status_item(item) for item in items])

    @router.get("/execution/orb/status", response_model=OrbStatusResponse)
    def workspace_orb_status(
        settings: Settings = Depends(get_settings),
        runtime: ControlPlaneRuntime = Depends(get_runtime),
        orb_runner: OrbRunnerLike = Depends(get_orb_runner),
        market_data_service: MarketDataService = Depends(get_market_data_service),
        workspace_repo: WorkspaceSettingsRepositoryLike = Depends(get_workspace_settings_repo),
    ) -> OrbStatusResponse:
        user_key, mode, workspace_key = _workspace_scope(settings=settings, runtime=runtime)
        _load_workspace_from_repo(
            market_data_service=market_data_service,
            workspace_repo=workspace_repo,
            user_key=user_key,
            mode=mode,
            workspace_key=workspace_key,
        )
        items = orb_runner.list_status(workspace_key=workspace_key)
        return OrbStatusResponse(items=[_to_orb_status_item(item) for item in items])

    @router.post("/contracts/resolve", response_model=ContractResolveResponse)
    def resolve_contracts(
        payload: ContractResolveRequest,
        contract_service: ContractServiceLike = Depends(get_contract_service),
    ) -> ContractResolveResponse:
        try:
            candidates = contract_service.resolve_candidates(payload.symbol)
        except ExecBotError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

        return ContractResolveResponse(
            candidates=[
                ContractCandidateResponse(
                    symbol=item.symbol,
                    con_id=item.con_id,
                    exchange=item.exchange,
                    primary_exchange=item.primary_exchange,
                    sec_type=item.sec_type,
                    currency=item.currency,
                )
                for item in candidates
            ]
        )

    @router.post("/contracts/pin", response_model=PinnedContractResponse)
    def pin_contract(
        payload: ContractPinRequest,
        contract_service: ContractServiceLike = Depends(get_contract_service),
    ) -> PinnedContractResponse:
        try:
            pinned = contract_service.pin_contract(
                symbol=payload.symbol,
                environment=payload.environment,
                selected_con_id=payload.con_id,
            )
        except ExecBotError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

        return _to_pinned_response(pinned)

    @router.get("/contracts/pinned", response_model=list[PinnedContractResponse])
    def list_pinned_contracts(
        environment: EnvironmentMode | None = None,
        reader: PinnedContractReaderLike = Depends(get_pinned_contract_reader),
    ) -> list[PinnedContractResponse]:
        pins = reader.list_active(environment=environment)
        return [_to_pinned_response(item) for item in pins]

    @router.post("/orders/intent", response_model=OrderIntentResponse)
    def submit_order_intent(
        payload: OrderIntentRequest,
        runtime: ControlPlaneRuntime = Depends(get_runtime),
        settings: Settings = Depends(get_settings),
        contract_service: ContractServiceLike = Depends(get_contract_service),
        order_manager: OrderManagerLike = Depends(get_order_manager),
    ) -> OrderIntentResponse:
        if runtime.kill_switch:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="kill switch active")
        if settings.dry_run:
            return OrderIntentResponse(
                accepted=False,
                reason="dry_run is enabled; set DRY_RUN=false to transmit orders",
            )

        mode = _current_mode(settings=settings, runtime=runtime)
        try:
            contract_service.require_pinned_contract(payload.symbol, environment=mode)

            intent_id = payload.intent_id or str(uuid4())
            intent = OrderIntent(
                intent_id=intent_id,
                symbol=payload.symbol.upper(),
                side=payload.side,
                entry_price=payload.entry_price,
                stop_price=payload.stop_price,
                risk_dollars=payload.risk_dollars,
                strategy_id=payload.strategy_id,
            )
            trade_id = order_manager.submit_intent(intent)
        except ExecBotError as exc:
            return OrderIntentResponse(accepted=False, reason=str(exc))

        return OrderIntentResponse(accepted=True, trade_id=trade_id)

    @router.post("/orders/cancel/{trade_id}", response_model=CancelOrderResponse)
    def cancel_order(
        trade_id: str,
        order_manager: OrderManagerLike = Depends(get_order_manager),
    ) -> CancelOrderResponse:
        try:
            order_manager.cancel_trade(trade_id)
        except ExecBotError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return CancelOrderResponse(cancelled=True, trade_id=trade_id)

    @router.get("/orders/open", response_model=OpenOrdersResponse)
    def list_open_orders(order_manager: OrderManagerLike = Depends(get_order_manager)) -> OpenOrdersResponse:
        trade_ids = order_manager.list_open_trades()
        return OpenOrdersResponse(trade_ids=trade_ids)
    @router.get("/strategies", response_model=StrategiesResponse)
    def list_strategies(registry: StrategyRegistryLike = Depends(get_strategy_registry)) -> StrategiesResponse:
        raw_items = registry.list()
        return StrategiesResponse(
            strategies=[
                StrategyStatus(
                    strategy_id=item.strategy_id,
                    running=item.running,
                    symbol=getattr(item, "symbol", None),
                    enabled=getattr(item, "enabled", None),
                    last_error=getattr(item, "last_error", None),
                )
                for item in raw_items
            ]
        )

    @router.post("/strategies/upsert", response_model=StrategyStatus)
    def upsert_strategy_definition(
        payload: StrategyUpsertRequest,
        registry: StrategyRegistryLike = Depends(get_strategy_registry),
    ) -> StrategyStatus:
        try:
            result = registry.upsert_definition(
                source_payload=payload.source_payload,
                source_format=payload.source_format,
            )
        except ExecBotError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return StrategyStatus(
            strategy_id=result.strategy_id,
            running=result.running,
            symbol=getattr(result, "symbol", None),
            enabled=getattr(result, "enabled", None),
            last_error=getattr(result, "last_error", None),
        )

    @router.get("/strategies/definition/{strategy_id}", response_model=StrategyDefinitionResponse)
    def get_strategy_definition(
        strategy_id: str,
        registry: StrategyRegistryLike = Depends(get_strategy_registry),
    ) -> StrategyDefinitionResponse:
        try:
            definition = registry.get_definition(strategy_id)
        except Exception as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        return StrategyDefinitionResponse(
            strategy_id=definition.strategy_id,
            source_format=definition.source_format,
            source_payload=definition.source_payload,
            symbol=definition.symbol,
            enabled=definition.enabled,
        )

    @router.post("/strategies/start", response_model=StrategyStatus)
    def start_strategy(
        payload: StrategyCommandRequest,
        runtime: ControlPlaneRuntime = Depends(get_runtime),
        registry: StrategyRegistryLike = Depends(get_strategy_registry),
    ) -> StrategyStatus:
        if runtime.kill_switch:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="kill switch active")
        try:
            result = registry.start(payload.strategy_id)
        except ExecBotError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return StrategyStatus(
            strategy_id=result.strategy_id,
            running=result.running,
            symbol=getattr(result, "symbol", None),
            enabled=getattr(result, "enabled", None),
            last_error=getattr(result, "last_error", None),
        )

    @router.post("/strategies/stop", response_model=StrategyStatus)
    def stop_strategy(
        payload: StrategyCommandRequest,
        registry: StrategyRegistryLike = Depends(get_strategy_registry),
    ) -> StrategyStatus:
        try:
            result = registry.stop(payload.strategy_id)
        except ExecBotError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return StrategyStatus(
            strategy_id=result.strategy_id,
            running=result.running,
            symbol=getattr(result, "symbol", None),
            enabled=getattr(result, "enabled", None),
            last_error=getattr(result, "last_error", None),
        )

    @router.post("/kill", response_model=KillResponse)
    def kill(
        runtime: ControlPlaneRuntime = Depends(get_runtime),
        registry: StrategyRegistryLike = Depends(get_strategy_registry),
        order_manager: OrderManagerLike = Depends(get_order_manager),
    ) -> KillResponse:
        runtime.kill_switch = True
        runtime.live_armed = False
        _set_order_manager_environment(order_manager, EnvironmentMode.PAPER)
        _set_strategy_environment(registry, EnvironmentMode.PAPER)
        registry.stop_all()
        return KillResponse(kill_switch=runtime.kill_switch, live_armed=runtime.live_armed)

    @router.post("/arm_live", response_model=ArmLiveResponse)
    def arm_live(
        settings: Settings = Depends(get_settings),
        runtime: ControlPlaneRuntime = Depends(get_runtime),
        order_manager: OrderManagerLike = Depends(get_order_manager),
        registry: StrategyRegistryLike = Depends(get_strategy_registry),
    ) -> ArmLiveResponse:
        if runtime.kill_switch:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="kill switch active")

        if not settings.live_trading or settings.ack_live_trading != "I_UNDERSTAND":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="live arming denied: LIVE_TRADING=true and ACK_LIVE_TRADING=I_UNDERSTAND required",
            )

        runtime.live_armed = True
        _set_order_manager_environment(order_manager, EnvironmentMode.LIVE)
        _set_strategy_environment(registry, EnvironmentMode.LIVE)
        return ArmLiveResponse(armed=True, mode=_current_mode(settings=settings, runtime=runtime))

    return router


def _workspace_scope(
    *,
    settings: Settings,
    runtime: ControlPlaneRuntime,
) -> tuple[str, EnvironmentMode, str]:
    user_key = settings.ibkr_account.strip() if settings.ibkr_account.strip() else "local"
    mode = _current_mode(settings=settings, runtime=runtime)
    workspace_key = build_workspace_key(user_key=user_key, environment=mode.value)
    return user_key, mode, workspace_key


def _save_workspace_to_repo(
    *,
    workspace_repo: WorkspaceSettingsRepositoryLike,
    user_key: str,
    mode: EnvironmentMode,
    rows: list[WorkspaceRowInput],
    columns: list[str],
) -> None:
    payload = {
        "rows": [item.model_dump(mode="json") for item in rows],
        "columns": columns,
    }
    workspace_repo.upsert(
        user_key=user_key,
        environment=mode,
        settings_json=json.dumps(payload, separators=(",", ":"), sort_keys=True),
    )


def _load_workspace_from_repo(
    *,
    market_data_service: MarketDataService,
    workspace_repo: WorkspaceSettingsRepositoryLike,
    user_key: str,
    mode: EnvironmentMode,
    workspace_key: str,
) -> None:
    market_data_service.ensure_workspace(workspace_key)
    existing = workspace_repo.get(user_key=user_key, environment=mode)

    if existing is None:
        market_data_service.set_columns(workspace_key, default_columns())
        market_data_service.set_rows(workspace_key, [])
        return

    try:
        payload = json.loads(existing.settings_json)
    except json.JSONDecodeError:
        market_data_service.set_columns(workspace_key, default_columns())
        market_data_service.set_rows(workspace_key, [])
        return

    column_values = payload.get("columns", default_columns())
    if not isinstance(column_values, list):
        column_values = default_columns()

    raw_rows = payload.get("rows", [])
    rows: list[WorkspaceRow] = []
    if isinstance(raw_rows, list):
        for item in raw_rows:
            if not isinstance(item, dict):
                continue
            try:
                parsed = WorkspaceRowInput.model_validate(item)
            except Exception:
                continue
            rows.append(
                WorkspaceRow(
                    row_id=parsed.row_id,
                    symbol=parsed.symbol,
                    sec_type=parsed.sec_type,
                    exchange=parsed.exchange,
                    currency=parsed.currency,
                    con_id=parsed.con_id,
                    primary_exchange=parsed.primary_exchange,
                )
            )

    market_data_service.set_columns(workspace_key, [str(value) for value in column_values])
    market_data_service.set_rows(workspace_key, rows)


def _current_mode(settings: Settings, runtime: ControlPlaneRuntime) -> EnvironmentMode:
    if settings.live_trading and settings.ack_live_trading == "I_UNDERSTAND" and runtime.live_armed:
        return EnvironmentMode.LIVE
    return EnvironmentMode.PAPER


def _to_pinned_response(item: PinnedContract) -> PinnedContractResponse:
    return PinnedContractResponse(
        symbol=item.symbol,
        environment=item.environment,
        con_id=item.con_id,
        exchange=item.exchange,
        primary_exchange=item.primary_exchange,
        sec_type=item.sec_type,
        currency=item.currency,
        is_active=item.is_active,
    )


def _set_order_manager_environment(order_manager: OrderManagerLike, environment: EnvironmentMode) -> None:
    setter = getattr(order_manager, "set_environment", None)
    if callable(setter):
        setter(environment)


def _set_strategy_environment(registry: StrategyRegistryLike, environment: EnvironmentMode) -> None:
    setter = getattr(registry, "set_environment", None)
    if callable(setter):
        setter(environment)


def _to_orb_status_item(item: OrbRowState) -> OrbStateItem:
    return OrbStateItem(row_id=item.row_id, state=item.state, trade_id=item.trade_id, detail=item.detail)


def _readiness_guidance(
    *,
    settings: Settings,
    runtime: ControlPlaneRuntime,
    connected: bool,
) -> list[str]:
    lines: list[str] = []
    if not connected:
        lines.append("Broker is disconnected. Start IB Gateway/TWS and verify host/port/client ID.")
    if settings.dry_run:
        lines.append("DRY_RUN is true. Set DRY_RUN=false to transmit orders.")
    if runtime.kill_switch:
        lines.append("Kill switch is active. POST /kill cannot be undone except restart; clear by restart and avoid kill.")
    if not settings.live_trading:
        lines.append("LIVE_TRADING is false. Live order mode cannot arm until enabled in config.")
    if settings.ack_live_trading != "I_UNDERSTAND":
        lines.append("ACK_LIVE_TRADING must equal I_UNDERSTAND for live arming.")
    if settings.live_trading and settings.ack_live_trading == "I_UNDERSTAND" and not runtime.live_armed:
        lines.append("Live gates configured. Call POST /arm_live to arm this session.")
    if runtime.live_armed and connected:
        lines.append("Live is armed and broker connected. Validate account and send minimal-size test order first.")
    if not lines:
        lines.append("System ready.")
    return lines
