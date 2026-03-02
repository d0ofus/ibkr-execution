"""Route registration for the control plane HTTP API."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Protocol, cast
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import HTMLResponse

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
    OrderIntentRequest,
    OrderIntentResponse,
    PinnedContractResponse,
    StatusResponse,
    StrategiesResponse,
    StrategyCommandRequest,
    StrategyDefinitionResponse,
    StrategyStatus,
    StrategyUpsertRequest,
)
from app.config import Settings
from app.domain.enums import EnvironmentMode
from app.domain.errors import ExecBotError
from app.domain.models import ContractRef, OrderIntent, PinnedContract
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


def get_strategy_registry(request: Request) -> StrategyRegistryLike:
    """Resolve strategy registry dependency from app state."""
    registry_obj = getattr(request.app.state, "strategy_registry", None)
    if registry_obj is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="strategy registry unavailable",
        )
    return cast(StrategyRegistryLike, registry_obj)


def build_router() -> APIRouter:
    """Construct API router with control-plane endpoints."""
    router = APIRouter()
    ui_path = Path(__file__).with_name("web_ui.html")

    @router.get("/", response_class=HTMLResponse, include_in_schema=False)
    def web_ui_root() -> HTMLResponse:
        return HTMLResponse(content=ui_path.read_text(encoding="utf-8"))

    @router.get("/ui", response_class=HTMLResponse, include_in_schema=False)
    def web_ui_page() -> HTMLResponse:
        return HTMLResponse(content=ui_path.read_text(encoding="utf-8"))

    @router.get("/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        return HealthResponse(status="ok")

    @router.get("/status", response_model=StatusResponse)
    def status_endpoint(
        settings: Settings = Depends(get_settings),
        runtime: ControlPlaneRuntime = Depends(get_runtime),
    ) -> StatusResponse:
        mode = _current_mode(settings=settings, runtime=runtime)
        return StatusResponse(
            mode=mode,
            live_enabled=settings.live_trading,
            live_armed=runtime.live_armed,
            dry_run=settings.dry_run,
            kill_switch=runtime.kill_switch,
            account=settings.ibkr_account,
        )

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
