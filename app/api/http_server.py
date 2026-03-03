"""FastAPI application factory and lifecycle wiring."""

from __future__ import annotations

from collections.abc import AsyncIterator
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI

from app.api.routes import BrokerConnectionProfile, ControlPlaneRuntime, StrategyRegistry, build_router
from app.config import Settings
from app.domain.enums import EnvironmentMode
from app.logging_setup import PeriodicMemoryLogger, create_periodic_memory_logger


@dataclass
class ApiDependencies:
    """Dependency container attached to FastAPI app state."""

    contract_service: Any | None = None
    pinned_contract_reader: Any | None = None
    order_manager: Any | None = None
    runtime: ControlPlaneRuntime | None = None
    startup_hooks: list[Callable[[], None]] | None = None
    shutdown_hooks: list[Callable[[], None]] | None = None
    app_state: dict[str, Any] | None = None
    memory_logger: PeriodicMemoryLogger | None = None


def create_http_app(settings: Settings, dependencies: ApiDependencies | None = None) -> FastAPI:
    """Create FastAPI app configured with shared services."""
    deps = dependencies or ApiDependencies()
    memory_logger = deps.memory_logger or create_periodic_memory_logger(
        interval_seconds=settings.memory_log_interval_seconds
    )

    startup_hooks = deps.startup_hooks or []
    shutdown_hooks = deps.shutdown_hooks or []

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        for hook in startup_hooks:
            hook()
        memory_logger.start()
        try:
            yield
        finally:
            memory_logger.stop()
            for hook in shutdown_hooks:
                hook()

    app = FastAPI(title=settings.app_name, lifespan=lifespan)

    app.state.settings = settings
    app.state.runtime = deps.runtime or ControlPlaneRuntime()
    app.state.broker_profiles = (deps.app_state or {}).get(
        "broker_profiles",
        {
            EnvironmentMode.PAPER: BrokerConnectionProfile(
                host=settings.ibkr_paper_host or settings.ibkr_host,
                port=settings.ibkr_paper_port or settings.ibkr_port,
                client_id=settings.ibkr_paper_client_id or settings.ibkr_client_id,
                account=settings.ibkr_paper_account or settings.ibkr_account,
            ),
            EnvironmentMode.LIVE: BrokerConnectionProfile(
                host=settings.ibkr_live_host,
                port=settings.ibkr_live_port,
                client_id=settings.ibkr_live_client_id,
                account=settings.ibkr_live_account or settings.ibkr_account,
            ),
        },
    )
    app.state.strategy_registry = StrategyRegistry()
    app.state.contract_service = deps.contract_service
    app.state.pinned_contract_reader = deps.pinned_contract_reader
    app.state.order_manager = deps.order_manager
    app.state.memory_logger = memory_logger
    for key, value in (deps.app_state or {}).items():
        setattr(app.state, key, value)

    app.include_router(build_router())
    return app
