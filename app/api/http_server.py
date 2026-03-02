"""FastAPI application factory and lifecycle wiring."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI

from app.api.routes import ControlPlaneRuntime, StrategyRegistry, build_router
from app.config import Settings
from app.logging_setup import PeriodicMemoryLogger, create_periodic_memory_logger


@dataclass
class ApiDependencies:
    """Dependency container attached to FastAPI app state."""

    contract_service: Any | None = None
    pinned_contract_reader: Any | None = None
    order_manager: Any | None = None
    memory_logger: PeriodicMemoryLogger | None = None


def create_http_app(settings: Settings, dependencies: ApiDependencies | None = None) -> FastAPI:
    """Create FastAPI app configured with shared services."""
    deps = dependencies or ApiDependencies()
    memory_logger = deps.memory_logger or create_periodic_memory_logger(
        interval_seconds=settings.memory_log_interval_seconds
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        memory_logger.start()
        try:
            yield
        finally:
            memory_logger.stop()

    app = FastAPI(title=settings.app_name, lifespan=lifespan)

    app.state.settings = settings
    app.state.runtime = ControlPlaneRuntime()
    app.state.strategy_registry = StrategyRegistry()
    app.state.contract_service = deps.contract_service
    app.state.pinned_contract_reader = deps.pinned_contract_reader
    app.state.order_manager = deps.order_manager
    app.state.memory_logger = memory_logger

    app.include_router(build_router())
    return app
