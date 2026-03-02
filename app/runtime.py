"""Production runtime composition for broker, persistence, and execution services."""

from __future__ import annotations

from decimal import Decimal
import logging
from typing import Any

from app.api.http_server import ApiDependencies
from app.api.routes import ControlPlaneRuntime
from app.broker.contracts import ContractService
from app.broker.ibapi_gateway import IbApiGateway
from app.broker.ibkr_client import IbkrClient
from app.broker.rate_limiter import PacingRateLimiter
from app.broker.reconnect import ReconnectPolicy, ReconnectSupervisor
from app.config import Settings
from app.domain.enums import EnvironmentMode
from app.execution.order_manager import OrderManager
from app.persistence.db import create_all_tables, create_engine_and_session
from app.persistence.repositories import (
    SqlAlchemyAuditLogRepository,
    SqlAlchemyPinnedContractRepository,
    SqlAlchemyTradeRepository,
)
from app.risk.limits import RiskLimits


def build_api_dependencies(settings: Settings, *, runtime: ControlPlaneRuntime) -> ApiDependencies:
    """Build concrete runtime dependencies for production API execution."""
    logger = logging.getLogger("ibkr_exec.runtime")

    engine, session_factory = create_engine_and_session(settings.database_url)
    create_all_tables(engine)

    pinned_repo = SqlAlchemyPinnedContractRepository(session_factory)
    trade_repo = SqlAlchemyTradeRepository(session_factory)
    audit_repo = SqlAlchemyAuditLogRepository(session_factory)

    transport = IbApiGateway(
        host=settings.ibkr_host,
        port=settings.ibkr_port,
        client_id=settings.ibkr_client_id,
    )
    reconnect_supervisor = ReconnectSupervisor(
        policy=ReconnectPolicy(),
    )
    rate_limiter = PacingRateLimiter(
        max_requests=settings.ibkr_max_requests_per_second,
        window_seconds=settings.ibkr_pacing_window_seconds,
    )
    broker_client = IbkrClient(
        transport=transport,
        reconnect_supervisor=reconnect_supervisor,
        rate_limiter=rate_limiter,
    )

    contract_service = ContractService(
        qualifier=transport,
        pinned_contract_store=pinned_repo,
        allowed_routing_exchanges={"SMART"},
    )
    risk_limits = RiskLimits(
        max_positions=settings.max_positions,
        max_notional=Decimal(str(settings.max_notional)),
        max_daily_loss=Decimal(str(settings.max_daily_loss)),
        max_orders_per_minute=settings.max_orders_per_minute,
        max_trades_per_symbol_per_day=settings.max_trades_per_symbol_per_day,
    )
    order_manager = OrderManager(
        broker_client=broker_client,
        contract_service=contract_service,
        trade_repository=trade_repo,
        risk_limits=risk_limits,
        min_stop_distance=Decimal(str(settings.min_stop_distance)),
        environment=EnvironmentMode.PAPER,
    )

    def startup_connect_broker() -> None:
        if settings.dry_run:
            logger.warning("dry_run enabled; skipping broker connection startup")
            return
        try:
            broker_client.connect()
            logger.info(
                "connected_to_ibkr host=%s port=%s client_id=%s",
                settings.ibkr_host,
                settings.ibkr_port,
                settings.ibkr_client_id,
            )
        except Exception as exc:
            logger.exception("ibkr_connect_failed: %s", exc)

    def shutdown_disconnect_broker() -> None:
        try:
            broker_client.disconnect()
        except Exception:
            logger.exception("ibkr_disconnect_failed")

    app_state: dict[str, Any] = {
        "db_engine": engine,
        "session_factory": session_factory,
        "broker_client": broker_client,
        "trade_repository": trade_repo,
        "pinned_contract_repository": pinned_repo,
        "audit_log_repository": audit_repo,
    }

    return ApiDependencies(
        contract_service=contract_service,
        pinned_contract_reader=pinned_repo,
        order_manager=order_manager,
        runtime=runtime,
        startup_hooks=[startup_connect_broker],
        shutdown_hooks=[shutdown_disconnect_broker],
        app_state=app_state,
    )
