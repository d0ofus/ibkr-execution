"""Production runtime composition for broker, persistence, and execution services."""

from __future__ import annotations

from decimal import Decimal
import logging
from pathlib import Path
from typing import Any

from app.api.http_server import ApiDependencies
from app.api.routes import BrokerConnectionProfile, ControlPlaneRuntime
from app.broker.contracts import ContractService
from app.broker.ibapi_gateway import IbApiGateway
from app.broker.ibkr_client import IbkrClient
from app.broker.ibkr_events import BrokerEvent, BrokerEventType
from app.broker.rate_limiter import PacingRateLimiter
from app.broker.reconnect import ReconnectPolicy, ReconnectSupervisor
from app.config import Settings
from app.data.market_data_service import MarketDataService
from app.domain.enums import EnvironmentMode
from app.execution.order_manager import OrderManager
from app.execution.orb_runner import OrbRunner
from app.execution.strategy_runtime import StrategyExecutionRuntime
from app.persistence.db import create_all_tables, create_engine_and_session
from app.persistence.repositories import (
    SqlAlchemyAuditLogRepository,
    SqlAlchemyPinnedContractRepository,
    SqlAlchemyTradeRepository,
    SqlAlchemyWorkspaceSettingsRepository,
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
    workspace_settings_repo = SqlAlchemyWorkspaceSettingsRepository(session_factory)

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
    strategy_runtime = StrategyExecutionRuntime(
        order_manager=order_manager,
        broker_client=broker_client,
        contract_service=contract_service,
        environment=EnvironmentMode.PAPER,
    )
    market_data_service = MarketDataService(
        broker_client=broker_client,
        stale_after_seconds=settings.watchdog_stale_after_seconds,
    )
    orb_runner = OrbRunner(
        order_manager=order_manager,
        market_data_service=market_data_service,
        broker_client=broker_client,
        runtime=runtime,
    )

    transport.register_realtime_bar_handler(strategy_runtime.on_realtime_bar)
    transport.register_quote_handler(market_data_service.on_quote_update)
    transport.register_order_status_handler(
        lambda order_id, _status, filled, _remaining: strategy_runtime.on_order_status_update(
            order_id=order_id,
            filled_quantity=filled,
        )
    )
    market_data_service.register_snapshot_handler(orb_runner.on_market_snapshot)

    def _handle_broker_event(event: BrokerEvent) -> None:
        if event.event_type == BrokerEventType.CONNECTION_OPENED:
            market_data_service.set_connectivity(connected=True)
        elif event.event_type in {
            BrokerEventType.CONNECTION_CLOSED,
            BrokerEventType.CONNECTIVITY_LOST,
            BrokerEventType.SOCKET_PORT_RESET,
            BrokerEventType.HEARTBEAT_TIMEOUT,
            BrokerEventType.MARKET_DATA_STALE,
        }:
            market_data_service.set_connectivity(connected=False, reason=event.event_type.value)

    broker_client.register_event_handler(_handle_broker_event)

    default_strategy_path = Path("app/replay/datasets/orb_strategy.yaml")
    if default_strategy_path.exists():
        try:
            strategy_runtime.upsert_definition(
                source_payload=default_strategy_path.read_text(encoding="utf-8"),
                source_format="yaml",
            )
        except Exception:
            logger.exception("failed_to_load_default_strategy")

    def startup_connect_broker() -> None:
        if settings.dry_run:
            logger.warning("dry_run enabled; skipping broker connection startup")
            return
        try:
            paper_profile = broker_profiles[EnvironmentMode.PAPER]
            broker_client.switch_connection_profile(
                host=paper_profile.host,
                port=paper_profile.port,
                client_id=paper_profile.client_id,
            )
            logger.info(
                "connected_to_ibkr host=%s port=%s client_id=%s",
                paper_profile.host,
                paper_profile.port,
                paper_profile.client_id,
            )
        except Exception as exc:
            logger.exception("ibkr_connect_failed: %s", exc)

    def shutdown_disconnect_broker() -> None:
        try:
            broker_client.disconnect()
        except Exception:
            logger.exception("ibkr_disconnect_failed")

    broker_profiles: dict[EnvironmentMode, BrokerConnectionProfile] = {
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
    }
    runtime.selected_profile = EnvironmentMode.PAPER

    app_state: dict[str, Any] = {
        "db_engine": engine,
        "session_factory": session_factory,
        "broker_client": broker_client,
        "broker_profiles": broker_profiles,
        "trade_repository": trade_repo,
        "pinned_contract_repository": pinned_repo,
        "workspace_settings_repository": workspace_settings_repo,
        "audit_log_repository": audit_repo,
        "strategy_registry": strategy_runtime,
        "strategy_runtime": strategy_runtime,
        "market_data_service": market_data_service,
        "orb_runner": orb_runner,
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
