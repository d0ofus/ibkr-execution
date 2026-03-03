"""API request and response schema models."""

from __future__ import annotations

from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field, model_validator

from app.domain.enums import EnvironmentMode, Side


class HealthResponse(BaseModel):
    """Health endpoint response model."""

    status: str


class StatusResponse(BaseModel):
    """Runtime status payload."""

    mode: EnvironmentMode
    live_enabled: bool
    live_armed: bool
    dry_run: bool
    kill_switch: bool
    account: str


class ContractResolveRequest(BaseModel):
    """Request for contract candidate resolution."""

    symbol: str


class ContractCandidateResponse(BaseModel):
    """Contract candidate response payload."""

    symbol: str
    con_id: int
    exchange: str
    primary_exchange: str | None
    sec_type: str
    currency: str


class ContractResolveResponse(BaseModel):
    """Response for contract resolution endpoint."""

    candidates: list[ContractCandidateResponse]


class ContractPinRequest(BaseModel):
    """Request payload for pinning a contract."""

    symbol: str
    con_id: int
    environment: EnvironmentMode = EnvironmentMode.PAPER


class PinnedContractResponse(BaseModel):
    """Pinned contract response payload."""

    symbol: str
    environment: EnvironmentMode
    con_id: int
    exchange: str
    primary_exchange: str | None
    sec_type: str
    currency: str
    is_active: bool


class OrderIntentRequest(BaseModel):
    """Request payload for creating an order intent."""

    symbol: str
    side: Side
    entry_price: Decimal = Field(gt=0)
    stop_price: Decimal = Field(gt=0)
    risk_dollars: Decimal = Field(gt=0)
    intent_id: str | None = None
    strategy_id: str | None = None


class OrderIntentResponse(BaseModel):
    """Response payload for order intent submission."""

    accepted: bool
    trade_id: str | None = None
    reason: str | None = None


class CancelOrderResponse(BaseModel):
    """Response payload for order cancellation command."""

    cancelled: bool
    trade_id: str


class OpenOrdersResponse(BaseModel):
    """Response payload listing open trade IDs."""

    trade_ids: list[str]


class StrategyStatus(BaseModel):
    """Single strategy runtime status."""

    strategy_id: str
    running: bool
    symbol: str | None = None
    enabled: bool | None = None
    last_error: str | None = None


class StrategiesResponse(BaseModel):
    """List of strategy runtime states."""

    strategies: list[StrategyStatus]


class StrategyCommandRequest(BaseModel):
    """Request payload for starting/stopping a strategy."""

    strategy_id: str


class StrategyUpsertRequest(BaseModel):
    """Request payload for creating/updating strategy source definitions."""

    source_format: Literal["yaml", "json"]
    source_payload: str


class StrategyDefinitionResponse(BaseModel):
    """Strategy source definition read model for UI/editor usage."""

    strategy_id: str
    source_format: Literal["yaml", "json"]
    source_payload: str
    symbol: str
    enabled: bool


class KillResponse(BaseModel):
    """Response payload for kill switch activation."""

    kill_switch: bool
    live_armed: bool


class ArmLiveResponse(BaseModel):
    """Response payload for live arming command."""

    armed: bool
    mode: EnvironmentMode


class WorkspaceRowInput(BaseModel):
    """Workspace row instrument definition."""

    row_id: str
    symbol: str
    sec_type: str = "STK"
    exchange: str = "SMART"
    currency: str = "USD"
    con_id: int | None = None
    primary_exchange: str | None = None


class WorkspaceStateResponse(BaseModel):
    """Workspace grid state and feed health payload."""

    workspace_key: str
    rows: list[WorkspaceRowInput]
    columns: list[str]
    connected: bool
    feed_healthy: bool


class WorkspaceRowsRequest(BaseModel):
    """Request payload for replacing workspace rows."""

    rows: list[WorkspaceRowInput]


class WorkspaceColumnsRequest(BaseModel):
    """Request payload for replacing workspace columns."""

    columns: list[str]


class OrbParameters(BaseModel):
    """ORB strategy runtime parameters."""

    qty: int = Field(gt=0)
    x1: Decimal = Field(gt=0)
    x2: Decimal = Field(gt=0)

    @model_validator(mode="after")
    def validate_levels(self) -> "OrbParameters":
        if self.x2 <= self.x1:
            raise ValueError("x2 must be greater than x1")
        return self


class OrbStartRequest(BaseModel):
    """Request to start ORB strategy on one or more rows."""

    row_ids: list[str] = Field(min_length=1)
    params: OrbParameters


class OrbStopRequest(BaseModel):
    """Request to stop ORB strategy on rows or all rows in workspace."""

    row_ids: list[str] = Field(default_factory=list)
    stop_all: bool = False


class OrbStateItem(BaseModel):
    """Single row ORB runtime state snapshot."""

    row_id: str
    state: str
    trade_id: str | None = None
    detail: str | None = None


class OrbStatusResponse(BaseModel):
    """ORB strategy status list payload."""

    items: list[OrbStateItem]


class RuntimeProfileResponse(BaseModel):
    """Runtime broker profile settings for paper/live."""

    host: str
    port: int
    client_id: int
    account: str


class RuntimeReadinessResponse(BaseModel):
    """Live readiness and connectivity checks."""

    selected_profile: EnvironmentMode
    broker_connected: bool
    kill_switch: bool
    dry_run: bool
    live_trading_enabled: bool
    ack_configured: bool
    live_armed: bool
    current_mode: EnvironmentMode
    paper_profile: RuntimeProfileResponse
    live_profile: RuntimeProfileResponse
    guidance: list[str]


class RuntimeProfileSwitchRequest(BaseModel):
    """Request payload for switching broker connection profile."""

    profile: EnvironmentMode


class RuntimeProfileConfigRequest(BaseModel):
    """Request payload for updating one broker runtime profile."""

    profile: EnvironmentMode
    host: str
    port: int = Field(gt=0)
    client_id: int = Field(ge=0)
    account: str = ""
