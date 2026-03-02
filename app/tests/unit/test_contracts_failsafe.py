"""Unit tests for contract failsafe qualification and pin-enforcement logic."""

from pathlib import Path
from uuid import uuid4

import pytest

from app.broker.contracts import ContractCandidate, ContractQualifier, ContractService
from app.domain.enums import EnvironmentMode
from app.domain.errors import ContractAmbiguityError, ContractError, ContractNotFoundError, PinnedContractRequiredError
from app.domain.models import PinnedContract
from app.persistence.db import create_all_tables, create_engine_and_session
from app.persistence.repositories import SqlAlchemyPinnedContractRepository


class StubQualifier(ContractQualifier):
    """Stub qualifier used for deterministic unit tests."""

    def __init__(self, responses: dict[str, list[ContractCandidate]]) -> None:
        self._responses = responses

    def qualify(self, symbol: str) -> list[ContractCandidate]:
        return list(self._responses.get(symbol, []))


def _new_db_url(test_name: str) -> str:
    base_dir = Path(".tmp_tests")
    base_dir.mkdir(parents=True, exist_ok=True)
    path = base_dir / f"{test_name}_{uuid4().hex}.db"
    return f"sqlite+pysqlite:///{path.as_posix()}"


def _build_service(
    qualifier: ContractQualifier,
    *,
    allowed_routing_exchanges: set[str] | None = None,
) -> tuple[ContractService, SqlAlchemyPinnedContractRepository]:
    engine, session_factory = create_engine_and_session(_new_db_url("contracts"))
    create_all_tables(engine)
    pin_store = SqlAlchemyPinnedContractRepository(session_factory)
    service = ContractService(
        qualifier=qualifier,
        pinned_contract_store=pin_store,
        allowed_routing_exchanges=allowed_routing_exchanges,
    )
    return service, pin_store


def test_resolve_candidates_filters_to_stk_usd_and_allowed_routing() -> None:
    qualifier = StubQualifier(
        {
            "AAPL": [
                ContractCandidate("AAPL", 101, "SMART", "NASDAQ", "STK", "USD"),
                ContractCandidate("AAPL", 102, "SMART", "NASDAQ", "OPT", "USD"),
                ContractCandidate("AAPL", 103, "SMART", "NASDAQ", "STK", "CAD"),
                ContractCandidate("AAPL", 104, "NYSE", "NYSE", "STK", "USD"),
            ]
        }
    )
    service, _ = _build_service(qualifier)

    resolved = service.resolve_candidates("aapl")

    assert len(resolved) == 1
    assert resolved[0].symbol == "AAPL"
    assert resolved[0].con_id == 101
    assert resolved[0].exchange == "SMART"


def test_resolve_candidates_raises_when_no_valid_contracts() -> None:
    qualifier = StubQualifier({"TSLA": [ContractCandidate("TSLA", 210, "NYSE", "NYSE", "STK", "USD")]})
    service, _ = _build_service(qualifier)

    with pytest.raises(ContractNotFoundError):
        _ = service.resolve_candidates("TSLA")


def test_ambiguity_requires_explicit_conid_selection() -> None:
    qualifier = StubQualifier(
        {
            "MSFT": [
                ContractCandidate("MSFT", 310, "SMART", "NASDAQ", "STK", "USD"),
                ContractCandidate("MSFT", 311, "SMART", "NASDAQ", "STK", "USD"),
            ]
        }
    )
    service, _ = _build_service(qualifier)

    resolved = service.resolve_candidates("MSFT")
    assert [item.con_id for item in resolved] == [310, 311]

    with pytest.raises(ContractAmbiguityError):
        _ = service.resolve_for_execution("MSFT")

    selected = service.resolve_for_execution("MSFT", selected_con_id=311)
    assert selected.con_id == 311
    assert selected.exchange == "SMART"


def test_resolve_for_execution_rejects_unknown_selected_conid() -> None:
    qualifier = StubQualifier(
        {"NVDA": [ContractCandidate("NVDA", 410, "SMART", "NASDAQ", "STK", "USD")]}
    )
    service, _ = _build_service(qualifier)

    with pytest.raises(ContractNotFoundError):
        _ = service.resolve_for_execution("NVDA", selected_con_id=999)


def test_pin_and_require_pinned_contract_returns_conid_qualified_reference() -> None:
    qualifier = StubQualifier(
        {"AMZN": [ContractCandidate("AMZN", 510, "SMART", "NASDAQ", "STK", "USD")]}
    )
    service, _ = _build_service(qualifier)

    pinned = service.pin_contract("AMZN", EnvironmentMode.PAPER, selected_con_id=510)
    assert pinned.con_id == 510
    assert pinned.symbol == "AMZN"

    contract = service.require_pinned_contract("AMZN", EnvironmentMode.PAPER)
    assert contract.con_id == 510
    assert contract.symbol == "AMZN"
    assert contract.exchange == "SMART"


def test_require_pinned_contract_rejects_unpinned_symbol() -> None:
    service, _ = _build_service(StubQualifier({}))

    with pytest.raises(PinnedContractRequiredError):
        _ = service.require_pinned_contract("QQQ", EnvironmentMode.PAPER)


def test_require_pinned_contract_rejects_policy_violating_pin() -> None:
    qualifier = StubQualifier({})
    service, pin_store = _build_service(qualifier)

    bad_pin = PinnedContract(
        symbol="IBM",
        environment=EnvironmentMode.PAPER,
        con_id=777,
        exchange="NYSE",
        primary_exchange="NYSE",
        sec_type="STK",
        currency="USD",
    )
    pin_store.pin(bad_pin)

    with pytest.raises(ContractError):
        _ = service.require_pinned_contract("IBM", EnvironmentMode.PAPER)
