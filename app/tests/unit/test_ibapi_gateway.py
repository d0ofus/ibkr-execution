"""Unit tests for IbApiGateway quote fallback behavior."""

from __future__ import annotations

from collections import defaultdict

from ibapi.client import EClient

from app.broker.ibapi_gateway import IbApiGateway
from app.domain.models import ContractRef


def test_quote_error_falls_back_to_delayed_for_stk(monkeypatch) -> None:
    calls: dict[str, list[tuple[object, ...]]] = defaultdict(list)

    monkeypatch.setattr(EClient, "cancelMktData", lambda self, req_id: calls["cancelMktData"].append((req_id,)))
    monkeypatch.setattr(
        EClient,
        "reqMarketDataType",
        lambda self, market_data_type: calls["reqMarketDataType"].append((market_data_type,)),
    )
    monkeypatch.setattr(
        EClient,
        "reqMktData",
        lambda self, req_id, contract, generic, snapshot, regulatory, options: calls["reqMktData"].append(
            (req_id, contract.symbol, generic, snapshot, regulatory, tuple(options))
        ),
    )

    gateway = IbApiGateway(host="127.0.0.1", port=4002, client_id=1)
    req_id = 7
    contract = ContractRef(symbol="CBA", con_id=0, exchange="SMART", sec_type="STK", currency="AUD")
    gateway._quote_request_to_contract[req_id] = contract  # noqa: SLF001
    gateway._quote_delayed_by_request_id[req_id] = False  # noqa: SLF001

    captured_delayed: list[bool] = []
    gateway.register_quote_handler(
        lambda quote: captured_delayed.append(quote.delayed)
        if quote.symbol == "CBA" and quote.delayed
        else None
    )

    gateway.error(req_id, 10285, "fractional size rules")

    assert calls["cancelMktData"] == [(req_id,)]
    assert calls["reqMarketDataType"] == [(3,)]
    assert calls["reqMktData"][0][0] == req_id
    assert calls["reqMktData"][0][1] == "CBA"
    assert gateway._quote_delayed_by_request_id[req_id] is True  # noqa: SLF001
    assert captured_delayed == [True]


def test_quote_error_falls_back_to_delayed_for_crypto_realtime_bars(monkeypatch) -> None:
    calls: dict[str, list[tuple[object, ...]]] = defaultdict(list)

    monkeypatch.setattr(EClient, "cancelRealTimeBars", lambda self, req_id: calls["cancelRealTimeBars"].append((req_id,)))
    monkeypatch.setattr(
        EClient,
        "reqMarketDataType",
        lambda self, market_data_type: calls["reqMarketDataType"].append((market_data_type,)),
    )
    monkeypatch.setattr(
        EClient,
        "reqRealTimeBars",
        lambda self, req_id, contract, bar_size, what_to_show, use_rth, options: calls["reqRealTimeBars"].append(
            (req_id, contract.symbol, bar_size, what_to_show, use_rth, tuple(options))
        ),
    )

    gateway = IbApiGateway(host="127.0.0.1", port=4002, client_id=1)
    req_id = 8
    contract = ContractRef(symbol="BTC", con_id=0, exchange="PAXOS", sec_type="CRYPTO", currency="USD")
    gateway._quote_request_to_contract[req_id] = contract  # noqa: SLF001
    gateway._quote_delayed_by_request_id[req_id] = False  # noqa: SLF001
    gateway._quote_realtime_bar_request_to_contract[req_id] = contract  # noqa: SLF001

    gateway.error(req_id, 10167, "delayed market data")

    assert calls["cancelRealTimeBars"] == [(req_id,)]
    assert calls["reqMarketDataType"] == [(3,)]
    assert calls["reqRealTimeBars"][0][0] == req_id
    assert calls["reqRealTimeBars"][0][1] == "BTC"
    assert calls["reqRealTimeBars"][0][2] == 5
    assert calls["reqRealTimeBars"][0][4] is False
    assert gateway._quote_delayed_by_request_id[req_id] is True  # noqa: SLF001


def test_quote_fallback_retries_only_once(monkeypatch) -> None:
    calls: dict[str, list[tuple[object, ...]]] = defaultdict(list)
    monkeypatch.setattr(
        EClient,
        "cancelMktData",
        lambda self, req_id: calls["cancelMktData"].append((req_id,)),
    )
    monkeypatch.setattr(
        EClient,
        "reqMarketDataType",
        lambda self, market_data_type: calls["reqMarketDataType"].append((market_data_type,)),
    )
    monkeypatch.setattr(
        EClient,
        "reqMktData",
        lambda self, req_id, contract, generic, snapshot, regulatory, options: calls["reqMktData"].append(
            (req_id, contract.symbol, generic, snapshot, regulatory, tuple(options))
        ),
    )

    gateway = IbApiGateway(host="127.0.0.1", port=4002, client_id=1)
    req_id = 9
    gateway._quote_request_to_contract[req_id] = ContractRef(  # noqa: SLF001
        symbol="AAPL",
        con_id=0,
        exchange="SMART",
        sec_type="STK",
        currency="USD",
    )
    gateway._quote_delayed_by_request_id[req_id] = False  # noqa: SLF001

    gateway.error(req_id, 354, "market data not subscribed")
    gateway.error(req_id, 354, "market data not subscribed")

    assert calls["cancelMktData"] == [(req_id,)]
    assert calls["reqMarketDataType"] == [(3,)]
    assert len(calls["reqMktData"]) == 1
