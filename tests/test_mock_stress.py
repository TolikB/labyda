from __future__ import annotations

import asyncio
import time
from collections import Counter
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from arbitrage_engine.config import AppConfig, load_config
from arbitrage_engine.connectors.base import BinaryMarketClient, OrderBookUnavailableException
from arbitrage_engine.engine import ArbitrageEngine
from arbitrage_engine.execution import ExecutionRouter
from arbitrage_engine.models import (
    ArbitrageSignal,
    BinarySide,
    ExecutionMode,
    ExecutionReport,
    MappingStatus,
    MarketConstraints,
    MarketSpec,
    OpenPosition,
    OrderBook,
    OrderBookLevel,
)
from arbitrage_engine.positions import PositionLedger
from arbitrage_engine.telegram import TelegramNotifier


class _StressClient(BinaryMarketClient):
    def __init__(self, venue_name: str) -> None:
        self.venue_name = venue_name
        self.cycle = 0
        self.in_flight = 0
        self.max_in_flight = 0
        self.buy_calls = 0
        self.sell_calls = 0
        self.recovery_events = 0

    async def watch_order_book(self, token_id: str) -> OrderBook:
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            await asyncio.sleep(0)
            market_number = int("".join(character for character in token_id if character.isdigit()) or "0")
            scenario = market_number % 10
            if scenario in {0, 1} and self.cycle % 3 == 0:
                self.recovery_events += 1
                raise OrderBookUnavailableException(f"mock reconnect gap for {token_id}")
            timestamp = time.time() - 10 if scenario == 2 else time.time()
            ask = 0.40 if scenario == 9 else 0.55
            size = 1 if scenario in {3, 4} else 100
            return OrderBook(
                bids=[OrderBookLevel(max(0.01, ask - 0.05), size)],
                asks=[OrderBookLevel(ask, size)],
                timestamp=timestamp,
            )
        finally:
            self.in_flight -= 1

    async def buy(
        self,
        token_id: str,
        side: BinarySide,
        contracts: float,
        max_price: float,
        *,
        condition_id: str | None = None,
        tick_size: str | None = None,
        neg_risk: bool | None = None,
    ) -> str:
        del side, contracts, max_price, condition_id, tick_size, neg_risk
        self.buy_calls += 1
        return f"mock-buy-{token_id}"

    async def sell(
        self,
        token_id: str,
        side: BinarySide,
        contracts: float,
        min_price: float,
        *,
        condition_id: str | None = None,
        tick_size: str | None = None,
        neg_risk: bool | None = None,
    ) -> str:
        del side, contracts, min_price, condition_id, tick_size, neg_risk
        self.sell_calls += 1
        return f"mock-sell-{token_id}"

    async def wait_filled(self, order_id: str, timeout_ms: int) -> ExecutionReport:
        del timeout_ms
        return ExecutionReport.from_amounts(order_id, 0, 0, "cancelled", 0)

    async def cancel_order(self, order_id: str) -> None:
        del order_id

    async def get_cash_balance(self) -> float:
        return 1_000.0

    async def get_market_constraints(
        self,
        token_id: str,
        condition_id: str | None = None,
    ) -> MarketConstraints:
        del token_id, condition_id
        return MarketConstraints(
            fee_rate_bps=25,
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.01"),
            minimum_notional=Decimal("1"),
        )


class _SilentTelegram(TelegramNotifier):
    def __init__(self) -> None:
        self.signal_count = 0

    async def send_html(self, message: str) -> None:
        del message

    async def send_signal(self, signal: ArbitrageSignal, is_test: bool, min_net_spread: float) -> None:
        del signal, is_test, min_net_spread
        self.signal_count += 1

    async def send_position_opened(self, signal: ArbitrageSignal, position: OpenPosition) -> None:
        del signal, position

    async def close(self) -> None:
        return


def _market(route: str, index: int) -> MarketSpec:
    second_venue = {
        "polymarket_predict": "Predict.fun",
        "polymarket_sx": "SX Bet",
        "polymarket_myriad": "Myriad",
    }[route]
    second_token = "" if route == "polymarket_myriad" else f"{route}-second-{index}"
    return MarketSpec(
        symbol=f"{route}-event-{index // 2}",
        target_label="YES",
        polymarket_token_id=f"{route}-poly-{index}",
        polymarket_side=BinarySide.YES,
        polymarket_market_id=f"{route}-poly-market-{index}",
        predict_fun_token_id=second_token,
        predict_fun_side=BinarySide.NO,
        predict_fun_market_id=None if route == "polymarket_myriad" else f"{route}-market-{index}",
        predict_fun_fee_rate_bps=25 if route == "polymarket_predict" else 0,
        venue_b_label=second_venue,
        myriad_market_id=f"myriad-{index}" if route == "polymarket_myriad" else None,
        myriad_side=BinarySide.NO,
        condition_id=f"condition-{route}-{index}",
        expires_at=datetime.now(UTC) + timedelta(hours=24),
        cutoff_at=datetime.now(UTC) + timedelta(hours=24),
        category="sports" if route == "polymarket_sx" else "crypto",
        mapping_status=MappingStatus.VERIFIED,
        verified_routes=frozenset({route}),
        rules_fingerprint=f"rules-{route}-{index}",
        resolution_source="mock official result",
        outcome_semantics="YES is the stated outcome",
    )


def _config(markets: list[MarketSpec], *, clob_hft: bool) -> AppConfig:
    base = load_config(Path(__file__).parents[1] / "config.example.json")
    enabled_routes = replace(
        base.routes,
        polymarket_predict=not clob_hft,
        polymarket_sx=clob_hft,
        polymarket_myriad=not clob_hft,
        predict_myriad=False,
        predict_sx=False,
        sx_myriad=False,
    )
    return replace(
        base,
        execution_mode=ExecutionMode.SHADOW,
        shadow_mode=True,
        scan_all=False,
        routes=enabled_routes,
        markets=markets,
        position_size_usd=20,
        max_order_size_usd=20,
        min_net_spread=0.01,
        max_open_positions=1,
        max_concurrent_market_evaluations=32 if clob_hft else 24,
        enable_predict_fun=not clob_hft,
        enable_sx_bet=clob_hft,
        predict_fun=replace(base.predict_fun, enabled=not clob_hft, api_key="mock"),
        sx_bet=replace(base.sx_bet, enabled=clob_hft),
        myriad_markets=replace(base.myriad_markets, enabled=not clob_hft),
        spread_policy=replace(
            base.spread_policy,
            route_floors={
                "polymarket_predict": 0.01,
                "polymarket_sx": 0.01,
                "polymarket_myriad": 0.01,
            },
            min_expected_profit_usd=0.05,
            safety_buffer_pct=0,
            fixed_chain_cost_usd=0.05,
        ),
    )


def _engine(
    config: AppConfig,
    counters: Counter[tuple[str, str]],
) -> tuple[ArbitrageEngine, tuple[_StressClient, ...], tuple[ExecutionRouter, ...], PositionLedger]:
    polymarket = _StressClient("Polymarket")
    predict = _StressClient("Predict.fun") if config.routes.polymarket_predict else None
    sx = _StressClient("SX Bet") if config.routes.polymarket_sx else None
    myriad = _StressClient("Myriad") if config.routes.polymarket_myriad else None
    telegram = _SilentTelegram()
    ledger = PositionLedger()
    market_locks: dict[str, asyncio.Lock] = {}
    capacity_lock = asyncio.Lock()
    pending_markets: set[str] = set()
    routers: list[ExecutionRouter] = []

    def router(second: _StressClient, label: str) -> ExecutionRouter:
        result = ExecutionRouter(
            config,
            polymarket,
            second,
            telegram,
            ledger,
            second_leg_label=label,
            market_locks=market_locks,
            capacity_lock=capacity_lock,
            pending_markets=pending_markets,
        )
        routers.append(result)
        return result

    predict_router = router(predict, "Predict.fun") if predict is not None else None
    sx_router = router(sx, "SX Bet") if sx is not None else None
    myriad_router = router(myriad, "Myriad") if myriad is not None else None
    engine = ArbitrageEngine(
        config,
        polymarket,
        predict,
        predict_router,
        sx_bet=sx,
        sx_execution=sx_router,
        myriad=myriad,
        myriad_execution=myriad_router,
        market_locks=market_locks,
        telegram=telegram,
        signal_evaluation_observer=lambda route, outcome, spread: counters.update([(route, outcome)]),
    )
    clients = tuple(client for client in (polymarket, predict, sx, myriad) if client is not None)
    return engine, clients, tuple(routers), ledger


@pytest.mark.asyncio
async def test_two_service_mock_stress_processes_at_least_10200_evaluations_without_execution_leaks() -> None:
    clob_markets = [_market("polymarket_sx", index) for index in range(100)]
    quote_markets = [
        *(_market("polymarket_predict", index) for index in range(100)),
        *(_market("polymarket_myriad", index) for index in range(100)),
    ]
    counters: Counter[tuple[str, str]] = Counter()
    clob_engine, clob_clients, clob_routers, clob_ledger = _engine(_config(clob_markets, clob_hft=True), counters)
    quote_engine, quote_clients, quote_routers, quote_ledger = _engine(
        _config(quote_markets, clob_hft=False),
        counters,
    )

    started = time.perf_counter()
    for cycle in range(284):
        for client in (*clob_clients, *quote_clients):
            client.cycle = cycle
        engines = [quote_engine.run_once()]
        if cycle < 107:
            engines.append(clob_engine.run_once())
        await asyncio.gather(*engines)
    elapsed = time.perf_counter() - started

    assert sum(counters.values()) >= 10_200
    assert sum(count for (route, _), count in counters.items() if route == "polymarket_sx") >= 3_400
    assert sum(count for (route, _), count in counters.items() if route == "polymarket_predict") >= 3_400
    assert sum(count for (route, _), count in counters.items() if route == "polymarket_myriad") >= 3_400
    assert max(client.max_in_flight for client in clob_clients) <= 32
    assert max(client.max_in_flight for client in quote_clients) <= 24
    assert sum(client.recovery_events for client in (*clob_clients, *quote_clients)) > 0
    assert sum(client.buy_calls + client.sell_calls for client in (*clob_clients, *quote_clients)) == 0
    assert clob_ledger.all() == []
    assert quote_ledger.all() == []
    assert elapsed < 30

    for router in (*clob_routers, *quote_routers):
        assert router._pending_markets == set()  # noqa: SLF001
        assert router._capital_reservations == {}  # noqa: SLF001
        assert router._active_orders == {}  # noqa: SLF001
        await router.close()
