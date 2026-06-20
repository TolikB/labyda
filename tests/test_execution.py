import asyncio
import time
import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock

from arbitrage_engine.config import (
    AppConfig,
    AutoCloseConfig,
    MyriadMarketsConfig,
    PolymarketConfig,
    PredictFunConfig,
    TelegramConfig,
    Web3NetworkConfig,
)
from arbitrage_engine.connectors.base import BinaryMarketClient
from arbitrage_engine.engine import ArbitrageEngine
from arbitrage_engine.execution import ExecutionRouter, _signal_key
from arbitrage_engine.models import (
    ArbitrageSignal,
    BinarySide,
    ExecutionReport,
    ExecutionStatus,
    ExitSignal,
    MarketConstraints,
    MarketSpec,
    OpenPosition,
    OrderBook,
    OrderBookLevel,
    PositionPlan,
    SpreadMetrics,
)
from arbitrage_engine.positions import PositionLedger
from arbitrage_engine.telegram import TelegramNotifier


class FakeBinaryClient(BinaryMarketClient):
    def __init__(self) -> None:
        self.bought = False
        self.sold = False
        self.cancelled = False
        self.fill_result = False
        self.fill_results: list[bool] = []
        self.partial_fill_results: list[float] = []
        self.order_amounts: dict[str, float] = {}
        self.sell_contracts: list[float] = []
        self.sell_calls = 0
        self.watch_tokens: list[str] = []
        self.bid = 0.55
        self.ask = 0.42
        self.cash_balance = 100.0
        self.book_timestamp = time.time()
        self.market_data_age: float | None = None
        self.reconnect_calls = 0

    async def watch_order_book(self, token_id: str) -> OrderBook:
        self.watch_tokens.append(token_id)
        return OrderBook(
            bids=[OrderBookLevel(self.bid, 1000)],
            asks=[OrderBookLevel(self.ask, 1000)],
            timestamp=self.book_timestamp,
        )

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
        del side, max_price, condition_id, tick_size, neg_risk
        self.bought = True
        order_id = f"buy-{token_id}"
        self.order_amounts[order_id] = contracts
        return order_id

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
        del side, min_price, condition_id, tick_size, neg_risk
        self.sold = True
        self.sell_calls += 1
        self.sell_contracts.append(contracts)
        order_id = f"sell-{token_id}"
        self.order_amounts[order_id] = contracts
        return order_id

    async def wait_filled(self, order_id: str, timeout_ms: int) -> ExecutionReport:
        del timeout_ms
        requested = self.order_amounts.get(order_id, 0.0)
        if self.partial_fill_results:
            amount_filled = self.partial_fill_results.pop(0)
            return ExecutionReport.from_amounts(order_id, requested, amount_filled, "partial")
        if self.fill_results:
            filled = self.fill_results.pop(0)
        else:
            filled = self.fill_result
        return ExecutionReport.from_amounts(
            order_id, requested, requested if filled else 0.0, "filled" if filled else "pending"
        )

    async def cancel_order(self, order_id: str) -> None:
        del order_id
        self.cancelled = True

    async def get_cash_balance(self) -> float:
        return self.cash_balance

    async def get_market_constraints(self, token_id: str, condition_id: str | None = None) -> MarketConstraints | None:
        del token_id, condition_id
        return MarketConstraints(
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.000001"),
            minimum_notional=Decimal("0.01"),
            fee_rate_bps=0,
        )

    def market_data_age_seconds(self) -> float | None:
        return self.market_data_age

    async def reconnect_market_data(self) -> None:
        self.reconnect_calls += 1


class FailingPredictClient(FakeBinaryClient):
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
        del token_id, side, contracts, max_price, condition_id, tick_size, neg_risk
        raise RuntimeError("predict failed")


class FakeTelegram(TelegramNotifier):
    def __init__(self) -> None:
        self.messages = 0
        self.closed = 0

    async def send_html(self, message: str) -> None:
        self.messages += 1

    async def send_signal(self, signal: ArbitrageSignal, is_test: bool, min_net_spread: float) -> None:
        del signal, is_test, min_net_spread
        self.messages += 1

    async def send_position_opened(self, signal: ArbitrageSignal, position: OpenPosition) -> None:
        del signal, position
        self.messages += 1

    async def close(self) -> None:
        self.closed += 1


def make_config(is_test: bool) -> AppConfig:
    return AppConfig(
        is_test=is_test,
        scan_all=False,
        position_size_usd=100,
        max_order_size_usd=100,
        min_net_spread=0.10,
        poll_interval_ms=250,
        polymarket_fill_timeout_ms=500,
        predict_fun_fill_timeout_ms=4000,
        myriad_fill_timeout_ms=4000,
        signal_alert_cooldown_seconds=900,
        categories_to_scan=["sports", "finance"],
        telegram=TelegramConfig(None, None),
        polymarket=PolymarketConfig(None, "https://clob.polymarket.com", 137, 0, None),
        predict_fun=PredictFunConfig(
            enabled=True,
            private_key=None,
            rpc_url="https://bsc-dataseed.binance.org",
            rpc_urls=["https://bsc-dataseed.binance.org"],
            chain_id=56,
            network="mainnet",
            api_base_url="https://api.predict.fun",
            api_key=None,
            ws_url=None,
            market_abi_path="abi/predict_fun_market.json",
            collateral_token_address=None,
            fee_rate_bps=0,
            precision=18,
            reserves_function="getPoolReserves",
            balance_function="balanceOf",
            max_priority_fee_gwei=3.0,
            confirmations=1,
            max_slippage_pct=0.015,
        ),
        myriad_markets=MyriadMarketsConfig(
            api_url="https://api-v2.myriadprotocol.com",
            ws_url="wss://ws.myriadprotocol.com/ws",
            api_key=None,
            private_key=None,
            rpc_url="https://bsc-dataseed.binance.org",
            rpc_urls=["https://bsc-dataseed.binance.org"],
            chain_id=56,
            exchange_address="0xa0b6f8ef8EdB64f395018D1933f2273Ce9f0f16A",
            conditional_tokens_address="0x6413734f92248D4B29ae35883290BD93212654Dc",
            collateral_tokens={},
            collateral_symbol="USDT",
            trading_fee_pct=0.0,
            max_slippage_pct=0.015,
            enabled=False,
        ),
        web3_networks={
            "bnb": Web3NetworkConfig(
                "https://bsc-dataseed.binance.org",
                ["https://bsc-dataseed.binance.org"],
                56,
                0.015,
                3.0,
                1,
            )
        },
        auto_close=AutoCloseConfig(True, 0.02),
        markets=[],
        shadow_mode=False,
    )


def make_market(expires_at: datetime | None = None) -> MarketSpec:
    return MarketSpec(
        symbol="BTC-USD",
        target_label=">$75,000",
        polymarket_token_id="poly-token",
        polymarket_side=BinarySide.YES,
        predict_fun_token_id="predict-token",
        predict_fun_side=BinarySide.NO,
        expires_at=expires_at,
        condition_id="condition",
        tick_size="0.01",
        neg_risk=False,
    )


def make_signal(net_spread: float = 0.11) -> ArbitrageSignal:
    return ArbitrageSignal(
        market=make_market(),
        plan=PositionPlan(100, 42, 100, 47, 100, 89),
        metrics=SpreadMetrics(0.11, net_spread, 11, 0, 0, 0.89),
        polymarket_price=0.42,
        predict_fun_price=0.47,
    )


class ExecutionTests(unittest.IsolatedAsyncioTestCase):
    async def test_entry_ledger_keeps_raw_fill_prices_when_fees_apply(self) -> None:
        first = FakeBinaryClient()
        second = FakeBinaryClient()
        first.fill_result = True
        second.fill_result = True
        config = make_config(False)
        config = replace(
            config,
            polymarket=replace(config.polymarket, trading_fee_pct=0.01),
            predict_fun=replace(config.predict_fun, fee_rate_bps=100),
        )
        router = ExecutionRouter(config, first, second, FakeTelegram())

        await router.handle_signal(make_signal())

        position = router.ledger.all()[0]
        self.assertEqual(position.polymarket_entry_price, 0.42)
        self.assertEqual(position.predict_fun_entry_price, 0.47)

    async def test_preflight_rejects_explicitly_stale_books(self) -> None:
        first = FakeBinaryClient()
        second = FakeBinaryClient()
        first.book_timestamp = time.time() - 5
        router = ExecutionRouter(
            replace(make_config(False), max_orderbook_age_seconds=2.0),
            first,
            second,
            FakeTelegram(),
        )

        await router.handle_signal(make_signal())

        self.assertFalse(first.bought)
        self.assertFalse(second.bought)

    async def test_shadow_scan_does_not_alert_from_stale_books(self) -> None:
        first = FakeBinaryClient()
        second = FakeBinaryClient()
        first.book_timestamp = time.time() - 5
        telegram = FakeTelegram()
        config = replace(make_config(True), markets=[make_market()], max_orderbook_age_seconds=2.0)
        router = ExecutionRouter(config, first, second, telegram)
        engine = ArbitrageEngine(config, first, second, router)

        await engine.run_once()

        self.assertEqual(telegram.messages, 0)

    async def test_market_data_heartbeat_reconnects_stale_stream(self) -> None:
        first = FakeBinaryClient()
        first.market_data_age = 30.0
        second = FakeBinaryClient()
        telegram = FakeTelegram()
        config = replace(
            make_config(True),
            websocket_heartbeat_interval_seconds=0.01,
            websocket_stale_after_seconds=10.0,
        )
        engine = ArbitrageEngine(
            config,
            first,
            second,
            None,
            telegram=telegram,
        )
        task = asyncio.create_task(engine._monitor_market_data_heartbeat())

        for _ in range(20):
            if first.reconnect_calls:
                break
            await asyncio.sleep(0.01)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

        self.assertGreaterEqual(first.reconnect_calls, 1)
        self.assertGreaterEqual(telegram.messages, 1)

    async def test_shadow_start_does_not_touch_live_balances(self) -> None:
        first = FakeBinaryClient()
        second = FakeBinaryClient()
        first.get_cash_balance = AsyncMock(  # type: ignore[method-assign]
            side_effect=AssertionError("live balance read")
        )
        second.get_cash_balance = AsyncMock(  # type: ignore[method-assign]
            side_effect=AssertionError("live balance read")
        )
        router = ExecutionRouter(
            replace(make_config(False), shadow_mode=True),
            first,
            second,
            FakeTelegram(),
        )

        await router.start()

        first.get_cash_balance.assert_not_awaited()
        second.get_cash_balance.assert_not_awaited()
        self.assertIsNone(router._balance_updater_task)
        await router.close()

    async def test_global_capital_reservation_prevents_cross_market_overallocation(self) -> None:
        class CountingClient(FakeBinaryClient):
            def __init__(self) -> None:
                super().__init__()
                self.buy_calls = 0

            async def buy(self, *args: Any, **kwargs: Any) -> str:
                self.buy_calls += 1
                return await super().buy(*args, **kwargs)

        first = CountingClient()
        second = CountingClient()
        first.fill_result = True
        second.fill_result = True
        first.cash_balance = 80.0
        second.cash_balance = 80.0
        ledger = PositionLedger()
        shared_balances: dict[str, float] = {}
        reservations: dict[str, float] = {}
        optimistic_debits: dict[str, float] = {}
        router = ExecutionRouter(
            make_config(False),
            first,
            second,
            FakeTelegram(),
            ledger,
            capacity_lock=asyncio.Lock(),
            balance_cache=shared_balances,
            capital_reservations=reservations,
            optimistic_debits=optimistic_debits,
        )
        first_signal = make_signal()
        signals = [
            replace(
                first_signal,
                market=replace(
                    first_signal.market,
                    symbol=f"MARKET-{index}",
                    polymarket_token_id=f"poly-{index}",
                    predict_fun_token_id=f"predict-{index}",
                ),
            )
            for index in range(10)
        ]

        await asyncio.gather(*(router.handle_signal(signal) for signal in signals))

        self.assertEqual(len(ledger.all()), 1)
        self.assertEqual(first.buy_calls, 1)
        self.assertEqual(second.buy_calls, 1)
        self.assertEqual(reservations, {})
        self.assertEqual(optimistic_debits, {"Polymarket": 42.0, "Predict.fun": 47.0})
        first.cash_balance = 38.0
        second.cash_balance = 33.0
        await router._refresh_balances()
        self.assertEqual(optimistic_debits, {})
        await router.close()

    async def test_entry_orders_are_submitted_concurrently(self) -> None:
        ready = 0
        both_started = asyncio.Event()

        class CoordinatedEntryClient(FakeBinaryClient):
            async def buy(self, *args: Any, **kwargs: Any) -> str:
                nonlocal ready
                ready += 1
                if ready == 2:
                    both_started.set()
                await asyncio.wait_for(both_started.wait(), timeout=0.1)
                return await super().buy(*args, **kwargs)

        first = CoordinatedEntryClient()
        second = CoordinatedEntryClient()
        first.fill_result = True
        second.fill_result = True
        router = ExecutionRouter(make_config(False), first, second, FakeTelegram())

        await router.handle_signal(make_signal())

        self.assertEqual(ready, 2)
        self.assertEqual(len(router.ledger.all()), 1)
        await router.close()

    async def test_signal_key_falls_back_when_token_ids_are_empty(self) -> None:
        signal = make_signal()
        signal = replace(
            signal,
            market=replace(
                signal.market,
                rules_fingerprint=None,
                polymarket_token_id="",
                predict_fun_token_id="",
            ),
        )

        self.assertEqual(_signal_key(signal), "BTC-USD:>$75,000")

    async def test_market_lock_prevents_concurrent_cross_route_entries(self) -> None:
        ledger = PositionLedger()
        market_locks: dict[str, asyncio.Lock] = {}
        capacity_lock = asyncio.Lock()
        pending_markets: set[str] = set()
        clients = [FakeBinaryClient() for _ in range(4)]
        for client in clients:
            client.fill_result = True
        telegram = FakeTelegram()
        router_a = ExecutionRouter(
            make_config(False),
            clients[0],
            clients[1],
            telegram,
            ledger,
            market_locks=market_locks,
            capacity_lock=capacity_lock,
            pending_markets=pending_markets,
        )
        router_b = ExecutionRouter(
            make_config(False),
            clients[2],
            clients[3],
            telegram,
            ledger,
            second_leg_label="Myriad",
            market_locks=market_locks,
            capacity_lock=capacity_lock,
            pending_markets=pending_markets,
        )
        signal_a = make_signal()
        signal_b = replace(
            signal_a,
            market=replace(
                signal_a.market,
                predict_fun_token_id="myriad-token",
                venue_b_label="Myriad",
            ),
        )

        await asyncio.gather(router_a.handle_signal(signal_a), router_b.handle_signal(signal_b))

        self.assertEqual(len(ledger.all()), 1)
        self.assertEqual(sum(int(client.bought) for client in clients), 2)
        await router_a.close()
        await router_b.close()

    async def test_max_open_positions_rejects_new_market(self) -> None:
        ledger = PositionLedger()
        ledger.add(
            OpenPosition(
                market=replace(make_market(), symbol="ETH-USD"),
                polymarket_contracts=1,
                polymarket_entry_price=0.4,
                predict_fun_contracts=1,
                predict_fun_entry_price=0.5,
                opened_at=datetime.now(UTC),
                polymarket_order_id="one",
                predict_fun_order_id="two",
            )
        )
        poly = FakeBinaryClient()
        predict = FakeBinaryClient()
        router = ExecutionRouter(
            replace(make_config(False), max_open_positions=1),
            poly,
            predict,
            FakeTelegram(),
            ledger,
        )

        await router.handle_signal(make_signal())

        self.assertFalse(poly.bought)
        self.assertFalse(predict.bought)

    async def test_preflight_uses_independent_polymarket_slippage_cap(self) -> None:
        poly = FakeBinaryClient()
        poly.ask = 0.421
        predict = FakeBinaryClient()
        config = make_config(False)
        config = replace(
            config,
            polymarket=replace(config.polymarket, max_slippage_pct=0.001),
            predict_fun=replace(config.predict_fun, max_slippage_pct=0.015),
        )
        router = ExecutionRouter(config, poly, predict, FakeTelegram())

        await router.handle_signal(make_signal())

        self.assertFalse(poly.bought)
        self.assertFalse(predict.bought)

    async def test_exit_orders_are_submitted_concurrently(self) -> None:
        ready = 0
        both_started = asyncio.Event()

        class CoordinatedExitClient(FakeBinaryClient):
            async def sell(self, *args: Any, **kwargs: Any) -> str:
                nonlocal ready
                ready += 1
                if ready == 2:
                    both_started.set()
                await asyncio.wait_for(both_started.wait(), timeout=0.1)
                return await super().sell(*args, **kwargs)

        first = CoordinatedExitClient()
        second = CoordinatedExitClient()
        first.fill_result = True
        second.fill_result = True
        ledger = PositionLedger()
        position = OpenPosition(
            market=make_market(),
            polymarket_contracts=10,
            polymarket_entry_price=0.42,
            predict_fun_contracts=10,
            predict_fun_entry_price=0.47,
            opened_at=datetime.now(UTC),
            polymarket_order_id="entry-a",
            predict_fun_order_id="entry-b",
        )
        ledger.add(position)
        router = ExecutionRouter(make_config(False), first, second, FakeTelegram(), ledger)

        await router.handle_exit_signal(ExitSignal(position, 0.5, 0.5, 0.1, 1.0))

        self.assertEqual(ready, 2)
        self.assertEqual(ledger.all(), [])

    async def test_invalid_zero_cost_position_does_not_stop_monitoring_cycle(self) -> None:
        first = FakeBinaryClient()
        second = FakeBinaryClient()
        ledger = PositionLedger()
        ledger.add(
            OpenPosition(
                market=make_market(),
                polymarket_contracts=10,
                polymarket_entry_price=0.0,
                predict_fun_contracts=0.0,
                predict_fun_entry_price=0.0,
                opened_at=datetime.now(UTC),
                polymarket_order_id="pending",
                predict_fun_order_id="",
            )
        )
        config = make_config(False)
        router = ExecutionRouter(config, first, second, FakeTelegram(), ledger)
        engine = ArbitrageEngine(config, first, second, router)

        await engine.run_once()

        self.assertEqual(len(ledger.all()), 1)

    async def test_close_releases_telegram_resources(self) -> None:
        telegram = FakeTelegram()
        router = ExecutionRouter(make_config(True), FakeBinaryClient(), FakeBinaryClient(), telegram)

        await router.close()

        self.assertEqual(telegram.closed, 1)

    async def test_execution_report_exposes_partial_fill_details(self) -> None:
        report = ExecutionReport.from_amounts("order", 100.0, 40.0, "partial", 0.42)

        self.assertEqual(report.status, ExecutionStatus.PARTIAL)
        self.assertEqual(report.amount_requested, 100.0)
        self.assertEqual(report.amount_filled, 40.0)
        self.assertEqual(report.remaining_amount, 60.0)
        self.assertEqual(report.avg_price, 0.42)

    async def test_dry_run_sends_telegram_without_orders(self) -> None:
        poly = FakeBinaryClient()
        predict = FakeBinaryClient()
        telegram = FakeTelegram()
        router = ExecutionRouter(make_config(True), poly, predict, telegram)

        await router.handle_signal(make_signal())

        self.assertFalse(poly.bought)
        self.assertFalse(predict.bought)
        self.assertEqual(telegram.messages, 1)

    async def test_dry_run_signal_alert_is_throttled_per_pair(self) -> None:
        poly = FakeBinaryClient()
        predict = FakeBinaryClient()
        telegram = FakeTelegram()
        router = ExecutionRouter(make_config(True), poly, predict, telegram)

        await router.handle_signal(make_signal())
        await router.handle_signal(make_signal())

        self.assertEqual(telegram.messages, 1)

    async def test_parallel_entry_cancels_both_unfilled_legs(self) -> None:
        poly = FakeBinaryClient()
        predict = FakeBinaryClient()
        router = ExecutionRouter(make_config(False), poly, predict, FakeTelegram())

        await router.handle_signal(make_signal())

        self.assertTrue(poly.bought)
        self.assertTrue(poly.cancelled)
        self.assertTrue(predict.bought)
        self.assertTrue(predict.cancelled)

    async def test_production_unwinds_polymarket_when_predict_leg_fails(self) -> None:
        poly = FakeBinaryClient()
        poly.fill_results = [True, True]
        predict = FailingPredictClient()
        telegram = FakeTelegram()
        router = ExecutionRouter(make_config(False), poly, predict, telegram)

        await router.handle_signal(make_signal())

        self.assertTrue(poly.bought)
        self.assertTrue(poly.sold)
        self.assertEqual(poly.sell_calls, 1)
        self.assertEqual(telegram.messages, 2)

    async def test_parallel_entry_unwinds_second_leg_when_first_leg_fails(self) -> None:
        first = FailingPredictClient()
        second = FakeBinaryClient()
        second.fill_results = [True, True]
        telegram = FakeTelegram()
        router = ExecutionRouter(make_config(False), first, second, telegram)

        await router.handle_signal(make_signal())

        self.assertTrue(second.bought)
        self.assertTrue(second.sold)
        self.assertEqual(second.sell_calls, 1)
        self.assertEqual(router.ledger.all(), [])
        self.assertEqual(telegram.messages, 2)

    async def test_production_open_sends_signal_and_open_notifications(self) -> None:
        poly = FakeBinaryClient()
        poly.fill_results = [True, *([False] * 7)]
        predict = FakeBinaryClient()
        predict.fill_result = True
        telegram = FakeTelegram()
        router = ExecutionRouter(make_config(False), poly, predict, telegram)

        await router.handle_signal(make_signal())

        self.assertEqual(telegram.messages, 2)
        self.assertEqual(len(router.ledger.all()), 1)

    async def test_spread_guard_rejects_both_legs_at_preflight(self) -> None:
        poly = FakeBinaryClient()
        poly.fill_results = [True, True]
        poly.ask = 0.51
        predict = FakeBinaryClient()
        predict.ask = 0.445
        predict.fill_result = False
        telegram = FakeTelegram()
        router = ExecutionRouter(make_config(False), poly, predict, telegram)
        signal = ArbitrageSignal(
            market=make_market(),
            plan=PositionPlan(100, 51, 100, 44.5, 100, 95.5),
            metrics=SpreadMetrics(0.11, 0.11, 11, 0, 0, 0.89),
            polymarket_price=0.51,
            predict_fun_price=0.445,
        )

        started = time.perf_counter()
        await router.handle_signal(signal)
        elapsed_ms = (time.perf_counter() - started) * 1000

        self.assertFalse(poly.bought)
        self.assertFalse(predict.bought)
        self.assertFalse(poly.sold)
        self.assertLess(elapsed_ms, 50)

    async def test_partial_second_leg_unwinds_only_unmatched_delta(self) -> None:
        poly = FakeBinaryClient()
        poly.fill_results = [True, True]
        predict = FakeBinaryClient()
        predict.partial_fill_results = [40.0]
        ledger = PositionLedger()
        router = ExecutionRouter(make_config(False), poly, predict, FakeTelegram(), ledger)

        await router.handle_signal(make_signal())

        self.assertTrue(predict.cancelled)
        self.assertEqual(poly.sell_contracts, [60.0])
        positions = ledger.all()
        self.assertEqual(len(positions), 1)
        self.assertEqual(positions[0].status, "open")
        self.assertEqual(positions[0].polymarket_contracts, 40.0)
        self.assertEqual(positions[0].predict_fun_contracts, 40.0)

    async def test_production_skips_new_position_when_reserved_balance_is_insufficient(self) -> None:
        poly = FakeBinaryClient()
        poly.fill_result = True
        poly.cash_balance = 80
        predict = FakeBinaryClient()
        predict.fill_result = True
        predict.cash_balance = 80
        telegram = FakeTelegram()
        ledger = PositionLedger()
        config = make_config(False)
        router = ExecutionRouter(config, poly, predict, telegram, ledger)

        await router.handle_signal(make_signal())
        await router.handle_signal(
            ArbitrageSignal(
                market=replace(
                    make_market(), polymarket_token_id="poly-token-2", predict_fun_token_id="predict-token-2"
                ),
                plan=PositionPlan(100, 42, 100, 47, 100, 89),
                metrics=SpreadMetrics(0.11, 0.11, 11, 0, 0, 0.89),
                polymarket_price=0.42,
                predict_fun_price=0.47,
            )
        )

        self.assertEqual(len(ledger.all()), 1)

    async def test_failed_predict_leg_creates_pending_unwind_without_raising(self) -> None:
        poly = FakeBinaryClient()
        poly.fill_results = [True, *([False] * 7)]
        predict = FailingPredictClient()
        telegram = FakeTelegram()
        router = ExecutionRouter(make_config(False), poly, predict, telegram)

        await router.handle_signal(make_signal())

        positions = router.ledger.all()
        self.assertEqual(len(positions), 1)
        self.assertEqual(positions[0].status, "unwind_pending")
        self.assertEqual(poly.sell_calls, 1)

    async def test_pending_unwind_retries_and_removes_position_when_filled(self) -> None:
        poly = FakeBinaryClient()
        poly.fill_result = True
        predict = FakeBinaryClient()
        telegram = FakeTelegram()
        ledger = PositionLedger()
        ledger.add(
            OpenPosition(
                market=make_market(),
                polymarket_contracts=100,
                polymarket_entry_price=0.42,
                predict_fun_contracts=0,
                predict_fun_entry_price=0,
                opened_at=datetime.now(UTC),
                polymarket_order_id="poly",
                predict_fun_order_id="",
                status="unwind_pending",
            )
        )
        config = make_config(False)
        router = ExecutionRouter(config, poly, predict, telegram, ledger)
        engine = ArbitrageEngine(config, poly, predict, router)

        await engine.run_once()

        self.assertEqual(ledger.all(), [])
        self.assertEqual(telegram.messages, 1)

    async def test_engine_evaluates_predict_fun_myriad_pair(self) -> None:
        poly = FakeBinaryClient()
        poly.ask = 0.40
        predict = FakeBinaryClient()
        predict.ask = 0.45
        myriad = FakeBinaryClient()
        myriad.ask = 0.44
        telegram = FakeTelegram()
        ledger = PositionLedger()
        config = make_config(True)
        market = replace(make_market(), myriad_market_id="123", myriad_side=BinarySide.NO)
        config = replace(
            config,
            myriad_markets=replace(config.myriad_markets, enabled=True),
            markets=[market],
        )
        poly_predict = ExecutionRouter(config, poly, predict, telegram, ledger)
        poly_myriad = ExecutionRouter(
            config,
            poly,
            myriad,
            telegram,
            ledger,
            second_leg_label="Myriad",
        )
        predict_myriad = ExecutionRouter(
            config,
            predict,
            myriad,
            telegram,
            ledger,
            first_leg_label="Predict.fun",
            second_leg_label="Myriad",
            first_leg_fill_timeout_ms=config.predict_fun_fill_timeout_ms,
            second_leg_fill_timeout_ms=config.predict_fun_fill_timeout_ms,
        )
        engine = ArbitrageEngine(
            config,
            poly,
            predict,
            poly_predict,
            myriad=myriad,
            myriad_execution=poly_myriad,
            predict_myriad_execution=predict_myriad,
        )

        await engine.run_once()

        self.assertIn("predict-token", predict.watch_tokens)
        self.assertIn("123:NO", myriad.watch_tokens)
        self.assertIn("123:YES", myriad.watch_tokens)
        self.assertEqual(telegram.messages, 3)

    async def test_engine_runs_polymarket_myriad_without_predict_fun(self) -> None:
        poly = FakeBinaryClient()
        poly.ask = 0.40
        myriad = FakeBinaryClient()
        myriad.ask = 0.44
        telegram = FakeTelegram()
        ledger = PositionLedger()
        market = replace(make_market(), predict_fun_token_id="", myriad_market_id="123", myriad_side=BinarySide.NO)
        config = make_config(True)
        config = replace(
            config,
            predict_fun=replace(config.predict_fun, enabled=False, api_key=None),
            myriad_markets=replace(config.myriad_markets, enabled=True),
            markets=[market],
        )
        poly_myriad = ExecutionRouter(
            config,
            poly,
            myriad,
            telegram,
            ledger,
            second_leg_label="Myriad",
        )
        engine = ArbitrageEngine(
            config,
            poly,
            None,
            None,
            myriad=myriad,
            myriad_execution=poly_myriad,
        )

        await engine.run_once()

        self.assertIn("poly-token", poly.watch_tokens)
        self.assertIn("123:NO", myriad.watch_tokens)
        self.assertEqual(telegram.messages, 1)

    async def test_auto_close_dry_run_sends_exit_message_without_orders(self) -> None:
        poly = FakeBinaryClient()
        predict = FakeBinaryClient()
        telegram = FakeTelegram()
        ledger = PositionLedger()
        market = make_market(datetime.now(UTC) + timedelta(minutes=30))
        position = OpenPosition(
            market=market,
            polymarket_contracts=100,
            polymarket_entry_price=0.42,
            predict_fun_contracts=100,
            predict_fun_entry_price=0.50,
            opened_at=datetime.now(UTC),
            polymarket_order_id="poly",
            predict_fun_order_id="predict",
        )
        ledger.add(position)
        config = make_config(True)
        router = ExecutionRouter(config, poly, predict, telegram, ledger)
        engine = ArbitrageEngine(config, poly, predict, router)

        await engine.run_once()

        self.assertEqual(telegram.messages, 1)
        self.assertFalse(poly.sold)
        self.assertFalse(predict.sold)
        self.assertEqual(ledger.all(), [position])

    async def test_auto_close_production_keeps_position_when_exit_not_filled(self) -> None:
        poly = FakeBinaryClient()
        predict = FakeBinaryClient()
        telegram = FakeTelegram()
        ledger = PositionLedger()
        market = make_market(datetime.now(UTC) + timedelta(minutes=30))
        ledger.add(
            OpenPosition(
                market=market,
                polymarket_contracts=100,
                polymarket_entry_price=0.42,
                predict_fun_contracts=100,
                predict_fun_entry_price=0.50,
                opened_at=datetime.now(UTC),
                polymarket_order_id="poly",
                predict_fun_order_id="predict",
            )
        )
        config = make_config(False)
        router = ExecutionRouter(config, poly, predict, telegram, ledger)
        engine = ArbitrageEngine(config, poly, predict, router)

        await engine.run_once()

        self.assertTrue(poly.cancelled)
        self.assertTrue(predict.cancelled)
        self.assertEqual(len(ledger.all()), 1)
        self.assertEqual(ledger.all()[0].status, "partial_exit_pending")
        self.assertEqual(telegram.messages, 1)

    async def test_auto_close_partial_fill_marks_closed_leg_only(self) -> None:
        poly = FakeBinaryClient()
        poly.fill_result = True
        predict = FakeBinaryClient()
        predict.fill_result = False
        telegram = FakeTelegram()
        ledger = PositionLedger()
        market = make_market(datetime.now(UTC) + timedelta(minutes=30))
        ledger.add(
            OpenPosition(
                market=market,
                polymarket_contracts=100,
                polymarket_entry_price=0.42,
                predict_fun_contracts=100,
                predict_fun_entry_price=0.50,
                opened_at=datetime.now(UTC),
                polymarket_order_id="poly",
                predict_fun_order_id="predict",
            )
        )
        config = make_config(False)
        router = ExecutionRouter(config, poly, predict, telegram, ledger)
        engine = ArbitrageEngine(config, poly, predict, router)

        await engine.run_once()

        position = ledger.all()[0]
        self.assertEqual(position.status, "partial_exit_pending")
        self.assertTrue(position.polymarket_closed)
        self.assertFalse(position.predict_fun_closed)
        self.assertEqual(poly.sell_calls, 1)

        predict.fill_result = True
        await engine.run_once()

        self.assertEqual(ledger.all(), [])
        self.assertEqual(poly.sell_calls, 1)

    async def test_unfilled_orders_do_not_consume_optimistic_balance(self) -> None:
        poly = FakeBinaryClient()
        predict = FakeBinaryClient()
        optimistic_debits: dict[str, float] = {}
        router = ExecutionRouter(
            make_config(False),
            poly,
            predict,
            FakeTelegram(),
            optimistic_debits=optimistic_debits,
        )

        await router.handle_signal(make_signal())

        self.assertEqual(optimistic_debits, {})

    async def test_partial_balance_refresh_reconciles_only_observed_debit(self) -> None:
        router = ExecutionRouter(make_config(False), FakeBinaryClient(), FakeBinaryClient(), FakeTelegram())
        router._balance_cache["Polymarket"] = 100.0
        router._optimistic_debits["Polymarket"] = 40.0

        router._apply_balance_refresh("Polymarket", 85.0)

        self.assertEqual(router._optimistic_debits["Polymarket"], 25.0)
        self.assertEqual(router._effective_balance("Polymarket"), 60.0)

    async def test_partial_exit_retries_only_remaining_contracts_and_accumulates_proceeds(self) -> None:
        poly = FakeBinaryClient()
        poly.partial_fill_results = [40.0]
        predict = FakeBinaryClient()
        predict.fill_result = True
        ledger = PositionLedger()
        position = OpenPosition(
            market=make_market(),
            polymarket_contracts=100,
            polymarket_entry_price=0.42,
            predict_fun_contracts=100,
            predict_fun_entry_price=0.47,
            opened_at=datetime.now(UTC),
            polymarket_order_id="entry-a",
            predict_fun_order_id="entry-b",
        )
        ledger.add(position)
        router = ExecutionRouter(make_config(False), poly, predict, FakeTelegram(), ledger)

        await router._close_position_legs(position, polymarket_exit_price=0.50, predict_fun_exit_price=0.55)
        pending = ledger.all()[0]
        self.assertEqual(pending.polymarket_closed_contracts, 40.0)
        self.assertEqual(poly.sell_contracts, [100.0])

        poly.partial_fill_results = [60.0]
        await router.retry_partial_exit(pending)

        self.assertEqual(poly.sell_contracts, [100.0, 60.0])
        self.assertEqual(ledger.all(), [])

    async def test_cancellation_leaves_durable_entry_intent_and_cancels_live_orders(self) -> None:
        both_waiting = asyncio.Event()
        waiting = 0

        class BlockingClient(FakeBinaryClient):
            async def wait_filled(self, order_id: str, timeout_ms: int) -> ExecutionReport:
                del order_id, timeout_ms
                nonlocal waiting
                waiting += 1
                if waiting == 2:
                    both_waiting.set()
                await asyncio.Event().wait()
                raise AssertionError("unreachable")

        first = BlockingClient()
        second = BlockingClient()
        router = ExecutionRouter(make_config(False), first, second, FakeTelegram())
        task = asyncio.create_task(router.handle_signal(make_signal()))
        await asyncio.wait_for(both_waiting.wait(), timeout=0.2)

        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

        self.assertTrue(first.cancelled)
        self.assertTrue(second.cancelled)
        self.assertEqual(router.ledger.all()[0].status, "entry_pending")


if __name__ == "__main__":
    unittest.main()
