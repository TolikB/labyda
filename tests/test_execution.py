import unittest
import time
from dataclasses import replace
from datetime import datetime, timedelta, timezone

from arbitrage_engine.config import (
    AppConfig,
    AutoCloseConfig,
    MyriadMarketsConfig,
    PolymarketConfig,
    PredictFunConfig,
    TelegramConfig,
    Web3NetworkConfig,
)
from arbitrage_engine.engine import ArbitrageEngine
from arbitrage_engine.execution import ExecutionRouter
from arbitrage_engine.models import (
    ArbitrageSignal,
    BinarySide,
    ExecutionReport,
    MarketSpec,
    OpenPosition,
    OrderBook,
    OrderBookLevel,
    PositionPlan,
    SpreadMetrics,
)
from arbitrage_engine.positions import PositionLedger


class FakeBinaryClient:
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

    async def watch_order_book(self, token_id: str):
        self.watch_tokens.append(token_id)
        return OrderBook(bids=[OrderBookLevel(self.bid, 1000)], asks=[OrderBookLevel(self.ask, 1000)])

    async def buy(self, token_id, side, contracts, max_price, **kwargs):
        self.bought = True
        order_id = f"buy-{token_id}"
        self.order_amounts[order_id] = contracts
        return order_id

    async def sell(self, token_id, side, contracts, min_price, **kwargs):
        self.sold = True
        self.sell_calls += 1
        self.sell_contracts.append(contracts)
        order_id = f"sell-{token_id}"
        self.order_amounts[order_id] = contracts
        return order_id

    async def wait_filled(self, order_id, timeout_ms):
        requested = self.order_amounts.get(order_id, 0.0)
        if self.partial_fill_results:
            amount_filled = self.partial_fill_results.pop(0)
            return ExecutionReport.from_amounts(order_id, requested, amount_filled, "partial")
        if self.fill_results:
            filled = self.fill_results.pop(0)
        else:
            filled = self.fill_result
        return ExecutionReport.from_amounts(order_id, requested, requested if filled else 0.0, "filled" if filled else "pending")

    async def cancel_order(self, order_id):
        self.cancelled = True

    async def get_cash_balance(self):
        return self.cash_balance


class FailingPredictClient(FakeBinaryClient):
    async def buy(self, token_id, side, contracts, max_price, **kwargs):
        raise RuntimeError("predict failed")


class FakeTelegram:
    def __init__(self) -> None:
        self.messages = 0

    async def send_html(self, message: str) -> None:
        self.messages += 1

    async def send_signal(self, signal, is_test, min_net_spread):
        self.messages += 1

    async def send_position_opened(self, signal, position):
        self.messages += 1


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
    )


def make_market(expires_at=None) -> MarketSpec:
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
    async def test_dry_run_sends_telegram_without_orders(self) -> None:
        poly = FakeBinaryClient()
        predict = FakeBinaryClient()
        telegram = FakeTelegram()
        router = ExecutionRouter(make_config(True), poly, predict, telegram)  # type: ignore[arg-type]

        await router.handle_signal(make_signal())

        self.assertFalse(poly.bought)
        self.assertFalse(predict.bought)
        self.assertEqual(telegram.messages, 1)

    async def test_dry_run_signal_alert_is_throttled_per_pair(self) -> None:
        poly = FakeBinaryClient()
        predict = FakeBinaryClient()
        telegram = FakeTelegram()
        router = ExecutionRouter(make_config(True), poly, predict, telegram)  # type: ignore[arg-type]

        await router.handle_signal(make_signal())
        await router.handle_signal(make_signal())

        self.assertEqual(telegram.messages, 1)

    async def test_production_cancels_unfilled_polymarket_without_predict_leg(self) -> None:
        poly = FakeBinaryClient()
        predict = FakeBinaryClient()
        router = ExecutionRouter(make_config(False), poly, predict, FakeTelegram())  # type: ignore[arg-type]

        await router.handle_signal(make_signal())

        self.assertTrue(poly.bought)
        self.assertTrue(poly.cancelled)
        self.assertFalse(predict.bought)

    async def test_production_unwinds_polymarket_when_predict_leg_fails(self) -> None:
        poly = FakeBinaryClient()
        poly.fill_results = [True, True]
        predict = FailingPredictClient()
        telegram = FakeTelegram()
        router = ExecutionRouter(make_config(False), poly, predict, telegram)  # type: ignore[arg-type]

        await router.handle_signal(make_signal())

        self.assertTrue(poly.bought)
        self.assertTrue(poly.sold)
        self.assertEqual(poly.sell_calls, 1)
        self.assertEqual(telegram.messages, 2)

    async def test_production_open_sends_signal_and_open_notifications(self) -> None:
        poly = FakeBinaryClient()
        poly.fill_results = [True, *([False] * 7)]
        predict = FakeBinaryClient()
        predict.fill_result = True
        telegram = FakeTelegram()
        router = ExecutionRouter(make_config(False), poly, predict, telegram)  # type: ignore[arg-type]

        await router.handle_signal(make_signal())

        self.assertEqual(telegram.messages, 2)
        self.assertEqual(len(router.ledger.all()), 1)

    async def test_spread_guard_cancels_second_leg_and_unwinds_quickly(self) -> None:
        poly = FakeBinaryClient()
        poly.fill_results = [True, True]
        poly.ask = 0.51
        predict = FakeBinaryClient()
        predict.ask = 0.445
        predict.fill_result = False
        telegram = FakeTelegram()
        router = ExecutionRouter(make_config(False), poly, predict, telegram)  # type: ignore[arg-type]
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

        self.assertTrue(predict.cancelled)
        self.assertTrue(poly.sold)
        self.assertLess(elapsed_ms, 50)

    async def test_partial_second_leg_unwinds_only_unmatched_delta(self) -> None:
        poly = FakeBinaryClient()
        poly.fill_results = [True, True]
        predict = FakeBinaryClient()
        predict.partial_fill_results = [40.0]
        ledger = PositionLedger()
        router = ExecutionRouter(make_config(False), poly, predict, FakeTelegram(), ledger)  # type: ignore[arg-type]

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
        router = ExecutionRouter(config, poly, predict, telegram, ledger)  # type: ignore[arg-type]

        await router.handle_signal(make_signal())
        await router.handle_signal(
            ArbitrageSignal(
                market=replace(make_market(), polymarket_token_id="poly-token-2", predict_fun_token_id="predict-token-2"),
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
        router = ExecutionRouter(make_config(False), poly, predict, telegram)  # type: ignore[arg-type]

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
                opened_at=datetime.now(timezone.utc),
                polymarket_order_id="poly",
                predict_fun_order_id="",
                status="unwind_pending",
            )
        )
        config = make_config(False)
        router = ExecutionRouter(config, poly, predict, telegram, ledger)  # type: ignore[arg-type]
        engine = ArbitrageEngine(config, poly, predict, router)  # type: ignore[arg-type]

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
        poly_predict = ExecutionRouter(config, poly, predict, telegram, ledger)  # type: ignore[arg-type]
        poly_myriad = ExecutionRouter(
            config,
            poly,
            myriad,
            telegram,
            ledger,
            second_leg_label="Myriad",
        )  # type: ignore[arg-type]
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
        )  # type: ignore[arg-type]
        engine = ArbitrageEngine(
            config,
            poly,
            predict,
            poly_predict,
            myriad=myriad,
            myriad_execution=poly_myriad,
            predict_myriad_execution=predict_myriad,
        )  # type: ignore[arg-type]

        await engine.run_once()

        self.assertIn("predict-token", predict.watch_tokens)
        self.assertIn("123:NO", myriad.watch_tokens)
        self.assertEqual(telegram.messages, 3)

    async def test_auto_close_dry_run_sends_exit_message_without_orders(self) -> None:
        poly = FakeBinaryClient()
        predict = FakeBinaryClient()
        telegram = FakeTelegram()
        ledger = PositionLedger()
        market = make_market(datetime.now(timezone.utc) + timedelta(minutes=30))
        ledger.add(
            OpenPosition(
                market=market,
                polymarket_contracts=100,
                polymarket_entry_price=0.42,
                predict_fun_contracts=100,
                predict_fun_entry_price=0.50,
                opened_at=datetime.now(timezone.utc),
                polymarket_order_id="poly",
                predict_fun_order_id="predict",
            )
        )
        config = make_config(True)
        router = ExecutionRouter(config, poly, predict, telegram, ledger)  # type: ignore[arg-type]
        engine = ArbitrageEngine(config, poly, predict, router)  # type: ignore[arg-type]

        await engine.run_once()

        self.assertEqual(telegram.messages, 1)
        self.assertFalse(poly.sold)
        self.assertFalse(predict.sold)
        self.assertEqual(ledger.all(), [])

    async def test_auto_close_production_keeps_position_when_exit_not_filled(self) -> None:
        poly = FakeBinaryClient()
        predict = FakeBinaryClient()
        telegram = FakeTelegram()
        ledger = PositionLedger()
        market = make_market(datetime.now(timezone.utc) + timedelta(minutes=30))
        ledger.add(
            OpenPosition(
                market=market,
                polymarket_contracts=100,
                polymarket_entry_price=0.42,
                predict_fun_contracts=100,
                predict_fun_entry_price=0.50,
                opened_at=datetime.now(timezone.utc),
                polymarket_order_id="poly",
                predict_fun_order_id="predict",
            )
        )
        config = make_config(False)
        router = ExecutionRouter(config, poly, predict, telegram, ledger)  # type: ignore[arg-type]
        engine = ArbitrageEngine(config, poly, predict, router)  # type: ignore[arg-type]

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
        market = make_market(datetime.now(timezone.utc) + timedelta(minutes=30))
        ledger.add(
            OpenPosition(
                market=market,
                polymarket_contracts=100,
                polymarket_entry_price=0.42,
                predict_fun_contracts=100,
                predict_fun_entry_price=0.50,
                opened_at=datetime.now(timezone.utc),
                polymarket_order_id="poly",
                predict_fun_order_id="predict",
            )
        )
        config = make_config(False)
        router = ExecutionRouter(config, poly, predict, telegram, ledger)  # type: ignore[arg-type]
        engine = ArbitrageEngine(config, poly, predict, router)  # type: ignore[arg-type]

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


if __name__ == "__main__":
    unittest.main()
