import unittest
from datetime import datetime, timedelta, timezone

from arbitrage_engine.config import AppConfig, AutoCloseConfig, BinanceConfig, PolymarketConfig, TelegramConfig
from arbitrage_engine.engine import ArbitrageEngine
from arbitrage_engine.execution import ExecutionRouter
from arbitrage_engine.models import (
    ArbitrageSignal,
    HedgeSide,
    MarketSpec,
    OpenPosition,
    OrderBook,
    OrderBookLevel,
    PolymarketSide,
    PositionPlan,
    SpreadMetrics,
)
from arbitrage_engine.positions import PositionLedger


class FakePolymarket:
    def __init__(self) -> None:
        self.created = False
        self.cancelled = False
        self.closed = False
        self.fill_result = False

    async def watch_order_book(self, token_id: str):
        return OrderBook(
            bids=[OrderBookLevel(0.55, 1000)],
            asks=[OrderBookLevel(0.56, 1000)],
        )

    async def create_signed_order(self, token_id, side, contracts, max_price, **kwargs):
        self.created = True
        return "poly-1"

    async def close_position(self, token_id, side, contracts, min_price, **kwargs):
        self.closed = True
        return "poly-close-1"

    async def wait_filled(self, order_id, timeout_ms):
        return self.fill_result

    async def cancel_order(self, order_id):
        self.cancelled = True

    async def get_usdc_balance(self):
        return 100


class FakeCefi:
    def __init__(self) -> None:
        self.hedged = False
        self.closed = False
        self.leverage_set = False

    async def watch_order_book(self, symbol: str):
        return OrderBook(
            bids=[OrderBookLevel(75000, 1)],
            asks=[OrderBookLevel(75100, 1)],
        )

    async def create_market_order(self, symbol, side, quantity):
        self.hedged = True
        return "hedge-1"

    async def set_leverage(self, symbol, leverage):
        self.leverage_set = True

    async def close_market_order(self, symbol, entry_side, quantity):
        self.closed = True
        return "hedge-close-1"

    async def get_usdt_balance(self):
        return 100


class FailingCefi(FakeCefi):
    async def create_market_order(self, symbol, side, quantity):
        raise RuntimeError("hedge failed")


class FakeTelegram:
    def __init__(self) -> None:
        self.messages = 0

    async def send_html(self, message: str) -> None:
        self.messages += 1

    async def send_signal(self, signal, is_test, min_net_spread):
        self.messages += 1


def make_config(is_test: bool) -> AppConfig:
    return AppConfig(
        is_test=is_test,
        max_order_size_usd=100,
        min_net_spread=0.05,
        cefi_taker_fee=0.0005,
        cefi_leverage=10,
        poll_interval_ms=250,
        polymarket_fill_timeout_ms=300,
        telegram=TelegramConfig(None, None),
        binance=BinanceConfig(None, None),
        polymarket=PolymarketConfig(None, "https://clob.polymarket.com", 137, 0, None),
        auto_close=AutoCloseConfig(True, 0.10, 3600),
        markets=[],
    )


def make_signal(net_spread: float) -> ArbitrageSignal:
    return ArbitrageSignal(
        market=MarketSpec("BTC-USD", ">$75,000", "token", PolymarketSide.YES, "BTC/USDT:USDT", HedgeSide.SHORT),
        plan=PositionPlan(238, 99.96, 0.0013, 100, 10),
        metrics=SpreadMetrics(0.0952, net_spread, 9.15, 0, 0),
        polymarket_price=0.42,
        cefi_price=75120,
    )


class ExecutionTests(unittest.IsolatedAsyncioTestCase):
    async def test_dry_run_sends_telegram_without_orders(self) -> None:
        poly = FakePolymarket()
        cefi = FakeCefi()
        telegram = FakeTelegram()
        router = ExecutionRouter(make_config(True), poly, cefi, telegram)  # type: ignore[arg-type]

        await router.handle_signal(make_signal(0.0543))

        self.assertFalse(poly.created)
        self.assertFalse(cefi.hedged)
        self.assertEqual(telegram.messages, 1)

    async def test_production_cancels_unfilled_poly_order_without_hedge(self) -> None:
        poly = FakePolymarket()
        cefi = FakeCefi()
        telegram = FakeTelegram()
        router = ExecutionRouter(make_config(False), poly, cefi, telegram)  # type: ignore[arg-type]

        await router.handle_signal(make_signal(0.0543))

        self.assertTrue(poly.created)
        self.assertTrue(poly.cancelled)
        self.assertFalse(cefi.hedged)

    async def test_production_unwinds_polymarket_when_hedge_fails_after_fill(self) -> None:
        poly = FakePolymarket()
        poly.fill_result = True
        cefi = FailingCefi()
        telegram = FakeTelegram()
        router = ExecutionRouter(make_config(False), poly, cefi, telegram)  # type: ignore[arg-type]

        with self.assertRaisesRegex(RuntimeError, "hedge failed"):
            await router.handle_signal(make_signal(0.0543))

        self.assertTrue(poly.created)
        self.assertTrue(poly.closed)
        self.assertTrue(cefi.leverage_set)
        self.assertEqual(telegram.messages, 1)

    async def test_production_exit_waits_for_poly_close_before_hedge_close(self) -> None:
        poly = FakePolymarket()
        poly.fill_result = False
        cefi = FakeCefi()
        telegram = FakeTelegram()
        ledger = PositionLedger()
        market = MarketSpec(
            "BTC-USD",
            ">$75,000",
            "token",
            PolymarketSide.YES,
            "BTC/USDT:USDT",
            HedgeSide.SHORT,
            datetime.now(timezone.utc) + timedelta(minutes=30),
            "condition",
            "0.01",
            False,
        )
        ledger.add(
            OpenPosition(
                market=market,
                polymarket_contracts=200,
                polymarket_entry_price=0.40,
                cefi_quantity=0.0013,
                cefi_entry_side=HedgeSide.SHORT,
                opened_at=datetime.now(timezone.utc),
                polymarket_order_id="poly-entry-1",
                cefi_order_id="hedge-entry-1",
            )
        )
        config = make_config(False)
        router = ExecutionRouter(config, poly, cefi, telegram, ledger)  # type: ignore[arg-type]
        engine = ArbitrageEngine(config, poly, cefi, router)  # type: ignore[arg-type]

        await engine.run_once()

        self.assertTrue(poly.closed)
        self.assertTrue(poly.cancelled)
        self.assertFalse(cefi.closed)

    async def test_auto_close_dry_run_sends_exit_message_without_orders(self) -> None:
        poly = FakePolymarket()
        cefi = FakeCefi()
        telegram = FakeTelegram()
        ledger = PositionLedger()
        expires_at = datetime.now(timezone.utc) + timedelta(minutes=30)
        market = MarketSpec(
            "BTC-USD",
            ">$75,000",
            "token",
            PolymarketSide.YES,
            "BTC/USDT:USDT",
            HedgeSide.SHORT,
            expires_at,
        )
        ledger.add(
            OpenPosition(
                market=market,
                polymarket_contracts=200,
                polymarket_entry_price=0.40,
                cefi_quantity=0.0013,
                cefi_entry_side=HedgeSide.SHORT,
                opened_at=datetime.now(timezone.utc),
                polymarket_order_id="poly-entry-1",
                cefi_order_id="hedge-entry-1",
            )
        )
        config = make_config(True)
        router = ExecutionRouter(config, poly, cefi, telegram, ledger)  # type: ignore[arg-type]
        engine = ArbitrageEngine(config, poly, cefi, router)  # type: ignore[arg-type]

        await engine.run_once()

        self.assertEqual(telegram.messages, 1)
        self.assertFalse(poly.closed)
        self.assertFalse(cefi.closed)
        self.assertEqual(ledger.all(), [])


if __name__ == "__main__":
    unittest.main()
