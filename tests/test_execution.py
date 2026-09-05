import asyncio
import tempfile
import time
import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, patch

from arbitrage_engine.chain_cost import LiveChainCostEstimator, LiveChainCostUnavailable, RouteChainCostQuote
from arbitrage_engine.config import (
    AppConfig,
    AutoCloseConfig,
    MyriadMarketsConfig,
    PolymarketConfig,
    PredictFunConfig,
    SxBetConfig,
    TelegramConfig,
    Web3NetworkConfig,
)
from arbitrage_engine.connectors.base import (
    BinaryMarketClient,
    OrderBookUnavailableException,
    OrderResidualExposure,
    OrderSubmissionRejected,
)
from arbitrage_engine.connectors.myriad import MyriadClient
from arbitrage_engine.discovery_lifecycle import ActiveMarketRegistry, DiscoveryResult
from arbitrage_engine.engine import ArbitrageEngine
from arbitrage_engine.execution import (
    EntrySubmissionCoordinator,
    ExecutionRouter,
    _entry_submission_window_open,
    _intent_status_from_report,
    _safe_preview_blockers,
    _signal_key,
)
from arbitrage_engine.models import (
    AmmPool,
    ArbitrageSignal,
    BinarySide,
    ExecutionMode,
    ExecutionReport,
    ExecutionStatus,
    ExitSignal,
    MappingStatus,
    MarketConstraints,
    MarketSpec,
    OpenPosition,
    OrderBook,
    OrderBookLevel,
    OrderIntentStatus,
    OrderPreview,
    PositionPlan,
    SpreadMetrics,
    VenueFeeQuote,
)
from arbitrage_engine.position_manager import PositionManager
from arbitrage_engine.positions import PositionLedger
from arbitrage_engine.risk import GlobalRiskController
from arbitrage_engine.telegram import TelegramNotifier


def _open_position(**kwargs: Any) -> OpenPosition:
    for name in (
        "polymarket_contracts",
        "polymarket_entry_price",
        "predict_fun_contracts",
        "predict_fun_entry_price",
    ):
        kwargs[name] = Decimal(str(kwargs[name]))
    return OpenPosition(**kwargs)


class FakeBinaryClient(BinaryMarketClient):
    def __init__(self) -> None:
        self.bought = False
        self.sold = False
        self.cancelled = False
        self.fill_result = False
        self.fill_results: list[bool] = []
        self.partial_fill_results: list[float] = []
        self.order_amounts: dict[str, float] = {}
        self.order_prices: dict[str, float] = {}
        self.fill_price_override: float | None = None
        self.sell_contracts: list[float] = []
        self.sell_calls = 0
        self.buy_tokens: list[str] = []
        self.preview_requests: list[tuple[Decimal, Decimal]] = []
        self.preview_signing_inputs: list[tuple[str | None, bool | None]] = []
        self.buy_signing_inputs: list[tuple[str | None, bool | None]] = []
        self.sell_tokens: list[str] = []
        self.watch_tokens: list[str] = []
        self.bid = 0.55
        self.ask = 0.42
        self.cash_balance = 100.0
        self.book_timestamp = time.time()
        self.market_data_age: float | None = None
        self.stream_connected: bool | None = None
        self.execution_fresh_override: bool | None = None
        self.reconnect_calls = 0
        self.synced_targets: list[set[str]] = []
        self.primed_targets: list[set[str]] = []
        self.constraints_tick_size = Decimal("0.01")

    async def watch_order_book(self, token_id: str) -> OrderBook:
        self.watch_tokens.append(token_id)
        return OrderBook(
            bids=[OrderBookLevel(self.bid, 1000)],
            asks=[OrderBookLevel(self.ask, 1000)],
            timestamp=self.book_timestamp,
        )

    def is_order_book_execution_fresh(
        self,
        token_id: str,
        book: OrderBook,
        max_age_seconds: float,
    ) -> bool:
        if self.execution_fresh_override is not None:
            return self.execution_fresh_override
        return super().is_order_book_execution_fresh(token_id, book, max_age_seconds)

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
        del side, condition_id
        self.buy_signing_inputs.append((tick_size, neg_risk))
        self.bought = True
        self.buy_tokens.append(token_id)
        order_id = f"buy-{token_id}"
        self.order_amounts[order_id] = contracts
        self.order_prices[order_id] = max_price
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
        del side, condition_id, tick_size, neg_risk
        self.sold = True
        self.sell_calls += 1
        self.sell_contracts.append(contracts)
        self.sell_tokens.append(token_id)
        order_id = f"sell-{token_id}"
        self.order_amounts[order_id] = contracts
        self.order_prices[order_id] = min_price
        return order_id

    async def wait_filled(self, order_id: str, timeout_ms: int) -> ExecutionReport:
        del timeout_ms
        requested = self.order_amounts.get(order_id, 0.0)
        if self.partial_fill_results:
            amount_filled = self.partial_fill_results.pop(0)
            average_price = self.fill_price_override
            if average_price is None:
                average_price = self.order_prices.get(order_id, 0.0)
            return ExecutionReport.from_amounts(order_id, requested, amount_filled, "partial", average_price)
        if self.fill_results:
            filled = self.fill_results.pop(0)
        else:
            filled = self.fill_result
        average_price = self.fill_price_override
        if average_price is None:
            average_price = self.order_prices.get(order_id, 0.0)
        return ExecutionReport.from_amounts(
            order_id,
            requested,
            requested if filled else 0.0,
            "filled" if filled else "pending",
            average_price if filled else 0.0,
        )

    async def cancel_order(self, order_id: str) -> None:
        del order_id
        self.cancelled = True

    async def get_cash_balance(self) -> float:
        return self.cash_balance

    async def get_market_constraints(self, token_id: str, condition_id: str | None = None) -> MarketConstraints | None:
        del token_id, condition_id
        return MarketConstraints(
            tick_size=self.constraints_tick_size,
            lot_size=Decimal("0.000001"),
            minimum_notional=Decimal("0.01"),
            fee_rate_bps=0,
        )

    async def _preview_buy_signature(
        self,
        token_id: str,
        side: BinarySide,
        contracts: Decimal,
        max_price: Decimal,
        *,
        condition_id: str | None,
        tick_size: str | None,
        neg_risk: bool | None,
    ) -> str | None:
        del token_id, side, condition_id
        self.preview_requests.append((contracts, max_price))
        self.preview_signing_inputs.append((tick_size, neg_risk))
        return "test-signed-preview"

    def market_data_age_seconds(self) -> float | None:
        return self.market_data_age

    def sync_market_data_targets(self, token_ids: set[str]) -> None:
        self.synced_targets.append(set(token_ids))

    async def prime_market_data_targets(self) -> None:
        self.primed_targets.append(set(self.synced_targets[-1]) if self.synced_targets else set())

    async def reconnect_market_data(self) -> None:
        self.reconnect_calls += 1

    def telemetry_snapshot(self) -> dict[str, float]:
        if self.stream_connected is None:
            return {}
        return {
            "connected": float(self.stream_connected),
            "reconnecting": float(not self.stream_connected),
        }

    def has_active_market_data_targets(self) -> bool:
        return bool(self.market_data_age is not None or any(self.synced_targets))


class CountingPreviewClient(FakeBinaryClient):
    def __init__(self, *, fail_on_signature_call: int | None = None) -> None:
        super().__init__()
        self.preview_signature_calls = 0
        self.fail_on_signature_call = fail_on_signature_call

    async def _preview_buy_signature(
        self,
        token_id: str,
        side: BinarySide,
        contracts: Decimal,
        max_price: Decimal,
        *,
        condition_id: str | None,
        tick_size: str | None,
        neg_risk: bool | None,
    ) -> str | None:
        del token_id, side, contracts, max_price, condition_id, tick_size, neg_risk
        self.preview_signature_calls += 1
        if self.preview_signature_calls == self.fail_on_signature_call:
            return None
        return "test-signed-preview"


class RefreshingMarketDataClient(CountingPreviewClient):
    async def watch_order_book(self, token_id: str) -> OrderBook:
        self.market_data_age = 0.0
        self.book_timestamp = time.time()
        return await super().watch_order_book(token_id)


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


class UnavailableBookClient(FakeBinaryClient):
    async def watch_order_book(self, token_id: str) -> OrderBook:
        del token_id
        raise OrderBookUnavailableException("no taker liquidity")


class StubChainCostEstimator:
    def __init__(self, result: RouteChainCostQuote | Exception) -> None:
        self.result = result
        self.calls: list[tuple[str, bool]] = []
        self.closed = False

    async def estimate(self, route: str, *, require_live: bool) -> RouteChainCostQuote:
        self.calls.append((route, require_live))
        if isinstance(self.result, Exception):
            raise self.result
        return self.result

    async def close(self) -> None:
        self.closed = True


def _zero_chain_cost_estimator() -> LiveChainCostEstimator:
    return cast(
        LiveChainCostEstimator,
        StubChainCostEstimator(
            RouteChainCostQuote(
                route="polymarket_predict",
                configured_floor_usd=Decimal(0),
                live_estimate_usd=Decimal(0),
                reserved_cost_usd=Decimal(0),
                multiplier=Decimal(1),
                live=False,
                components=(),
            )
        ),
    )


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


class SlowTelegram(FakeTelegram):
    async def send_html(self, message: str) -> None:
        del message
        await asyncio.sleep(0.05)
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
        sx_bet_fill_timeout_ms=4000,
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
        sx_bet=SxBetConfig(
            enabled=False,
            api_base_url="https://api.sx.bet",
            api_key=None,
            private_key=None,
            rpc_url="https://rpc-rollup.sx.technology",
            rpc_urls=["https://rpc-rollup.sx.technology"],
            chain_id=4162,
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


def make_verified_market(expires_at: datetime | None = None) -> MarketSpec:
    return replace(
        make_market(expires_at),
        mapping_status=MappingStatus.VERIFIED,
        verified_routes=frozenset({"polymarket_predict"}),
        rules_fingerprint="rules",
        resolution_source="https://example.test/rules",
        outcome_semantics="YES and NO are complementary",
        category="crypto",
    )


def make_signal(net_spread: float = 0.11) -> ArbitrageSignal:
    return ArbitrageSignal(
        market=make_market(),
        plan=PositionPlan(*(Decimal(value) for value in ("100", "42", "100", "47", "100", "89"))),
        metrics=SpreadMetrics(0.11, net_spread, 11, 0, 0, 0.89),
        polymarket_price=0.42,
        predict_fun_price=0.47,
    )


def make_verified_signal(net_spread: float = 0.11) -> ArbitrageSignal:
    return replace(make_signal(net_spread), market=make_verified_market())


class EntrySubmissionCutoffTests(unittest.TestCase):
    def test_sx_requires_a_cutoff_and_safety_buffer(self) -> None:
        now = datetime(2026, 8, 20, 12, tzinfo=UTC)

        self.assertFalse(_entry_submission_window_open(make_market(), includes_sx=True, now=now))
        self.assertFalse(
            _entry_submission_window_open(
                make_market(now + timedelta(seconds=25)),
                includes_sx=True,
                now=now,
            )
        )
        self.assertTrue(
            _entry_submission_window_open(
                make_market(now + timedelta(seconds=26)),
                includes_sx=True,
                now=now,
            )
        )

    def test_earliest_market_cutoff_wins(self) -> None:
        now = datetime(2026, 8, 20, 12, tzinfo=UTC)
        market = replace(
            make_market(now + timedelta(hours=1)),
            cutoff_at=now + timedelta(seconds=10),
        )

        self.assertFalse(_entry_submission_window_open(market, includes_sx=True, now=now))
        self.assertTrue(_entry_submission_window_open(make_market(), includes_sx=False, now=now))


class ExecutionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._canary_deadline_patch = patch.dict(
            "os.environ",
            {"FUNDED_CANARY_DEADLINE_UNIX": str(time.time() + 86_400)},
        )
        self._canary_deadline_patch.start()
        self.addCleanup(self._canary_deadline_patch.stop)

    async def test_discovery_only_route_cannot_submit_in_canary(self) -> None:
        first = FakeBinaryClient()
        second = FakeBinaryClient()
        config = make_config(False)
        funded_routes = replace(
            config.routes,
            polymarket_myriad=True,
            polymarket_predict=False,
            predict_myriad=False,
            predict_sx=False,
            polymarket_sx=False,
            sx_myriad=False,
        )
        config = replace(
            config,
            execution_mode=ExecutionMode.CANARY,
            funded_routes=funded_routes,
        )
        router = ExecutionRouter(config, first, second, FakeTelegram())

        await router.handle_signal(make_verified_signal())

        self.assertFalse(first.bought)
        self.assertFalse(second.bought)
        self.assertEqual(first.preview_requests, [])
        self.assertEqual(second.preview_requests, [])

    async def test_funded_canary_deadline_blocks_new_entry_and_pauses(self) -> None:
        config = replace(
            make_config(False),
            execution_mode=ExecutionMode.CANARY,
            live_trading_confirmed=True,
            _execution_mode_explicit=True,
        )
        first = FakeBinaryClient()
        second = FakeBinaryClient()
        with patch.dict("os.environ", {"FUNDED_CANARY_DEADLINE_UNIX": str(time.time() - 1)}):
            router = ExecutionRouter(config, first, second, FakeTelegram())
            await router.handle_signal(make_signal())

        self.assertTrue(router.is_paused)
        self.assertEqual(router._risk.pause_reason, "funded_canary_window_complete")  # noqa: SLF001
        self.assertFalse(first.bought)
        self.assertFalse(second.bought)

    async def test_funded_canary_uses_runtime_updated_shared_deadline_file(self) -> None:
        config = replace(
            make_config(False),
            execution_mode=ExecutionMode.CANARY,
            live_trading_confirmed=True,
            _execution_mode_explicit=True,
        )
        first = FakeBinaryClient()
        second = FakeBinaryClient()
        with tempfile.TemporaryDirectory() as temporary_directory:
            deadline_path = Path(temporary_directory) / "deadline"
            deadline_path.write_text(str(time.time() + 60), encoding="utf-8")
            with patch.dict(
                "os.environ",
                {
                    "FUNDED_CANARY_DEADLINE_FILE": str(deadline_path),
                    "FUNDED_CANARY_DEADLINE_UNIX": str(time.time() + 3600),
                },
            ):
                router = ExecutionRouter(config, first, second, FakeTelegram())
                deadline_path.write_text(str(time.time() - 1), encoding="utf-8")
                await router.handle_signal(make_signal())

        self.assertTrue(router.is_paused)
        self.assertEqual(router._risk.pause_reason, "funded_canary_window_complete")  # noqa: SLF001
        self.assertFalse(first.bought)
        self.assertFalse(second.bought)

    async def test_funded_canary_without_deadline_source_fails_closed(self) -> None:
        config = replace(
            make_config(False),
            execution_mode=ExecutionMode.CANARY,
            live_trading_confirmed=True,
            _execution_mode_explicit=True,
        )
        first = FakeBinaryClient()
        second = FakeBinaryClient()
        with patch.dict("os.environ", {}, clear=True):
            router = ExecutionRouter(config, first, second, FakeTelegram())
            await router.handle_signal(make_signal())

        self.assertTrue(router.is_paused)
        self.assertEqual(router._risk.pause_reason, "funded_canary_window_complete")  # noqa: SLF001
        self.assertFalse(first.bought)
        self.assertFalse(second.bought)

    async def test_submitted_entry_exactly_matches_signed_preview_and_25_usd_leg_cap(self) -> None:
        class CapturingClient(FakeBinaryClient):
            def __init__(self) -> None:
                super().__init__()
                self.prepared_fingerprints: list[str | None] = []

            async def buy_with_order_id_persistence(self, *args: Any, **kwargs: Any) -> str:
                self.prepared_fingerprints.append(kwargs.get("prepared_order_fingerprint"))
                return await super().buy_with_order_id_persistence(*args, **kwargs)

        first = CapturingClient()
        second = CapturingClient()
        first.constraints_tick_size = Decimal("0.001")
        second.constraints_tick_size = Decimal("0.005")
        config = replace(
            make_config(False),
            position_size_usd=50,
            max_order_size_usd=50,
            max_total_notional_usd=52,
            max_venue_exposure_usd=25,
            max_market_exposure_usd=52,
            max_open_positions=1,
        )
        signal = replace(
            make_verified_signal(),
            market=replace(
                make_verified_market(),
                tick_size="0.01",
                neg_risk=True,
                predict_fun_neg_risk=True,
            ),
            plan=PositionPlan(
                polymarket_contracts=Decimal("50"),
                polymarket_capital_usd=Decimal("21"),
                predict_fun_contracts=Decimal("50"),
                predict_fun_capital_usd=Decimal("21"),
                payout_contracts=Decimal("50"),
                total_cost_usd=Decimal("42"),
            ),
        )
        router = ExecutionRouter(config, first, second, FakeTelegram())

        await router.handle_signal(signal)

        first_contracts, first_limit = first.preview_requests[-1]
        second_contracts, second_limit = second.preview_requests[-1]
        self.assertEqual(first.order_amounts["buy-poly-token"], float(first_contracts))
        self.assertEqual(first.order_prices["buy-poly-token"], float(first_limit))
        self.assertEqual(second.order_amounts["buy-predict-token"], float(second_contracts))
        self.assertEqual(second.order_prices["buy-predict-token"], float(second_limit))
        self.assertEqual(first.preview_signing_inputs[-1], ("0.001", True))
        self.assertEqual(first.buy_signing_inputs[-1], first.preview_signing_inputs[-1])
        self.assertEqual(second.preview_signing_inputs[-1], (None, True))
        self.assertEqual(second.buy_signing_inputs[-1], second.preview_signing_inputs[-1])
        self.assertEqual(first.prepared_fingerprints, ["test-signed-preview"])
        self.assertEqual(second.prepared_fingerprints, ["test-signed-preview"])
        self.assertEqual(first_contracts, second_contracts)
        self.assertLessEqual(first_contracts * first_limit, Decimal("25"))
        self.assertLessEqual(second_contracts * second_limit, Decimal("25"))

    async def test_both_prepared_legs_are_claimed_before_any_submission_starts(self) -> None:
        class ClaimingClient(FakeBinaryClient):
            def __init__(self, *, reject_claim: bool = False) -> None:
                super().__init__()
                self.reject_claim = reject_claim
                self.claimed: list[str | None] = []
                self.released: list[str | None] = []

            def claim_prepared_order(self, fingerprint: str | None, **kwargs: Any) -> str | None:
                del kwargs
                self.claimed.append(fingerprint)
                if self.reject_claim:
                    raise OrderSubmissionRejected("prepared preview expired")
                return fingerprint

            def release_prepared_order(self, fingerprint: str | None) -> None:
                self.released.append(fingerprint)

        first = ClaimingClient()
        second = ClaimingClient(reject_claim=True)
        router = ExecutionRouter(make_config(False), first, second, FakeTelegram())

        with self.assertRaisesRegex(OrderSubmissionRejected, "preview expired"):
            await router._execute_production(  # noqa: SLF001
                make_signal(),
                first_order_fingerprint="first-preview",
                second_order_fingerprint="second-preview",
            )

        self.assertFalse(first.bought)
        self.assertFalse(second.bought)
        self.assertEqual(first.released, ["first-preview"])
        self.assertEqual(router.ledger.all(), [])

    def test_canary_risk_allows_fee_bearing_25_usd_legs_with_bounded_buffer(self) -> None:
        config = replace(
            make_config(False),
            position_size_usd=50,
            max_order_size_usd=50,
            max_total_notional_usd=52,
            max_venue_exposure_usd=25,
            max_market_exposure_usd=52,
            max_open_positions=1,
        )
        router = ExecutionRouter(config, FakeBinaryClient(), FakeBinaryClient(), FakeTelegram())
        signal = replace(
            make_verified_signal(),
            plan=PositionPlan(
                polymarket_contracts=Decimal("50"),
                polymarket_capital_usd=Decimal("25"),
                predict_fun_contracts=Decimal("50"),
                predict_fun_capital_usd=Decimal("25"),
                payout_contracts=Decimal("50"),
                total_cost_usd=Decimal("51.25"),
                polymarket_fee_usd=Decimal("0.75"),
                predict_fun_fee_usd=Decimal("0.50"),
            ),
            metrics=replace(make_verified_signal().metrics, fixed_chain_cost_usd=0.25),
        )

        self.assertTrue(router._risk_limits_allow(signal))  # noqa: SLF001
        over_limit = replace(signal, plan=replace(signal.plan, total_cost_usd=Decimal("51.76")))
        self.assertFalse(router._risk_limits_allow(over_limit))  # noqa: SLF001
        invalid_chain_cost = replace(signal, metrics=replace(signal.metrics, fixed_chain_cost_usd=-0.01))
        self.assertFalse(router._risk_limits_allow(invalid_chain_cost))  # noqa: SLF001
        self.assertTrue(  # noqa: SLF001
            router._risk_limits_allow(signal, all_in_cost_usd=Decimal("52.00"))
        )
        self.assertFalse(  # noqa: SLF001
            router._risk_limits_allow(signal, all_in_cost_usd=Decimal("52.01"))
        )

    async def test_entry_ledger_keeps_raw_fill_prices_when_fees_apply(self) -> None:
        first = FakeBinaryClient()
        second = FakeBinaryClient()
        first.fill_result = True
        second.fill_result = True
        first.fill_price_override = 0.415
        second.fill_price_override = 0.465
        config = make_config(False)
        config = replace(
            config,
            polymarket=replace(config.polymarket, trading_fee_pct=0.01),
            predict_fun=replace(config.predict_fun, fee_rate_bps=100),
        )
        router = ExecutionRouter(config, first, second, FakeTelegram())

        await router.handle_signal(make_signal())

        position = router.ledger.all()[0]
        self.assertEqual(position.polymarket_entry_price, Decimal("0.415"))
        self.assertEqual(position.predict_fun_entry_price, Decimal("0.465"))

    async def test_filled_report_without_average_price_pauses_and_stays_pending(self) -> None:
        first = FakeBinaryClient()
        second = FakeBinaryClient()
        first.fill_result = True
        second.fill_result = True
        first.fill_price_override = 0.0
        router = ExecutionRouter(make_config(False), first, second, FakeTelegram())

        await router.handle_signal(make_signal())

        position = router.ledger.all()[0]
        self.assertTrue(router.is_paused)
        self.assertEqual(position.status, "entry_pending")
        self.assertEqual(position.polymarket_entry_price, 0.0)
        self.assertEqual(
            position.predict_fun_entry_price,
            Decimal(str(second.order_prices["buy-predict-token"])),
        )

    async def test_initial_entry_pending_has_no_synthetic_execution_prices(self) -> None:
        router = ExecutionRouter(make_config(False), FakeBinaryClient(), FakeBinaryClient(), FakeTelegram())

        await router._save_entry_pending(make_signal())

        position = router.ledger.all()[0]
        self.assertEqual(position.status, "entry_pending")
        self.assertEqual(position.polymarket_entry_price, 0.0)
        self.assertEqual(position.predict_fun_entry_price, 0.0)

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

    async def test_preflight_accepts_connector_confirmed_quiet_book(self) -> None:
        first = FakeBinaryClient()
        second = FakeBinaryClient()
        first.book_timestamp = time.time() - 5
        first.execution_fresh_override = True
        first.fill_result = True
        second.fill_result = True
        router = ExecutionRouter(
            replace(make_config(False), max_orderbook_age_seconds=2.0),
            first,
            second,
            FakeTelegram(),
        )

        await router.handle_signal(make_signal())

        self.assertTrue(first.bought)
        self.assertTrue(second.bought)

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

    async def test_shadow_scan_accepts_connector_confirmed_quiet_book(self) -> None:
        first = FakeBinaryClient()
        second = FakeBinaryClient()
        first.book_timestamp = time.time() - 5
        first.execution_fresh_override = True
        observed: list[tuple[str, str, float | None]] = []
        config = replace(make_config(True), markets=[make_market()], max_orderbook_age_seconds=2.0)
        router = ExecutionRouter(config, first, second, FakeTelegram())
        engine = ArbitrageEngine(
            config,
            first,
            second,
            router,
            signal_evaluation_observer=lambda route, outcome, net_spread: observed.append((route, outcome, net_spread)),
        )

        await engine.run_once()

        self.assertTrue(observed)
        self.assertNotEqual(observed[0][1], "stale_book")

    async def test_engine_reports_below_threshold_evaluation_by_route(self) -> None:
        first = FakeBinaryClient()
        second = FakeBinaryClient()
        first.ask = 0.55
        second.ask = 0.55
        observed: list[tuple[str, str, float | None]] = []
        calibration: list[tuple[str, float | None]] = []
        config = replace(make_config(True), markets=[make_verified_market()])
        router = ExecutionRouter(config, first, second, FakeTelegram())
        engine = ArbitrageEngine(
            config,
            first,
            second,
            router,
            signal_evaluation_observer=lambda route, outcome, net_spread: observed.append((route, outcome, net_spread)),
            calibration_observer=lambda route, adverse_move: calibration.append((route, adverse_move)),
        )

        await engine.run_once()

        self.assertEqual(observed, [("polymarket_predict", "below_min_net_spread", -0.1)])
        self.assertEqual(calibration, [("polymarket_predict", None)])
        self.assertFalse(first.bought)
        self.assertFalse(second.bought)

    async def test_engine_scan_uses_live_reserved_chain_cost_in_route_economics(self) -> None:
        first = FakeBinaryClient()
        second = FakeBinaryClient()
        first.ask = 0.42
        second.ask = 0.42
        observed: list[tuple[str, str, float | None]] = []
        economics: list[dict[str, float]] = []
        config = replace(make_config(True), markets=[make_verified_market()])
        router = ExecutionRouter(config, first, second, FakeTelegram())
        chain_cost = StubChainCostEstimator(
            RouteChainCostQuote(
                route="polymarket_predict",
                configured_floor_usd=Decimal("0.25"),
                live_estimate_usd=Decimal("20"),
                reserved_cost_usd=Decimal("20"),
                multiplier=Decimal("1.5"),
                live=True,
                components=(),
            )
        )
        engine = ArbitrageEngine(
            config,
            first,
            second,
            router,
            signal_evaluation_observer=lambda route, outcome, net_spread: observed.append((route, outcome, net_spread)),
            market_economics_observer=lambda route, values: economics.append(values),
            chain_cost_estimator=cast(LiveChainCostEstimator, chain_cost),
        )

        await engine.run_once()

        self.assertEqual(chain_cost.calls, [("polymarket_predict", False)])
        self.assertEqual(observed[0][0:2], ("polymarket_predict", "below_min_net_spread"))
        self.assertLess(observed[0][2] or 0.0, 0.0)
        self.assertEqual(economics, [{"chain_cost_usd": 20.0}])
        self.assertFalse(first.bought)
        self.assertFalse(second.bought)

    async def test_engine_scan_fails_closed_when_required_live_chain_cost_is_unavailable(self) -> None:
        first = FakeBinaryClient()
        second = FakeBinaryClient()
        observed: list[tuple[str, str, float | None]] = []
        config = make_config(True)
        config = replace(
            config,
            markets=[make_verified_market()],
            spread_policy=replace(config.spread_policy, require_live_gas_estimate=True),
        )
        router = ExecutionRouter(config, first, second, FakeTelegram())
        chain_cost = StubChainCostEstimator(LiveChainCostUnavailable("rpc unavailable"))
        engine = ArbitrageEngine(
            config,
            first,
            second,
            router,
            signal_evaluation_observer=lambda route, outcome, net_spread: observed.append((route, outcome, net_spread)),
            chain_cost_estimator=cast(LiveChainCostEstimator, chain_cost),
        )

        await engine.run_once()

        self.assertEqual(chain_cost.calls, [("polymarket_predict", True)])
        self.assertEqual(observed, [("polymarket_predict", "chain_cost_unavailable", None)])
        self.assertEqual(first.watch_tokens, [])
        self.assertEqual(second.watch_tokens, [])

    async def test_shadow_engine_keeps_evaluating_while_risk_is_paused(self) -> None:
        first = FakeBinaryClient()
        second = FakeBinaryClient()
        first.ask = 0.55
        second.ask = 0.55
        observed: list[tuple[str, str, float | None]] = []
        config = replace(make_config(True), markets=[make_verified_market()])
        router = ExecutionRouter(config, first, second, FakeTelegram())
        await router._risk.pause("shadow opportunity monitor")  # noqa: SLF001
        engine = ArbitrageEngine(
            config,
            first,
            second,
            router,
            signal_evaluation_observer=lambda route, outcome, net_spread: observed.append((route, outcome, net_spread)),
        )

        await engine.run_once()

        self.assertEqual(observed, [("polymarket_predict", "below_min_net_spread", -0.1)])
        self.assertFalse(first.bought)
        self.assertFalse(second.bought)
        self.assertEqual(first.synced_targets[-1], {"poly-token"})
        self.assertEqual(second.synced_targets[-1], {"predict-token"})

    async def test_paused_shadow_subscribes_only_to_verified_market_targets(self) -> None:
        first = FakeBinaryClient()
        second = FakeBinaryClient()
        verified = make_verified_market()
        unverified = replace(
            make_market(),
            polymarket_token_id="unverified-poly-token",
            predict_fun_token_id="unverified-predict-token",
        )
        config = replace(make_config(True), markets=[verified, unverified])
        router = ExecutionRouter(config, first, second, FakeTelegram())
        await router._risk.pause("shadow opportunity monitor")  # noqa: SLF001
        engine = ArbitrageEngine(config, first, second, router)

        engine._sync_market_data_targets()  # noqa: SLF001
        await engine.run_once()

        self.assertEqual(first.synced_targets[-1], {"poly-token"})
        self.assertEqual(second.synced_targets[-1], {"predict-token"})
        # A selected shadow candidate is read again by the signed preflight;
        # assert the selected universe rather than coupling this test to the
        # number of safety reads for that same token.
        self.assertEqual(set(first.watch_tokens), {"poly-token"})
        self.assertEqual(set(second.watch_tokens), {"predict-token"})

    async def test_production_shadow_subscribes_only_to_verified_market_targets_without_pause(self) -> None:
        first = FakeBinaryClient()
        second = FakeBinaryClient()
        verified = make_verified_market()
        unverified = replace(
            make_market(),
            polymarket_token_id="unverified-poly-token",
            predict_fun_token_id="unverified-predict-token",
        )
        config = replace(
            make_config(True),
            markets=[verified, unverified],
            shadow_require_verified_mappings=True,
            execution_mode=ExecutionMode.SHADOW,
            _execution_mode_explicit=True,
        )
        router = ExecutionRouter(config, first, second, FakeTelegram())
        engine = ArbitrageEngine(config, first, second, router)

        engine._sync_market_data_targets()  # noqa: SLF001
        await engine.run_once()

        self.assertEqual(first.synced_targets[-1], {"poly-token"})
        self.assertEqual(second.synced_targets[-1], {"predict-token"})
        self.assertEqual(set(first.watch_tokens), {"poly-token"})
        self.assertEqual(set(second.watch_tokens), {"predict-token"})

    async def test_engine_exposes_exact_selected_targets_for_funded_routes(self) -> None:
        first = FakeBinaryClient()
        second = FakeBinaryClient()
        base = make_config(True)
        routes = replace(
            base.routes,
            polymarket_myriad=False,
            polymarket_predict=True,
            predict_myriad=False,
        )
        config = replace(
            base,
            routes=routes,
            funded_routes=routes,
            markets=[make_verified_market()],
        )
        engine = ArbitrageEngine(
            config,
            first,
            second,
            ExecutionRouter(config, first, second, FakeTelegram()),
        )

        self.assertEqual(engine.funded_market_data_targets(), {"polymarket_predict": ()})

        await engine.run_once()

        self.assertEqual(
            engine.funded_market_data_targets(),
            {
                "polymarket_predict": (
                    ("Polymarket", "poly-token"),
                    ("Predict.fun", "predict-token"),
                )
            },
        )

    async def test_canary_stale_funded_target_blocks_all_service_entries_after_bootstrap(self) -> None:
        poly = CountingPreviewClient()
        predict = CountingPreviewClient()
        myriad = CountingPreviewClient()
        poly.market_data_age = 0.1
        predict.market_data_age = 0.1
        myriad.market_data_age = 12.0
        base = make_config(True)
        routes = replace(
            base.routes,
            polymarket_myriad=True,
            polymarket_predict=True,
            predict_myriad=False,
        )
        predict_market = make_verified_market()
        myriad_market = replace(
            make_verified_market(),
            symbol="myriad-market",
            polymarket_token_id="myriad-poly-token",
            predict_fun_token_id="",
            myriad_market_id="myriad-token",
            myriad_side=BinarySide.NO,
            venue_b_label="Myriad",
            verified_routes=frozenset({"polymarket_myriad"}),
        )
        config = replace(
            base,
            execution_mode=ExecutionMode.CANARY,
            _execution_mode_explicit=True,
            routes=routes,
            funded_routes=routes,
            markets=[predict_market, myriad_market],
            myriad_markets=replace(base.myriad_markets, enabled=True),
        )
        balances: dict[str, Decimal | float] = {
            "Polymarket": 1_000.0,
            "Predict.fun": 1_000.0,
            "Myriad": 1_000.0,
        }
        predict_router = ExecutionRouter(
            config,
            poly,
            predict,
            FakeTelegram(),
            balance_cache=balances,
        )
        myriad_router = ExecutionRouter(
            config,
            poly,
            myriad,
            FakeTelegram(),
            second_leg_label="Myriad",
            balance_cache=balances,
        )
        observed: list[tuple[str, str, float | None]] = []
        engine = ArbitrageEngine(
            config,
            poly,
            predict,
            predict_router,
            myriad=myriad,
            myriad_execution=myriad_router,
            signal_evaluation_observer=lambda route, outcome, net_spread: observed.append(
                (route, outcome, net_spread)
            ),
            chain_cost_estimator=_zero_chain_cost_estimator(),
        )
        engine.set_entry_readiness(engine.funded_market_data_ready)
        predict_router.set_entry_readiness(engine.funded_market_data_ready)
        myriad_router.set_entry_readiness(engine.funded_market_data_ready)

        self.assertFalse(engine.funded_market_data_ready())
        await engine.run_once()

        self.assertFalse(engine.funded_market_data_ready())
        self.assertEqual(observed, [])
        self.assertIn("myriad-token:NO", myriad.watch_tokens)
        self.assertFalse(poly.bought)
        self.assertFalse(predict.bought)
        self.assertFalse(myriad.bought)

        myriad.market_data_age = 0.1
        await engine.run_once()

        self.assertTrue(engine.funded_market_data_ready())
        self.assertTrue(observed)

    async def test_canary_primes_stale_funded_target_before_service_entry_gate(self) -> None:
        poly = CountingPreviewClient()
        myriad = RefreshingMarketDataClient()
        poly.market_data_age = 0.1
        myriad.market_data_age = 12.0
        base = make_config(True)
        routes = replace(
            base.routes,
            polymarket_myriad=True,
            polymarket_predict=False,
            predict_myriad=False,
            predict_sx=False,
            polymarket_sx=False,
            sx_myriad=False,
        )
        market = replace(
            make_verified_market(),
            symbol="myriad-market",
            polymarket_token_id="myriad-poly-token",
            predict_fun_token_id="",
            myriad_market_id="myriad-token",
            myriad_side=BinarySide.NO,
            venue_b_label="Myriad",
            verified_routes=frozenset({"polymarket_myriad"}),
        )
        config = replace(
            base,
            execution_mode=ExecutionMode.CANARY,
            _execution_mode_explicit=True,
            routes=routes,
            funded_routes=routes,
            markets=[market],
            myriad_markets=replace(base.myriad_markets, enabled=True),
        )
        balances: dict[str, Decimal | float] = {
            "Polymarket": 1_000.0,
            "Myriad": 1_000.0,
        }
        router = ExecutionRouter(
            config,
            poly,
            myriad,
            FakeTelegram(),
            second_leg_label="Myriad",
            balance_cache=balances,
        )
        observed: list[tuple[str, str, float | None]] = []
        engine = ArbitrageEngine(
            config,
            poly,
            None,
            None,
            myriad=myriad,
            myriad_execution=router,
            signal_evaluation_observer=lambda route, outcome, net_spread: observed.append(
                (route, outcome, net_spread)
            ),
            chain_cost_estimator=_zero_chain_cost_estimator(),
        )
        engine.set_entry_readiness(engine.funded_market_data_ready)
        router.set_entry_readiness(engine.funded_market_data_ready)

        await engine.run_once()

        self.assertEqual(myriad.market_data_age, 0.0)
        self.assertIn("myriad-token:NO", myriad.watch_tokens)
        self.assertTrue(engine.funded_market_data_ready())
        self.assertTrue(observed)

    async def test_canary_real_myriad_bootstrap_is_shared_and_refreshes_before_gate(self) -> None:
        class EngineMyriadClient(MyriadClient):
            def __init__(self, config: MyriadMarketsConfig) -> None:
                super().__init__(config)
                self.orderbook_calls = 0

            def _ensure_ws_task(self) -> None:
                return

            async def get_orderbook(self, market_id: int, outcome_id: int) -> dict[str, object]:
                self.orderbook_calls += 1
                await asyncio.sleep(0.01)
                return {
                    "marketId": market_id,
                    "outcomeId": outcome_id,
                    "bids": [["230000000000000000", "1000000000000000000"]],
                    "asks": [["240000000000000000", "1000000000000000000"]],
                }

        poly = CountingPreviewClient()
        poly.market_data_age = 0.1
        base = make_config(True)
        routes = replace(
            base.routes,
            polymarket_myriad=True,
            polymarket_predict=False,
            predict_myriad=False,
            predict_sx=False,
            polymarket_sx=False,
            sx_myriad=False,
        )
        myriad_config = replace(base.myriad_markets, enabled=True)
        myriad = EngineMyriadClient(myriad_config)
        myriad._ws_connected = True  # noqa: SLF001
        market = replace(
            make_verified_market(),
            symbol="myriad-market",
            polymarket_token_id="myriad-poly-token",
            predict_fun_token_id="",
            myriad_market_id="553",
            myriad_side=BinarySide.NO,
            venue_b_label="Myriad",
            verified_routes=frozenset({"polymarket_myriad"}),
        )
        config = replace(
            base,
            execution_mode=ExecutionMode.CANARY,
            _execution_mode_explicit=True,
            routes=routes,
            funded_routes=routes,
            markets=[market],
            myriad_markets=myriad_config,
        )
        router = ExecutionRouter(
            config,
            poly,
            myriad,
            FakeTelegram(),
            second_leg_label="Myriad",
        )
        await router._risk.pause("test canary remains fail-closed")  # noqa: SLF001
        engine = ArbitrageEngine(
            config,
            poly,
            None,
            None,
            myriad=myriad,
            myriad_execution=router,
            chain_cost_estimator=_zero_chain_cost_estimator(),
        )
        readiness_checks: list[bool] = []

        def funded_gate() -> bool:
            ready = engine.funded_market_data_ready()
            readiness_checks.append(ready)
            return ready

        engine.set_entry_readiness(funded_gate)
        router.set_entry_readiness(funded_gate)

        await engine.run_once()

        token_id = "553:NO"
        self.assertEqual(myriad.orderbook_calls, 1)
        self.assertEqual(readiness_checks, [True])
        self.assertTrue(myriad.market_data_target_ready(token_id, config.max_orderbook_age_seconds))

        myriad._books[token_id] = replace(  # noqa: SLF001
            myriad._books[token_id],  # noqa: SLF001
            timestamp=time.time() - 12.0,
        )
        myriad._book_timestamps[token_id] = time.monotonic() - 12.0  # noqa: SLF001
        myriad._stale_refresh_attempted_at[token_id] = 0.0  # noqa: SLF001
        readiness_checks.clear()

        await engine.run_once()

        self.assertEqual(myriad.orderbook_calls, 2)
        self.assertEqual(readiness_checks, [True])
        self.assertTrue(myriad.market_data_target_ready(token_id, config.max_orderbook_age_seconds))

    async def test_proactive_refresh_is_due_only_and_deduplicated_across_funded_routes(self) -> None:
        class ProactiveRefreshClient(CountingPreviewClient):
            def __init__(self, ages: dict[str, float | None]) -> None:
                super().__init__()
                self.ages = ages
                self.refreshed_targets: list[str] = []

            def market_data_target_age_seconds(self, token_id: str) -> float | None:
                return self.ages.get(token_id)

            async def refresh_market_data_target(self, token_id: str) -> bool:
                self.refreshed_targets.append(token_id)
                self.ages[token_id] = 0.0
                return True

        poly = ProactiveRefreshClient({"poly-due": 1.2})
        predict = ProactiveRefreshClient({"predict-fresh": 0.2})
        myriad = ProactiveRefreshClient({"myriad-due": None})
        engine = ArbitrageEngine(
            make_config(False),
            poly,
            predict,
            None,
            myriad=myriad,
            chain_cost_estimator=_zero_chain_cost_estimator(),
        )
        engine._funded_market_data_targets_by_route = {  # noqa: SLF001
            "polymarket_myriad": (
                ("Myriad", "myriad-due"),
                ("Polymarket", "poly-due"),
            ),
            "polymarket_predict": (
                ("Polymarket", "poly-due"),
                ("Predict.fun", "predict-fresh"),
            ),
        }

        await engine._refresh_funded_market_data_targets(1.0)  # noqa: SLF001

        self.assertEqual(poly.refreshed_targets, ["poly-due"])
        self.assertEqual(predict.refreshed_targets, [])
        self.assertEqual(myriad.refreshed_targets, ["myriad-due"])

    async def test_background_refresh_keeps_quiet_real_myriad_target_continuously_fresh(self) -> None:
        class QuietMyriadClient(MyriadClient):
            def __init__(self, config: MyriadMarketsConfig) -> None:
                super().__init__(config)
                self.orderbook_calls = 0

            def _ensure_ws_task(self) -> None:
                return

            async def get_orderbook(self, market_id: int, outcome_id: int) -> dict[str, object]:
                self.orderbook_calls += 1
                await asyncio.sleep(0.005)
                return {
                    "marketId": market_id,
                    "outcomeId": outcome_id,
                    "bids": [["230000000000000000", "1000000000000000000"]],
                    "asks": [["240000000000000000", "1000000000000000000"]],
                }

        base = make_config(False)
        config = replace(base, max_orderbook_age_seconds=0.4)
        myriad = QuietMyriadClient(
            replace(
                base.myriad_markets,
                enabled=True,
                order_book_ttl_ms=50,
                websocket_stale_after_ms=300,
            )
        )
        myriad.set_market_data_execution_freshness(config.max_orderbook_age_seconds)
        token_id = "553:NO"
        myriad._ensure_token_subscription(token_id, 553)  # noqa: SLF001
        await myriad.watch_order_book(token_id)
        engine = ArbitrageEngine(
            config,
            CountingPreviewClient(),
            None,
            None,
            myriad=myriad,
            chain_cost_estimator=_zero_chain_cost_estimator(),
        )
        engine._funded_market_data_targets_by_route = {  # noqa: SLF001
            "polymarket_myriad": (("Myriad", token_id),),
        }
        monitor = asyncio.create_task(engine._maintain_funded_market_data_freshness())  # noqa: SLF001
        samples: list[bool] = []
        deadline = asyncio.get_running_loop().time() + 1.0
        try:
            while asyncio.get_running_loop().time() < deadline:
                samples.append(
                    myriad.market_data_target_ready(
                        token_id,
                        config.max_orderbook_age_seconds,
                    )
                )
                await asyncio.sleep(0.02)
        finally:
            monitor.cancel()
            await asyncio.gather(monitor, return_exceptions=True)

        self.assertTrue(samples)
        self.assertTrue(all(samples))
        self.assertGreaterEqual(myriad.orderbook_calls, 3)
        self.assertLessEqual(myriad.orderbook_calls, 7)

    async def test_canary_stale_discovery_only_target_does_not_block_funded_route(self) -> None:
        poly = CountingPreviewClient()
        predict = CountingPreviewClient()
        myriad = CountingPreviewClient()
        poly.market_data_age = 0.1
        predict.market_data_age = 0.1
        myriad.market_data_age = 12.0
        base = make_config(True)
        discovery_routes = replace(
            base.routes,
            polymarket_myriad=True,
            polymarket_predict=True,
            predict_myriad=False,
        )
        funded_routes = replace(
            discovery_routes,
            polymarket_myriad=False,
            polymarket_predict=True,
        )
        myriad_market = replace(
            make_verified_market(),
            symbol="myriad-market",
            polymarket_token_id="myriad-poly-token",
            predict_fun_token_id="",
            myriad_market_id="myriad-token",
            myriad_side=BinarySide.NO,
            venue_b_label="Myriad",
            verified_routes=frozenset({"polymarket_myriad"}),
        )
        config = replace(
            base,
            execution_mode=ExecutionMode.CANARY,
            _execution_mode_explicit=True,
            routes=discovery_routes,
            funded_routes=funded_routes,
            markets=[make_verified_market(), myriad_market],
            myriad_markets=replace(base.myriad_markets, enabled=True),
        )
        balances: dict[str, Decimal | float] = {
            "Polymarket": 1_000.0,
            "Predict.fun": 1_000.0,
            "Myriad": 1_000.0,
        }
        predict_router = ExecutionRouter(
            config,
            poly,
            predict,
            FakeTelegram(),
            balance_cache=balances,
        )
        predict_router._funded_canary_deadline_unix = time.time() + 60  # noqa: SLF001
        myriad_router = ExecutionRouter(
            config,
            poly,
            myriad,
            FakeTelegram(),
            second_leg_label="Myriad",
            balance_cache=balances,
        )
        engine = ArbitrageEngine(
            config,
            poly,
            predict,
            predict_router,
            myriad=myriad,
            myriad_execution=myriad_router,
            chain_cost_estimator=_zero_chain_cost_estimator(),
        )
        engine.set_entry_readiness(engine.funded_market_data_ready)
        predict_router.set_entry_readiness(engine.funded_market_data_ready)
        myriad_router.set_entry_readiness(engine.funded_market_data_ready)

        with patch.object(
            engine,
            "_prime_funded_market_data_targets",
            wraps=engine._prime_funded_market_data_targets,  # noqa: SLF001
        ) as targeted_prime:
            await engine.run_once()

        self.assertTrue(engine.funded_market_data_ready())
        targeted_prime.assert_awaited_once()
        prime_call = targeted_prime.await_args
        assert prime_call is not None
        self.assertEqual(
            prime_call.args[0],
            {
                "polymarket_predict": {
                    ("Polymarket", "poly-token"),
                    ("Predict.fun", "predict-token"),
                }
            },
        )
        self.assertTrue(poly.bought)
        self.assertTrue(predict.bought)
        self.assertFalse(myriad.bought)

    async def test_engine_rotates_bounded_market_data_windows_across_full_universe(self) -> None:
        first = FakeBinaryClient()
        second = FakeBinaryClient()
        first.ask = 0.55
        second.ask = 0.55
        markets = [
            replace(
                make_verified_market(),
                symbol=f"market-{index}",
                polymarket_token_id=f"poly-{index}",
                predict_fun_token_id=f"predict-{index}",
            )
            for index in range(5)
        ]
        config = replace(
            make_config(True),
            markets=markets,
            max_concurrent_market_evaluations=2,
        )
        router = ExecutionRouter(config, first, second, FakeTelegram())
        engine = ArbitrageEngine(config, first, second, router)

        for _ in range(3):
            await engine.run_once()

        first_windows = [window for window in first.synced_targets if window]
        second_windows = [window for window in second.synced_targets if window]
        self.assertTrue(first_windows)
        self.assertTrue(second_windows)
        self.assertNotIn(set(), first.synced_targets)
        self.assertNotIn(set(), second.synced_targets)
        # Rotation briefly preserves the previous funded window alongside the
        # next one. The union stays hard-bounded at twice the normal window and
        # is pruned back immediately after the new funded map is published.
        self.assertLessEqual(max(map(len, first_windows)), 4)
        self.assertLessEqual(max(map(len, second_windows)), 4)
        self.assertLessEqual(len(first_windows[-1]), 2)
        self.assertLessEqual(len(second_windows[-1]), 2)
        self.assertEqual(set(first.watch_tokens), {f"poly-{index}" for index in range(5)})
        self.assertEqual(set(second.watch_tokens), {f"predict-{index}" for index in range(5)})
        self.assertTrue(all(window in first_windows for window in first.primed_targets))
        self.assertTrue(all(window in second_windows for window in second.primed_targets))

    async def test_engine_holds_market_data_window_long_enough_for_snapshot_reuse(self) -> None:
        first = FakeBinaryClient()
        second = FakeBinaryClient()
        first.ask = 0.55
        second.ask = 0.55
        markets = [
            replace(
                make_verified_market(),
                symbol=f"market-{index}",
                polymarket_token_id=f"poly-{index}",
                predict_fun_token_id=f"predict-{index}",
            )
            for index in range(5)
        ]
        config = replace(
            make_config(True),
            markets=markets,
            max_concurrent_market_evaluations=2,
            market_data_target_hold_seconds=60.0,
        )
        router = ExecutionRouter(config, first, second, FakeTelegram())
        engine = ArbitrageEngine(config, first, second, router)

        await engine.run_once()
        first_window = set(first.watch_tokens)
        second_window = set(second.watch_tokens)
        await engine.run_once()

        self.assertEqual(set(first.watch_tokens[2:]), first_window)
        self.assertEqual(set(second.watch_tokens[2:]), second_window)
        self.assertEqual(len([window for window in first.synced_targets if window]), 1)

        engine._evaluation_window_expires_at_by_route["polymarket_predict"] = 0.0  # noqa: SLF001
        await engine.run_once()

        self.assertNotEqual(set(first.watch_tokens[-2:]), first_window)
        self.assertNotEqual(set(second.watch_tokens[-2:]), second_window)

    async def test_engine_prefetches_bounded_route_window_and_rotates_evaluations_inside_it(self) -> None:
        first = FakeBinaryClient()
        second = FakeBinaryClient()
        first.ask = 0.55
        second.ask = 0.55
        markets = [
            replace(
                make_verified_market(),
                symbol=f"market-{index}",
                polymarket_token_id=f"poly-{index}",
                predict_fun_token_id=f"predict-{index}",
            )
            for index in range(8)
        ]
        config = replace(
            make_config(True),
            markets=markets,
            max_concurrent_market_evaluations=2,
            market_data_target_hold_seconds_by_route={"polymarket_predict": 60.0},
            market_data_prefetch_multiplier_by_route={"polymarket_predict": 2},
        )
        router = ExecutionRouter(config, first, second, FakeTelegram())
        engine = ArbitrageEngine(config, first, second, router)

        await engine.run_once()
        first_prefetch_window = set(first.synced_targets[-1])
        self.assertEqual(len(first_prefetch_window), 4)
        self.assertEqual(set(first.watch_tokens), {"poly-0", "poly-1"})
        funded_window = engine.funded_market_data_targets()["polymarket_predict"]
        self.assertEqual(
            {token_id for venue, token_id in funded_window if venue == "Polymarket"},
            {"poly-0", "poly-1"},
        )
        self.assertEqual(
            {token_id for venue, token_id in funded_window if venue == "Predict.fun"},
            {"predict-0", "predict-1"},
        )
        self.assertNotEqual(
            {token_id for venue, token_id in funded_window if venue == "Polymarket"},
            first_prefetch_window,
        )

        await engine.run_once()
        self.assertEqual(len(first.synced_targets), 1)
        second_cycle = set(first.watch_tokens[2:])
        self.assertTrue(second_cycle.issubset(first_prefetch_window))
        self.assertTrue(second_cycle & {"poly-0", "poly-1"})
        self.assertTrue(second_cycle - {"poly-0", "poly-1"})
        second_funded_window = engine.funded_market_data_targets()["polymarket_predict"]
        self.assertEqual(
            {token_id for venue, token_id in second_funded_window if venue == "Polymarket"},
            second_cycle,
        )

        engine._evaluation_window_expires_at_by_route["polymarket_predict"] = 0.0  # noqa: SLF001
        await engine.run_once()
        self.assertEqual(len(first.synced_targets), 2)
        self.assertNotEqual(set(first.synced_targets[-1]), first_prefetch_window)
        self.assertLessEqual(len(first.watch_tokens[-2:]), config.max_concurrent_market_evaluations)

    async def test_funded_window_rotation_preserves_published_targets_until_prime_finishes(self) -> None:
        class BlockingRotationClient(FakeBinaryClient):
            def __init__(self) -> None:
                super().__init__()
                self.active_targets: set[str] = set()
                self.ready_targets: set[str] = set()
                self.published_targets: set[str] = set()
                self.block_new_targets = False
                self.blocked_token: str | None = None
                self.prime_started = asyncio.Event()
                self.release_prime = asyncio.Event()

            def sync_market_data_targets(self, token_ids: set[str]) -> None:
                self.active_targets = set(token_ids)
                self.ready_targets.intersection_update(token_ids)
                super().sync_market_data_targets(token_ids)

            def market_data_target_ready(self, token_id: str, max_age_seconds: float) -> bool:
                del max_age_seconds
                return token_id in self.active_targets and token_id in self.ready_targets

            async def watch_order_book(self, token_id: str) -> OrderBook:
                if (
                    self.block_new_targets
                    and token_id not in self.published_targets
                    and token_id not in self.ready_targets
                ):
                    self.blocked_token = token_id
                    self.prime_started.set()
                    await self.release_prime.wait()
                self.ready_targets.add(token_id)
                return await super().watch_order_book(token_id)

        first = BlockingRotationClient()
        second = BlockingRotationClient()
        first.ask = 0.55
        second.ask = 0.55
        markets = [
            replace(
                make_verified_market(),
                symbol=f"market-{index}",
                polymarket_token_id=f"poly-{index}",
                predict_fun_token_id=f"predict-{index}",
            )
            for index in range(3)
        ]
        base = make_config(True)
        routes = replace(
            base.routes,
            polymarket_predict=True,
            polymarket_myriad=False,
            predict_myriad=False,
            predict_sx=False,
            polymarket_sx=False,
            sx_myriad=False,
        )
        config = replace(
            base,
            routes=routes,
            funded_routes=routes,
            markets=markets,
            max_concurrent_market_evaluations=1,
            market_data_target_hold_seconds_by_route={"polymarket_predict": 60.0},
        )
        router = ExecutionRouter(config, first, second, FakeTelegram())
        engine = ArbitrageEngine(config, first, second, router)

        await engine.run_once()
        old_window = engine.funded_market_data_targets()["polymarket_predict"]
        old_first = {token for venue, token in old_window if venue == "Polymarket"}
        old_second = {token for venue, token in old_window if venue == "Predict.fun"}
        self.assertTrue(engine.funded_market_data_ready())
        first.published_targets = set(old_first)
        second.published_targets = set(old_second)
        first.block_new_targets = True
        second.block_new_targets = True
        engine._evaluation_window_expires_at_by_route["polymarket_predict"] = 0.0  # noqa: SLF001

        rotation = asyncio.create_task(engine.run_once())
        await asyncio.wait_for(
            asyncio.gather(first.prime_started.wait(), second.prime_started.wait()),
            timeout=1.0,
        )

        self.assertEqual(engine.funded_market_data_targets()["polymarket_predict"], old_window)
        self.assertTrue(engine.funded_market_data_ready())
        self.assertTrue(old_first.issubset(first.active_targets))
        self.assertTrue(old_second.issubset(second.active_targets))
        self.assertIsNotNone(first.blocked_token)
        self.assertIsNotNone(second.blocked_token)

        first.release_prime.set()
        second.release_prime.set()
        await rotation

        new_window = engine.funded_market_data_targets()["polymarket_predict"]
        self.assertNotEqual(new_window, old_window)
        self.assertTrue(engine.funded_market_data_ready())
        self.assertFalse(old_first & first.active_targets)
        self.assertFalse(old_second & second.active_targets)

    async def test_engine_keeps_recent_executable_market_and_reserves_exploration_slot(self) -> None:
        class DepthSelectiveClient(FakeBinaryClient):
            async def watch_order_book(self, token_id: str) -> OrderBook:
                self.watch_tokens.append(token_id)
                size = 1000 if token_id == "predict-0" else 0.01
                return OrderBook(
                    bids=[OrderBookLevel(self.bid, 1000)],
                    asks=[OrderBookLevel(self.ask, size)],
                    timestamp=self.book_timestamp,
                )

        first = FakeBinaryClient()
        second = DepthSelectiveClient()
        markets = [
            replace(
                make_verified_market(),
                symbol=f"market-{index}",
                polymarket_token_id=f"poly-{index}",
                predict_fun_token_id=f"predict-{index}",
            )
            for index in range(4)
        ]
        config = replace(
            make_config(True),
            markets=markets,
            min_net_spread=0.50,
            max_concurrent_market_evaluations=2,
            market_data_target_hold_seconds_by_route={"polymarket_predict": 1.0},
            market_data_executable_priority_seconds_by_route={"polymarket_predict": 60.0},
        )
        router = ExecutionRouter(config, first, second, FakeTelegram())
        engine = ArbitrageEngine(config, first, second, router)

        await engine.run_once()
        executable_key = next(iter(engine._recent_executable_evaluations))  # noqa: SLF001
        observation = engine._recent_executable_evaluations[executable_key]  # noqa: SLF001
        engine._recent_executable_evaluations[executable_key] = replace(  # noqa: SLF001
            observation,
            observed_at=time.monotonic() - 5.0,
        )
        engine._evaluation_window_expires_at_by_route["polymarket_predict"] = 0.0  # noqa: SLF001
        first.watch_tokens.clear()
        second.watch_tokens.clear()

        await engine.run_once()

        self.assertIn("poly-0", first.watch_tokens)
        self.assertIn("predict-0", second.watch_tokens)
        self.assertEqual(len(first.watch_tokens), 2)
        self.assertEqual(len(second.watch_tokens), 2)
        self.assertNotEqual(set(second.watch_tokens), {"predict-0"})
        self.assertIn("predict-0", second.synced_targets[-1])

    async def test_engine_reserves_configured_exploration_fraction(self) -> None:
        first = FakeBinaryClient()
        second = FakeBinaryClient()
        markets = [
            replace(
                make_verified_market(),
                symbol=f"market-{index}",
                polymarket_token_id=f"poly-{index}",
                predict_fun_token_id=f"predict-{index}",
            )
            for index in range(8)
        ]
        config = replace(
            make_config(True),
            markets=markets,
            min_net_spread=0.50,
            max_concurrent_market_evaluations=4,
            market_data_target_hold_seconds_by_route={"polymarket_predict": 60.0},
            market_data_exploration_fraction_by_route={"polymarket_predict": 0.5},
        )
        router = ExecutionRouter(config, first, second, FakeTelegram())
        engine = ArbitrageEngine(config, first, second, router)

        await engine.run_once()
        engine._evaluation_window_expires_at_by_route["polymarket_predict"] = 0.0  # noqa: SLF001
        first.watch_tokens.clear()
        second.watch_tokens.clear()

        await engine.run_once()

        self.assertGreaterEqual(len(set(second.watch_tokens) & {"predict-4", "predict-5", "predict-6"}), 2)

    async def test_engine_prioritizes_best_recent_net_spread(self) -> None:
        first = FakeBinaryClient()
        second = FakeBinaryClient()
        markets = [
            replace(
                make_verified_market(),
                symbol=f"market-{index}",
                polymarket_token_id=f"poly-{index}",
                predict_fun_token_id=f"predict-{index}",
            )
            for index in range(3)
        ]
        config = replace(
            make_config(True),
            markets=markets,
            max_concurrent_market_evaluations=2,
            market_data_target_hold_seconds_by_route={"polymarket_predict": 60.0},
            market_data_exploration_fraction_by_route={"polymarket_predict": 0.5},
        )
        router = ExecutionRouter(config, first, second, FakeTelegram())
        engine = ArbitrageEngine(config, first, second, router)

        await engine.run_once()
        evaluations = list(engine._planned_evaluations)  # noqa: SLF001
        engine._recent_executable_evaluations.clear()  # noqa: SLF001
        engine._mark_recent_executable("polymarket_predict", evaluations[0].targets, -0.10)  # noqa: SLF001
        engine._mark_recent_executable("polymarket_predict", evaluations[1].targets, 0.02)  # noqa: SLF001
        engine._evaluation_window_expires_at_by_route["polymarket_predict"] = 0.0  # noqa: SLF001

        first.watch_tokens.clear()
        second.watch_tokens.clear()
        await engine.run_once()

        self.assertIn("predict-1", second.watch_tokens)

    async def test_engine_reuses_planned_evaluations_until_market_snapshot_changes(self) -> None:
        first = FakeBinaryClient()
        second = FakeBinaryClient()
        markets = tuple(
            replace(
                make_verified_market(),
                symbol=f"market-{index}",
                polymarket_token_id=f"poly-{index}",
                predict_fun_token_id=f"predict-{index}",
            )
            for index in range(2)
        )
        snapshots: list[tuple[MarketSpec, ...]] = [markets]
        config = replace(
            make_config(True),
            markets=[],
            min_net_spread=0.50,
            max_concurrent_market_evaluations=2,
            market_data_target_hold_seconds=60.0,
        )
        router = ExecutionRouter(config, first, second, FakeTelegram())
        engine = ArbitrageEngine(
            config,
            first,
            second,
            router,
            market_provider=lambda: snapshots[0],
        )

        await engine.run_once()
        first_plan = engine._planned_evaluations  # noqa: SLF001
        first_route_index = engine._planned_evaluations_by_route  # noqa: SLF001
        first_held_window = engine._held_evaluations_by_route["polymarket_predict"]  # noqa: SLF001
        await engine.run_once()

        self.assertIs(engine._planned_evaluations, first_plan)  # noqa: SLF001
        self.assertIs(engine._planned_evaluations_by_route, first_route_index)  # noqa: SLF001
        self.assertIs(  # noqa: SLF001
            engine._held_evaluations_by_route["polymarket_predict"],
            first_held_window,
        )

        snapshots[0] = (
            *markets,
            replace(
                make_verified_market(),
                symbol="market-2",
                polymarket_token_id="poly-2",
                predict_fun_token_id="predict-2",
            ),
        )
        await engine.run_once()

        self.assertIsNot(engine._planned_evaluations, first_plan)  # noqa: SLF001
        self.assertIsNot(engine._planned_evaluations_by_route, first_route_index)  # noqa: SLF001
        self.assertIsNot(  # noqa: SLF001
            engine._held_evaluations_by_route["polymarket_predict"],
            first_held_window,
        )
        self.assertEqual(len(engine._planned_evaluations), 3)  # noqa: SLF001

    async def test_engine_reserves_evaluation_capacity_for_each_enabled_route(self) -> None:
        poly = FakeBinaryClient()
        predict = FakeBinaryClient()
        myriad = FakeBinaryClient()
        config = make_config(True)
        predict_markets = [
            replace(
                make_verified_market(),
                symbol=f"predict-{index}",
                polymarket_token_id=f"predict-poly-{index}",
                predict_fun_token_id=f"predict-token-{index}",
            )
            for index in range(5)
        ]
        myriad_market = replace(
            make_verified_market(),
            symbol="myriad-market",
            polymarket_token_id="myriad-poly-token",
            predict_fun_token_id="",
            myriad_market_id="myriad-token",
            myriad_side=BinarySide.NO,
            venue_b_label="Myriad",
            verified_routes=frozenset({"polymarket_myriad"}),
        )
        config = replace(
            config,
            max_concurrent_market_evaluations=2,
            markets=[*predict_markets, myriad_market],
            myriad_markets=replace(config.myriad_markets, enabled=True),
            routes=replace(config.routes, predict_myriad=False),
        )
        predict_router = ExecutionRouter(config, poly, predict, FakeTelegram())
        myriad_router = ExecutionRouter(config, poly, myriad, FakeTelegram(), second_leg_label="Myriad")
        engine = ArbitrageEngine(
            config,
            poly,
            predict,
            predict_router,
            myriad=myriad,
            myriad_execution=myriad_router,
        )

        await engine.run_once()

        self.assertEqual(len(predict.synced_targets[-1]), 1)
        self.assertEqual(myriad.synced_targets[-1], {"myriad-token:NO"})
        self.assertEqual(poly.synced_targets[-1], {"predict-poly-0", "myriad-poly-token"})

    async def test_engine_alternates_routes_when_only_one_evaluation_slot_is_available(self) -> None:
        poly = FakeBinaryClient()
        predict = FakeBinaryClient()
        myriad = FakeBinaryClient()
        config = make_config(True)
        predict_market = replace(
            make_verified_market(),
            symbol="predict-market",
            polymarket_token_id="predict-poly",
            predict_fun_token_id="predict-token",
        )
        myriad_market = replace(
            make_verified_market(),
            symbol="myriad-market",
            polymarket_token_id="myriad-poly",
            predict_fun_token_id="",
            myriad_market_id="myriad-token",
            myriad_side=BinarySide.NO,
            venue_b_label="Myriad",
            verified_routes=frozenset({"polymarket_myriad"}),
        )
        config = replace(
            config,
            max_concurrent_market_evaluations=1,
            markets=[predict_market, myriad_market],
            myriad_markets=replace(config.myriad_markets, enabled=True),
            routes=replace(config.routes, predict_myriad=False),
        )
        engine = ArbitrageEngine(
            config,
            poly,
            predict,
            ExecutionRouter(config, poly, predict, FakeTelegram()),
            myriad=myriad,
            myriad_execution=ExecutionRouter(config, poly, myriad, FakeTelegram(), second_leg_label="Myriad"),
        )

        await engine.run_once()
        await engine.run_once()

        self.assertEqual(set(predict.watch_tokens), {"predict-token"})
        self.assertEqual(set(myriad.watch_tokens), {"myriad-token:NO"})

    async def test_engine_allocates_bounded_evaluation_slots_by_route_weight(self) -> None:
        poly = FakeBinaryClient()
        predict = FakeBinaryClient()
        myriad = FakeBinaryClient()
        config = make_config(True)
        predict_markets = [
            replace(
                make_verified_market(),
                symbol=f"predict-{index}",
                polymarket_token_id=f"predict-poly-{index}",
                predict_fun_token_id=f"predict-token-{index}",
            )
            for index in range(5)
        ]
        myriad_markets = [
            replace(
                make_verified_market(),
                symbol=f"myriad-{index}",
                polymarket_token_id=f"myriad-poly-{index}",
                predict_fun_token_id="",
                myriad_market_id=str(index),
                myriad_side=BinarySide.NO,
                venue_b_label="Myriad",
                verified_routes=frozenset({"polymarket_myriad"}),
            )
            for index in range(5)
        ]
        config = replace(
            config,
            max_concurrent_market_evaluations=6,
            market_evaluation_weight_by_route={
                "polymarket_predict": 1,
                "polymarket_myriad": 2,
            },
            markets=[*predict_markets, *myriad_markets],
            myriad_markets=replace(config.myriad_markets, enabled=True),
            routes=replace(config.routes, predict_myriad=False),
        )
        engine = ArbitrageEngine(
            config,
            poly,
            predict,
            ExecutionRouter(config, poly, predict, FakeTelegram()),
            myriad=myriad,
            myriad_execution=ExecutionRouter(config, poly, myriad, FakeTelegram(), second_leg_label="Myriad"),
        )

        await engine.run_once()

        # Signed preflight performs additional reads of the selected tokens.
        # Unique tokens are the actual bounded evaluation allocation.
        self.assertEqual(len(set(predict.watch_tokens)), 2)
        self.assertEqual(len(set(myriad.watch_tokens)), 4)
        self.assertEqual(len(set(poly.watch_tokens)), 6)

    async def test_canary_engine_does_not_evaluate_while_risk_is_paused(self) -> None:
        first = CountingPreviewClient()
        second = CountingPreviewClient()
        observed: list[tuple[str, str, float | None]] = []
        market = make_verified_market()
        config = replace(
            make_config(True),
            execution_mode=ExecutionMode.CANARY,
            _execution_mode_explicit=True,
            markets=[market],
        )
        router = ExecutionRouter(config, first, second, FakeTelegram())
        await router._risk.pause("canary circuit open")  # noqa: SLF001
        engine = ArbitrageEngine(
            config,
            first,
            second,
            router,
            signal_evaluation_observer=lambda route, outcome, net_spread: observed.append((route, outcome, net_spread)),
        )

        await engine.run_once()

        self.assertEqual(observed, [])
        self.assertEqual(first.watch_tokens, [])
        self.assertEqual(second.watch_tokens, [])
        self.assertEqual(first.preview_signature_calls, 0)
        self.assertEqual(second.preview_signature_calls, 0)

    async def test_canary_engine_does_not_evaluate_while_reconciliation_is_not_ready(self) -> None:
        first = CountingPreviewClient()
        second = CountingPreviewClient()
        observed: list[tuple[str, str, float | None]] = []
        config = replace(
            make_config(True),
            execution_mode=ExecutionMode.CANARY,
            _execution_mode_explicit=True,
            markets=[make_verified_market()],
        )
        router = ExecutionRouter(config, first, second, FakeTelegram())
        engine = ArbitrageEngine(
            config,
            first,
            second,
            router,
            signal_evaluation_observer=lambda route, outcome, net_spread: observed.append(
                (route, outcome, net_spread)
            ),
        )
        reconciliation_ready = False
        engine.set_entry_readiness(lambda: reconciliation_ready)

        await engine.run_once()

        self.assertEqual(observed, [])
        self.assertEqual(first.watch_tokens, [])
        self.assertEqual(second.watch_tokens, [])
        self.assertEqual(first.preview_signature_calls, 0)
        self.assertEqual(second.preview_signature_calls, 0)

        reconciliation_ready = True
        await engine.run_once()
        self.assertTrue(observed)

    async def test_entry_rechecks_reconciliation_after_slow_signed_preflight(self) -> None:
        class BlockingPreviewClient(CountingPreviewClient):
            def __init__(self) -> None:
                super().__init__()
                self.preview_started = asyncio.Event()
                self.release_preview = asyncio.Event()

            async def _preview_buy_signature(
                self,
                token_id: str,
                side: BinarySide,
                contracts: Decimal,
                max_price: Decimal,
                *,
                condition_id: str | None,
                tick_size: str | None,
                neg_risk: bool | None,
            ) -> str | None:
                self.preview_started.set()
                await self.release_preview.wait()
                return await super()._preview_buy_signature(
                    token_id,
                    side,
                    contracts,
                    max_price,
                    condition_id=condition_id,
                    tick_size=tick_size,
                    neg_risk=neg_risk,
                )

        first = BlockingPreviewClient()
        second = CountingPreviewClient()
        config = replace(
            make_config(True),
            execution_mode=ExecutionMode.CANARY,
            _execution_mode_explicit=True,
        )
        router = ExecutionRouter(config, first, second, FakeTelegram())
        reconciliation_ready = True
        router.set_entry_readiness(lambda: reconciliation_ready)

        entry = asyncio.create_task(router.handle_signal(make_verified_signal(0.20)))
        await asyncio.wait_for(first.preview_started.wait(), timeout=1.0)
        reconciliation_ready = False
        first.release_preview.set()
        await entry

        self.assertFalse(first.bought)
        self.assertFalse(second.bought)
        self.assertEqual(router.ledger.all(), [])

    async def test_entry_rejects_replaced_discovery_snapshot_after_slow_signed_preflight(self) -> None:
        class BlockingPreviewClient(CountingPreviewClient):
            def __init__(self) -> None:
                super().__init__()
                self.preview_started = asyncio.Event()
                self.release_preview = asyncio.Event()

            async def _preview_buy_signature(
                self,
                token_id: str,
                side: BinarySide,
                contracts: Decimal,
                max_price: Decimal,
                *,
                condition_id: str | None,
                tick_size: str | None,
                neg_risk: bool | None,
            ) -> str | None:
                self.preview_started.set()
                await self.release_preview.wait()
                return await super()._preview_buy_signature(
                    token_id,
                    side,
                    contracts,
                    max_price,
                    condition_id=condition_id,
                    tick_size=tick_size,
                    neg_risk=neg_risk,
                )

        old_market = make_verified_market()
        replacement_market = replace(
            old_market,
            symbol="replacement-market",
            polymarket_token_id="replacement-poly-token",
            predict_fun_token_id="replacement-predict-token",
        )
        registry = ActiveMarketRegistry(
            [old_market],
            route_statuses=(("polymarket_predict", "ready_verified"),),
        )
        first = BlockingPreviewClient()
        second = CountingPreviewClient()
        base = make_config(True)
        funded_routes = replace(
            base.routes,
            polymarket_myriad=False,
            polymarket_predict=True,
            predict_myriad=False,
        )
        config = replace(
            base,
            execution_mode=ExecutionMode.CANARY,
            _execution_mode_explicit=True,
            routes=funded_routes,
            funded_routes=funded_routes,
        )
        router = ExecutionRouter(config, first, second, FakeTelegram())
        engine = ArbitrageEngine(
            config,
            first,
            second,
            router,
            market_provider=lambda: registry.tradable_snapshot(config.execution_mode),
            market_generation_provider=lambda: registry.generation,
            chain_cost_estimator=_zero_chain_cost_estimator(),
        )

        def runtime_ready() -> bool:
            return registry.ready and engine.funded_market_data_ready()

        engine.set_entry_readiness(runtime_ready)
        router.set_entry_readiness(runtime_ready)
        router.set_entry_snapshot_readiness(
            lambda generation: generation is not None
            and registry.ready
            and generation == registry.generation
        )

        entry = asyncio.create_task(engine.run_once())
        await asyncio.wait_for(first.preview_started.wait(), timeout=1.0)
        registry.publish(
            DiscoveryResult(
                (replacement_market,),
                route_statuses=(("polymarket_predict", "ready_verified"),),
            )
        )
        self.assertTrue(registry.ready)
        first.release_preview.set()
        await entry

        self.assertFalse(first.bought)
        self.assertFalse(second.bought)
        self.assertEqual(router.ledger.all(), [])

    async def test_entry_rechecks_reconciliation_after_durable_intent_write(self) -> None:
        first = FakeBinaryClient()
        second = FakeBinaryClient()
        created: list[Any] = []
        updates: list[tuple[str, OrderIntentStatus]] = []
        both_intents_created = asyncio.Event()
        release_intent_writes = asyncio.Event()

        async def create_order_intent(intent: Any) -> None:
            created.append(intent)
            if len(created) == 2:
                both_intents_created.set()
            await release_intent_writes.wait()

        async def update_order_intent(
            client_order_id: str,
            status: OrderIntentStatus,
            **kwargs: Any,
        ) -> None:
            del kwargs
            updates.append((client_order_id, status))

        repository = SimpleNamespace(
            create_order_intent=create_order_intent,
            update_order_intent=update_order_intent,
        )
        config = replace(
            make_config(True),
            execution_mode=ExecutionMode.CANARY,
            _execution_mode_explicit=True,
        )
        router = ExecutionRouter(
            config,
            first,
            second,
            FakeTelegram(),
            repository=repository,  # type: ignore[arg-type]
        )
        reconciliation_ready = True
        router.set_entry_readiness(lambda: reconciliation_ready)
        market = make_verified_market()

        submissions = asyncio.gather(
                router._submit_entry_leg(  # noqa: SLF001
                    client=first,
                    market=market,
                    venue_label="Polymarket",
                    token_id=market.polymarket_token_id,
                    side=market.polymarket_side,
                    contracts=Decimal("1"),
                    max_price=0.5,
                    capital_usd=Decimal("0.5"),
                    timeout_ms=100,
                ),
                router._submit_entry_leg(  # noqa: SLF001
                    client=second,
                    market=market,
                    venue_label="Predict.fun",
                    token_id=market.predict_fun_token_id or "",
                    side=market.predict_fun_side,
                    contracts=Decimal("1"),
                    max_price=0.5,
                    capital_usd=Decimal("0.5"),
                    timeout_ms=100,
                ),
        )
        await asyncio.wait_for(both_intents_created.wait(), timeout=1.0)
        reconciliation_ready = False
        release_intent_writes.set()
        results = await submissions

        self.assertFalse(first.bought)
        self.assertFalse(second.bought)
        self.assertTrue(all(isinstance(result.error, OrderSubmissionRejected) for result in results))
        final_statuses = {
            client_order_id: status
            for client_order_id, status in updates
        }
        self.assertEqual(set(final_statuses.values()), {OrderIntentStatus.CANCELLED})
        self.assertEqual(len(final_statuses), 2)

    async def test_paused_shadow_engine_collects_signed_technical_evidence_without_orders(self) -> None:
        first = CountingPreviewClient()
        second = CountingPreviewClient()
        first.ask = 0.40
        second.ask = 0.40
        observed: list[tuple[str, str, float | None]] = []
        preflight_outcomes: list[tuple[str, str]] = []
        config = replace(
            make_config(True),
            markets=[make_verified_market()],
            position_size_usd=20,
            shadow_preflight_samples=3,
            shadow_preflight_sample_interval_seconds=0,
            shadow_preflight_cooldown_seconds=0,
        )
        router = ExecutionRouter(
            config,
            first,
            second,
            FakeTelegram(),
            shadow_preflight_observer=lambda route, outcome: preflight_outcomes.append((route, outcome)),
        )
        await router._risk.pause("shadow opportunity monitor")  # noqa: SLF001
        engine = ArbitrageEngine(
            config,
            first,
            second,
            router,
            signal_evaluation_observer=lambda route, outcome, net_spread: observed.append((route, outcome, net_spread)),
        )

        await engine.run_once()

        self.assertEqual(observed, [("polymarket_predict", "eligible_signal", 0.2)])
        self.assertEqual(first.preview_signature_calls, 3)
        self.assertEqual(second.preview_signature_calls, 3)
        self.assertEqual(preflight_outcomes, [("polymarket_predict", "evidence_passed")])
        self.assertFalse(first.bought)
        self.assertFalse(second.bought)

    async def test_calibration_counts_each_valid_evaluation_but_deduplicates_adverse_observations(self) -> None:
        first = FakeBinaryClient()
        second = FakeBinaryClient()
        first.ask = 0.55
        second.ask = 0.55
        calibration: list[tuple[str, float | None]] = []
        config = replace(make_config(True), markets=[make_market()])
        router = ExecutionRouter(config, first, second, FakeTelegram())
        engine = ArbitrageEngine(
            config,
            first,
            second,
            router,
            calibration_observer=lambda route, adverse_move: calibration.append((route, adverse_move)),
        )

        await engine.run_once()
        await engine.run_once()
        second.book_timestamp += 1
        await engine.run_once()

        self.assertEqual(
            calibration,
            [
                ("polymarket_predict", None),
                ("polymarket_predict", None),
                ("polymarket_predict", None),
            ],
        )

    async def test_calibration_counts_changed_amm_reserve_snapshot(self) -> None:
        client = FakeBinaryClient()
        calibration: list[tuple[str, float | None]] = []
        config = replace(make_config(True), markets=[make_market()])
        engine = ArbitrageEngine(
            config,
            client,
            client,
            None,
            calibration_observer=lambda route, adverse_move: calibration.append((route, adverse_move)),
        )
        book = await client.watch_order_book("token")

        engine._record_route_calibration(  # noqa: SLF001
            "polymarket_predict",
            "market",
            0.03,
            book,
            None,
            None,
            AmmPool(100, 100),
        )
        engine._record_route_calibration(  # noqa: SLF001
            "polymarket_predict",
            "market",
            0.04,
            book,
            None,
            None,
            AmmPool(110, 90),
        )

        self.assertEqual(len(calibration), 2)

    async def test_calibration_ignores_reference_older_than_retention_window(self) -> None:
        client = FakeBinaryClient()
        calibration: list[tuple[str, float | None]] = []
        config = replace(make_config(True), markets=[make_market()])
        engine = ArbitrageEngine(
            config,
            client,
            client,
            None,
            calibration_observer=lambda route, adverse_move: calibration.append((route, adverse_move)),
        )

        with patch("arbitrage_engine.engine.time.monotonic", side_effect=(100.0, 131.0)):
            engine._record_route_calibration(  # noqa: SLF001
                "polymarket_predict",
                "market",
                0.20,
                None,
                None,
                AmmPool(100, 100),
                None,
            )
            engine._record_route_calibration(  # noqa: SLF001
                "polymarket_predict",
                "market",
                -0.20,
                None,
                None,
                AmmPool(110, 90),
                None,
            )

        self.assertEqual(
            calibration,
            [
                ("polymarket_predict", None),
                ("polymarket_predict", None),
            ],
        )

    async def test_calibration_measures_recent_reference_at_execution_horizon(self) -> None:
        client = FakeBinaryClient()
        calibration: list[tuple[str, float | None]] = []
        config = replace(make_config(True), markets=[make_market()])
        engine = ArbitrageEngine(
            config,
            client,
            client,
            None,
            calibration_observer=lambda route, adverse_move: calibration.append((route, adverse_move)),
        )

        with patch("arbitrage_engine.engine.time.monotonic", side_effect=(100.0, 105.0)):
            engine._record_route_calibration(  # noqa: SLF001
                "polymarket_predict",
                "market",
                0.05,
                None,
                None,
                AmmPool(100, 100),
                None,
            )
            engine._record_route_calibration(  # noqa: SLF001
                "polymarket_predict",
                "market",
                0.02,
                None,
                None,
                AmmPool(110, 90),
                None,
            )

        self.assertEqual(calibration[0], ("polymarket_predict", None))
        self.assertEqual(calibration[1][0], "polymarket_predict")
        self.assertAlmostEqual(calibration[1][1] or 0.0, 0.03)

    async def test_engine_reports_unavailable_orderbook_by_route(self) -> None:
        first = FakeBinaryClient()
        second = UnavailableBookClient()
        observed: list[tuple[str, str, float | None]] = []
        config = replace(make_config(True), markets=[make_market()])
        router = ExecutionRouter(config, first, second, FakeTelegram())
        engine = ArbitrageEngine(
            config,
            first,
            second,
            router,
            signal_evaluation_observer=lambda route, outcome, net_spread: observed.append((route, outcome, net_spread)),
        )

        await engine.run_once()

        self.assertEqual(observed, [("polymarket_predict", "unavailable_book", None)])

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

    async def test_market_data_heartbeat_monitors_sx_stream(self) -> None:
        sx = FakeBinaryClient()
        sx.market_data_age = 30.0
        sx.stream_connected = False
        telegram = FakeTelegram()
        config = replace(
            make_config(True),
            websocket_heartbeat_interval_seconds=0.01,
            websocket_stale_after_seconds=10.0,
        )
        engine = ArbitrageEngine(
            config,
            FakeBinaryClient(),
            FakeBinaryClient(),
            None,
            sx_bet=sx,
            telegram=telegram,
        )
        task = asyncio.create_task(engine._monitor_market_data_heartbeat())

        for _ in range(20):
            if sx.reconnect_calls:
                break
            await asyncio.sleep(0.01)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

        self.assertGreaterEqual(sx.reconnect_calls, 1)
        self.assertGreaterEqual(telegram.messages, 1)

    async def test_run_forever_drains_active_cycle_before_shutdown(self) -> None:
        cycle_started = asyncio.Event()
        release_cycle = asyncio.Event()
        shutdown_event = asyncio.Event()

        class DrainingEngine(ArbitrageEngine):
            def __init__(self) -> None:
                super().__init__(
                    replace(make_config(True), poll_interval_ms=60_000),
                    FakeBinaryClient(),
                    FakeBinaryClient(),
                    None,
                )
                self.cycles = 0

            async def run_once(self) -> None:
                self.cycles += 1
                cycle_started.set()
                await release_cycle.wait()

        engine = DrainingEngine()
        task = asyncio.create_task(engine.run_forever(shutdown_event=shutdown_event))
        await asyncio.wait_for(cycle_started.wait(), timeout=0.2)

        shutdown_event.set()
        await asyncio.sleep(0)
        self.assertFalse(task.done())

        release_cycle.set()
        await asyncio.wait_for(task, timeout=0.2)
        self.assertEqual(engine.cycles, 1)

    async def test_run_forever_wakes_immediately_from_poll_delay_on_shutdown(self) -> None:
        shutdown_event = asyncio.Event()

        class IdleEngine(ArbitrageEngine):
            def __init__(self) -> None:
                super().__init__(
                    replace(make_config(True), poll_interval_ms=60_000),
                    FakeBinaryClient(),
                    FakeBinaryClient(),
                    None,
                )
                self.cycle_completed = asyncio.Event()

            async def run_once(self) -> None:
                self.cycle_completed.set()

        engine = IdleEngine()
        task = asyncio.create_task(engine.run_forever(shutdown_event=shutdown_event))
        await asyncio.wait_for(engine.cycle_completed.wait(), timeout=0.2)

        shutdown_event.set()

        await asyncio.wait_for(task, timeout=0.2)

    async def test_market_data_heartbeat_does_not_reconnect_quiet_connected_stream(self) -> None:
        first = FakeBinaryClient()
        first.market_data_age = 30.0
        first.stream_connected = True
        telegram = FakeTelegram()
        config = replace(
            make_config(True),
            websocket_heartbeat_interval_seconds=0.01,
            websocket_stale_after_seconds=10.0,
        )
        engine = ArbitrageEngine(config, first, FakeBinaryClient(), None, telegram=telegram)
        task = asyncio.create_task(engine._monitor_market_data_heartbeat())

        await asyncio.sleep(0.05)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

        self.assertEqual(first.reconnect_calls, 0)
        self.assertEqual(telegram.messages, 0)

    async def test_market_data_heartbeat_ignores_disconnected_client_without_targets(self) -> None:
        first = FakeBinaryClient()
        first.stream_connected = False
        telegram = FakeTelegram()
        config = replace(make_config(True), websocket_heartbeat_interval_seconds=0.01)
        engine = ArbitrageEngine(config, first, FakeBinaryClient(), None, telegram=telegram)
        task = asyncio.create_task(engine._monitor_market_data_heartbeat())

        await asyncio.sleep(0.05)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

        self.assertEqual(first.reconnect_calls, 0)
        self.assertEqual(telegram.messages, 0)

    async def test_market_data_heartbeat_does_not_block_second_reconnect_on_slow_telegram(self) -> None:
        first = FakeBinaryClient()
        first.market_data_age = 30.0
        myriad = FakeBinaryClient()
        myriad.market_data_age = 30.0
        telegram = SlowTelegram()
        config = replace(
            make_config(True),
            websocket_heartbeat_interval_seconds=0.01,
            websocket_stale_after_seconds=10.0,
        )
        engine = ArbitrageEngine(
            config,
            first,
            None,
            None,
            myriad=myriad,
            telegram=telegram,
        )
        task = asyncio.create_task(engine._monitor_market_data_heartbeat())

        for _ in range(10):
            if first.reconnect_calls and myriad.reconnect_calls:
                break
            await asyncio.sleep(0.01)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        background = list(engine._background_tasks)
        for pending in background:
            pending.cancel()
        if background:
            await asyncio.gather(*background, return_exceptions=True)

        self.assertGreaterEqual(first.reconnect_calls, 1)
        self.assertGreaterEqual(myriad.reconnect_calls, 1)

    async def test_engine_syncs_active_and_open_position_market_data_targets(self) -> None:
        poly = FakeBinaryClient()
        predict = FakeBinaryClient()
        myriad = FakeBinaryClient()
        ledger = PositionLedger()
        config = make_config(True)
        market = replace(make_market(), myriad_market_id="123", myriad_side=BinarySide.NO)
        open_market = replace(
            market,
            venue_b_label="Myriad",
            predict_fun_token_id="999:YES",
            predict_fun_side=BinarySide.YES,
        )
        ledger.add(
            _open_position(
                market=open_market,
                polymarket_contracts=10,
                polymarket_entry_price=0.42,
                predict_fun_contracts=10,
                predict_fun_entry_price=0.47,
                opened_at=datetime.now(UTC),
                polymarket_order_id="poly",
                predict_fun_order_id="myriad",
            )
        )
        config = replace(
            config,
            myriad_markets=replace(config.myriad_markets, enabled=True),
            markets=[market],
        )
        poly_myriad = ExecutionRouter(config, poly, myriad, FakeTelegram(), ledger, second_leg_label="Myriad")
        engine = ArbitrageEngine(
            config,
            poly,
            predict,
            None,
            myriad=myriad,
            myriad_execution=poly_myriad,
        )

        await engine.run_once()

        self.assertIn({"poly-token"}, poly.synced_targets)
        self.assertIn({"123:NO", "999:YES"}, myriad.synced_targets)
        self.assertEqual(predict.synced_targets, [])

    async def test_shadow_start_refreshes_read_only_balance_state_once(self) -> None:
        first = FakeBinaryClient()
        second = FakeBinaryClient()
        first.get_cash_balance = AsyncMock(return_value=350.0)  # type: ignore[method-assign]
        second.get_cash_balance = AsyncMock(return_value=350.0)  # type: ignore[method-assign]
        router = ExecutionRouter(
            replace(make_config(False), shadow_mode=True),
            first,
            second,
            FakeTelegram(),
        )

        await router.start()

        first.get_cash_balance.assert_awaited_once()
        second.get_cash_balance.assert_awaited_once()
        self.assertEqual(router._balance_cache, {"Polymarket": 350.0, "Predict.fun": 350.0})
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
        shared_balances: dict[str, Decimal | float] = {}
        reservations: dict[str, Decimal | float] = {}
        optimistic_debits: dict[str, Decimal | float] = {}
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
        self.assertEqual(optimistic_debits, {"Polymarket": Decimal("50"), "Predict.fun": Decimal("50")})
        first.cash_balance = 30.0
        second.cash_balance = 30.0
        await router._refresh_balances()
        self.assertEqual(optimistic_debits, {})
        await router.close()

    async def test_runtime_balance_state_snapshot_exposes_effective_and_available_balances(self) -> None:
        router = ExecutionRouter(
            make_config(False),
            FakeBinaryClient(),
            FakeBinaryClient(),
            FakeTelegram(),
            balance_cache={"Polymarket": 350.0, "Predict.fun": 330.0},
            capital_reservations={"Predict.fun": 15.0},
            optimistic_debits={"Polymarket": 25.0, "Predict.fun": 40.0},
        )

        snapshot: dict[str, Any] = router._runtime_balance_state_snapshot()  # noqa: SLF001

        assert snapshot["venues"]["Polymarket"]["effective_balance_usd"] == "325.0"
        assert snapshot["venues"]["Polymarket"]["available_after_reservations_usd"] == "325.0"
        assert snapshot["venues"]["Predict.fun"]["effective_balance_usd"] == "290.0"
        assert snapshot["venues"]["Predict.fun"]["available_after_reservations_usd"] == "275.0"
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

    async def test_shared_entry_coordinator_serializes_entries_across_routes(self) -> None:
        first_entry_started = asyncio.Event()
        release_first_entry = asyncio.Event()

        class BlockingEntryClient(FakeBinaryClient):
            async def buy(self, *args: Any, **kwargs: Any) -> str:
                first_entry_started.set()
                await release_first_entry.wait()
                return await super().buy(*args, **kwargs)

        config = replace(
            make_config(False),
            execution_mode=ExecutionMode.CANARY,
            _execution_mode_explicit=True,
            max_open_positions=5,
            max_total_notional_usd=500,
            max_venue_exposure_usd=250,
            max_market_exposure_usd=100,
        )
        shared_coordinator = EntrySubmissionCoordinator()
        ledger = PositionLedger()
        market_locks: dict[str, asyncio.Lock] = {}
        capacity_lock = asyncio.Lock()
        pending_markets: set[str] = set()
        first_clients = (BlockingEntryClient(), FakeBinaryClient())
        second_clients = (FakeBinaryClient(), FakeBinaryClient())
        for client in (*first_clients, *second_clients):
            client.fill_result = True
            client.cash_balance = 1000.0
        router_a = ExecutionRouter(
            config,
            first_clients[0],
            first_clients[1],
            FakeTelegram(),
            ledger,
            market_locks=market_locks,
            capacity_lock=capacity_lock,
            pending_markets=pending_markets,
            entry_submission_coordinator=shared_coordinator,
        )
        router_b = ExecutionRouter(
            config,
            second_clients[0],
            second_clients[1],
            FakeTelegram(),
            ledger,
            market_locks=market_locks,
            capacity_lock=capacity_lock,
            pending_markets=pending_markets,
            entry_submission_coordinator=shared_coordinator,
        )
        first_signal = make_verified_signal()
        second_signal = replace(
            first_signal,
            market=replace(
                first_signal.market,
                symbol="ETH-USD",
                polymarket_token_id="poly-eth",
                predict_fun_token_id="predict-eth",
            ),
        )

        first_task = asyncio.create_task(router_a.handle_signal(first_signal))
        await asyncio.wait_for(first_entry_started.wait(), timeout=1.0)
        second_task = asyncio.create_task(router_b.handle_signal(second_signal))
        await asyncio.sleep(0.05)
        self.assertFalse(second_clients[0].bought)
        self.assertFalse(second_clients[1].bought)

        release_first_entry.set()
        await asyncio.gather(first_task, second_task)

        self.assertEqual(len(ledger.all()), 2)
        self.assertTrue(second_clients[0].bought)
        self.assertTrue(second_clients[1].bought)
        await router_a.close()
        await router_b.close()

    async def test_shared_entry_order_budget_applies_across_routes(self) -> None:
        config = replace(
            make_config(False),
            execution_mode=ExecutionMode.CANARY,
            _execution_mode_explicit=True,
            max_orders_per_minute=2,
            max_open_positions=5,
            max_total_notional_usd=500,
            max_venue_exposure_usd=250,
            max_market_exposure_usd=100,
        )
        coordinator = EntrySubmissionCoordinator()
        ledger = PositionLedger()
        first_clients = (FakeBinaryClient(), FakeBinaryClient())
        second_clients = (FakeBinaryClient(), FakeBinaryClient())
        for client in (*first_clients, *second_clients):
            client.fill_result = True
            client.cash_balance = 1000.0
        router_a = ExecutionRouter(
            config,
            first_clients[0],
            first_clients[1],
            FakeTelegram(),
            ledger,
            entry_submission_coordinator=coordinator,
        )
        router_b = ExecutionRouter(
            config,
            second_clients[0],
            second_clients[1],
            FakeTelegram(),
            ledger,
            entry_submission_coordinator=coordinator,
        )
        first_signal = make_verified_signal()
        second_signal = replace(
            first_signal,
            market=replace(
                first_signal.market,
                symbol="ETH-USD",
                polymarket_token_id="poly-eth",
                predict_fun_token_id="predict-eth",
            ),
        )

        await router_a.handle_signal(first_signal)
        await router_b.handle_signal(second_signal)

        self.assertEqual(len(ledger.all()), 1)
        self.assertTrue(first_clients[0].bought)
        self.assertTrue(first_clients[1].bought)
        self.assertFalse(second_clients[0].bought)
        self.assertFalse(second_clients[1].bought)
        await router_a.close()
        await router_b.close()

    async def test_unknown_entry_outcome_pauses_shared_risk_before_next_route_entry(self) -> None:
        statuses: list[OrderIntentStatus] = []

        async def create_order_intent(intent: Any) -> None:
            del intent

        async def update_order_intent(
            client_order_id: str,
            status: OrderIntentStatus,
            **kwargs: Any,
        ) -> None:
            del client_order_id, kwargs
            statuses.append(status)

        async def record_runtime_balance_state(snapshot: Any) -> None:
            del snapshot

        repository = SimpleNamespace(
            create_order_intent=create_order_intent,
            update_order_intent=update_order_intent,
            record_runtime_balance_state=record_runtime_balance_state,
        )
        config = replace(
            make_config(False),
            execution_mode=ExecutionMode.CANARY,
            _execution_mode_explicit=True,
            max_open_positions=5,
            max_total_notional_usd=500,
            max_venue_exposure_usd=250,
            max_market_exposure_usd=100,
        )
        risk = GlobalRiskController(10, 3)
        coordinator = EntrySubmissionCoordinator()
        unknown_client = FakeBinaryClient()
        first_router = ExecutionRouter(
            config,
            unknown_client,
            FakeBinaryClient(),
            FakeTelegram(),
            risk_controller=risk,
            repository=cast(Any, repository),
            entry_submission_coordinator=coordinator,
        )

        result = await first_router._submit_entry_leg(  # noqa: SLF001
            client=unknown_client,
            market=make_verified_market(),
            venue_label="Polymarket",
            token_id="unknown-poly",
            side=BinarySide.YES,
            contracts=Decimal("10"),
            max_price=0.42,
            capital_usd=Decimal("4.2"),
            timeout_ms=500,
        )

        self.assertIsNotNone(result.report)
        self.assertEqual(statuses[-1], OrderIntentStatus.UNKNOWN)
        self.assertTrue(risk.is_paused())

        next_clients = (FakeBinaryClient(), FakeBinaryClient())
        for client in next_clients:
            client.fill_result = True
        next_router = ExecutionRouter(
            config,
            next_clients[0],
            next_clients[1],
            FakeTelegram(),
            risk_controller=risk,
            entry_submission_coordinator=coordinator,
        )
        await next_router.handle_signal(make_verified_signal())

        self.assertFalse(next_clients[0].bought)
        self.assertFalse(next_clients[1].bought)
        await first_router.close()
        await next_router.close()

    async def test_five_distinct_markets_are_allowed_then_duplicate_and_sixth_are_rejected(self) -> None:
        config = replace(
            make_config(False),
            execution_mode=ExecutionMode.CANARY,
            _execution_mode_explicit=True,
            position_size_usd=50,
            max_order_size_usd=50,
            max_open_positions=5,
            max_total_notional_usd=252,
            max_venue_exposure_usd=125,
            max_market_exposure_usd=52,
            max_orders_per_minute=10,
        )
        first = FakeBinaryClient()
        second = FakeBinaryClient()
        first.fill_result = True
        second.fill_result = True
        first.cash_balance = 1000.0
        second.cash_balance = 1000.0
        router = ExecutionRouter(config, first, second, FakeTelegram())
        base_signal = replace(
            make_verified_signal(),
            plan=PositionPlan(
                polymarket_contracts=Decimal("50"),
                polymarket_capital_usd=Decimal("21"),
                predict_fun_contracts=Decimal("50"),
                predict_fun_capital_usd=Decimal("21"),
                payout_contracts=Decimal("50"),
                total_cost_usd=Decimal("42"),
            ),
        )
        signals = [
            replace(
                base_signal,
                market=replace(
                    base_signal.market,
                    symbol=f"MARKET-{index}",
                    polymarket_token_id=f"poly-{index}",
                    predict_fun_token_id=f"predict-{index}",
                ),
            )
            for index in range(6)
        ]

        for signal in signals[:5]:
            await router.handle_signal(signal)
        submitted_after_five = (len(first.buy_tokens), len(second.buy_tokens))
        await router.handle_signal(signals[0])
        await router.handle_signal(signals[5])

        self.assertEqual(len(router.ledger.all()), 5)
        self.assertEqual(submitted_after_five, (5, 5))
        self.assertEqual((len(first.buy_tokens), len(second.buy_tokens)), submitted_after_five)
        await router.close()

    def test_fifth_fee_bearing_position_hits_locked_exposure_boundaries_exactly(self) -> None:
        config = replace(
            make_config(False),
            position_size_usd=50,
            max_order_size_usd=50,
            max_open_positions=5,
            max_total_notional_usd=252,
            max_venue_exposure_usd=125,
            max_market_exposure_usd=52,
        )
        ledger = PositionLedger()
        for index in range(4):
            ledger.add(
                _open_position(
                    market=replace(
                        make_verified_market(),
                        symbol=f"OPEN-{index}",
                        polymarket_token_id=f"open-poly-{index}",
                        predict_fun_token_id=f"open-predict-{index}",
                    ),
                    polymarket_contracts=50,
                    polymarket_entry_price=0.5,
                    predict_fun_contracts=50,
                    predict_fun_entry_price=0.5,
                    opened_at=datetime.now(UTC),
                    polymarket_order_id=f"poly-order-{index}",
                    predict_fun_order_id=f"predict-order-{index}",
                )
            )
        router = ExecutionRouter(config, FakeBinaryClient(), FakeBinaryClient(), FakeTelegram(), ledger)
        signal = replace(
            make_verified_signal(),
            market=replace(
                make_verified_market(),
                symbol="FIFTH",
                polymarket_token_id="fifth-poly",
                predict_fun_token_id="fifth-predict",
            ),
            plan=PositionPlan(
                polymarket_contracts=Decimal("50"),
                polymarket_capital_usd=Decimal("25"),
                predict_fun_contracts=Decimal("50"),
                predict_fun_capital_usd=Decimal("25"),
                payout_contracts=Decimal("50"),
                total_cost_usd=Decimal("52"),
                polymarket_fee_usd=Decimal("1"),
                predict_fun_fee_usd=Decimal("1"),
            ),
        )

        self.assertTrue(router._risk_limits_allow(signal, all_in_cost_usd=Decimal("52")))  # noqa: SLF001
        self.assertFalse(  # noqa: SLF001
            router._risk_limits_allow(signal, all_in_cost_usd=Decimal("52.01"))
        )
        venue_over = replace(
            signal,
            plan=replace(signal.plan, polymarket_capital_usd=Decimal("25.01")),
        )
        self.assertFalse(router._risk_limits_allow(venue_over, all_in_cost_usd=Decimal("52")))  # noqa: SLF001

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
            _open_position(
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

    async def test_25_usd_leg_rejects_book_that_would_move_beyond_best_ask(self) -> None:
        class ThinTopOfBookClient(FakeBinaryClient):
            async def watch_order_book(self, token_id: str) -> OrderBook:
                self.watch_tokens.append(token_id)
                return OrderBook(
                    bids=[OrderBookLevel(0.40, 1000)],
                    asks=[
                        OrderBookLevel(0.42, 50),
                        OrderBookLevel(0.421, 1000),
                    ],
                    timestamp=self.book_timestamp,
                )

        config = replace(
            make_config(False),
            execution_mode=ExecutionMode.CANARY,
            _execution_mode_explicit=True,
            position_size_usd=50,
            max_order_size_usd=50,
            max_open_positions=5,
            max_total_notional_usd=252,
            max_venue_exposure_usd=125,
            max_market_exposure_usd=52,
        )
        first = ThinTopOfBookClient()
        second = FakeBinaryClient()
        first.fill_result = True
        second.fill_result = True
        signal = replace(
            make_verified_signal(),
            plan=PositionPlan(
                polymarket_contracts=Decimal("50"),
                polymarket_capital_usd=Decimal("21"),
                predict_fun_contracts=Decimal("50"),
                predict_fun_capital_usd=Decimal("21"),
                payout_contracts=Decimal("50"),
                total_cost_usd=Decimal("42"),
            ),
        )
        router = ExecutionRouter(config, first, second, FakeTelegram())

        await router.handle_signal(signal)

        self.assertFalse(first.bought)
        self.assertFalse(second.bought)
        self.assertEqual(router.ledger.all(), [])
        await router.close()

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
        position = _open_position(
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

        await router.handle_exit_signal(ExitSignal(position, Decimal("0.5"), Decimal("0.5"), 0.1, Decimal("1.0")))

        self.assertEqual(ready, 2)
        self.assertEqual(ledger.all(), [])

    async def test_exit_submission_exception_preserves_order_id_for_reconciliation(self) -> None:
        class SubmissionUnknown(RuntimeError):
            def __init__(self, order_id: str) -> None:
                self.order_id = order_id
                super().__init__("ack timeout")

        class UnknownExitClient(FakeBinaryClient):
            async def sell(self, *args: Any, **kwargs: Any) -> str:
                token_id = str(kwargs["token_id"])
                contracts = float(kwargs["contracts"])
                min_price = float(kwargs["min_price"])
                order_id = f"digest-{token_id}"
                self.order_amounts[order_id] = contracts
                self.order_prices[order_id] = min_price
                raise SubmissionUnknown(order_id)

        client = UnknownExitClient()
        client.fill_result = True
        router = ExecutionRouter(make_config(False), client, FakeBinaryClient(), FakeTelegram())

        result = await router._submit_exit_leg(  # noqa: SLF001
            client=client,
            market=make_market(),
            venue_label="SX Bet",
            already_closed=False,
            token_id="sx-token",
            side=BinarySide.NO,
            contracts=Decimal("10"),
            min_price=Decimal("0.45"),
            timeout_ms=3600,
        )

        self.assertEqual(result.order_id, "digest-sx-token")
        self.assertIsNotNone(result.report)
        assert result.report is not None
        self.assertEqual(result.report.status, ExecutionStatus.FILLED)
        self.assertTrue(client.cancelled)

    async def test_entry_persists_deterministic_order_id_before_connector_post(self) -> None:
        events: list[tuple[str, str, str | None]] = []
        created_routes: list[str] = []

        class DurableIdClient(FakeBinaryClient):
            async def buy_with_order_id_persistence(self, *args: Any, **kwargs: Any) -> str:
                token_id = str(kwargs["token_id"])
                contracts = float(kwargs["contracts"])
                max_price = float(kwargs["max_price"])
                client_order_id = str(kwargs["client_order_id"])
                order_id = f"digest-{token_id}"
                self.order_amounts[order_id] = contracts
                self.order_prices[order_id] = max_price
                await kwargs["persist_order_id"](order_id)
                events.append(("connector", "client-id", client_order_id))
                events.append(("connector", "post", order_id))
                return order_id

        async def create_order_intent(intent: Any) -> None:
            events.append(("repository", "create", intent.client_order_id))
            created_routes.append(intent.route)

        async def update_order_intent(client_order_id: str, status: Any, **kwargs: Any) -> None:
            del client_order_id
            events.append(("repository", status.value, kwargs.get("venue_order_id")))

        async def record_runtime_balance_state(snapshot: Any) -> None:
            del snapshot

        repository = SimpleNamespace(
            create_order_intent=create_order_intent,
            update_order_intent=update_order_intent,
            record_runtime_balance_state=record_runtime_balance_state,
        )
        client = DurableIdClient()
        client.fill_result = True
        router = ExecutionRouter(
            make_config(False),
            client,
            FakeBinaryClient(),
            FakeTelegram(),
            repository=cast(Any, repository),
        )

        result = await router._submit_entry_leg(  # noqa: SLF001
            client=client,
            market=make_market(),
            venue_label="SX Bet",
            token_id="sx-token",
            side=BinarySide.YES,
            contracts=Decimal("10"),
            max_price=0.45,
            capital_usd=Decimal("4.5"),
            timeout_ms=3600,
        )

        persisted = events.index(("repository", "SUBMITTING", "digest-sx-token"))
        posted = events.index(("connector", "post", "digest-sx-token"))
        created_client_order_id = next(
            value for source, action, value in events if (source, action) == ("repository", "create")
        )
        self.assertLess(persisted, posted)
        self.assertIn(("connector", "client-id", created_client_order_id), events)
        self.assertEqual(created_routes, ["polymarket_predict"])
        self.assertIsNone(result.error)
        self.assertEqual(result.order_id, "digest-sx-token")

    async def test_sx_pre_submit_persistence_rechecks_discovery_generation_before_transport(self) -> None:
        transport_called = False
        venue_id_write_started = asyncio.Event()
        release_venue_id_write = asyncio.Event()
        statuses: list[OrderIntentStatus] = []

        class SxPreSubmitClient(FakeBinaryClient):
            def persists_order_id_before_submission(self) -> bool:
                return True

            async def buy_with_order_id_persistence(self, *args: Any, **kwargs: Any) -> str:
                nonlocal transport_called
                order_id = "digest-sx-pre-submit"
                await kwargs["persist_order_id"](order_id)
                transport_called = True
                return order_id

        async def create_order_intent(intent: Any) -> None:
            del intent

        async def update_order_intent(
            client_order_id: str,
            status: OrderIntentStatus,
            **kwargs: Any,
        ) -> None:
            del client_order_id
            if status is OrderIntentStatus.SUBMITTING and kwargs.get("venue_order_id"):
                venue_id_write_started.set()
                await release_venue_id_write.wait()
            statuses.append(status)

        async def record_runtime_balance_state(snapshot: Any) -> None:
            del snapshot

        repository = SimpleNamespace(
            create_order_intent=create_order_intent,
            update_order_intent=update_order_intent,
            record_runtime_balance_state=record_runtime_balance_state,
        )
        old_market = make_market()
        replacement_market = replace(
            old_market,
            symbol="replacement-market",
            polymarket_token_id="replacement-poly-token",
            predict_fun_token_id="replacement-predict-token",
        )
        registry = ActiveMarketRegistry([old_market])
        expected_generation = registry.generation
        client = SxPreSubmitClient()
        router = ExecutionRouter(
            make_config(False),
            client,
            FakeBinaryClient(),
            FakeTelegram(),
            repository=cast(Any, repository),
        )
        router.set_entry_snapshot_readiness(
            lambda generation: generation is not None
            and registry.ready
            and generation == registry.generation
        )

        submission = asyncio.create_task(
            router._submit_entry_leg(  # noqa: SLF001
                client=client,
                market=old_market,
                venue_label="SX Bet",
                token_id="sx-token",
                side=BinarySide.YES,
                contracts=Decimal("10"),
                max_price=0.45,
                capital_usd=Decimal("4.5"),
                timeout_ms=3600,
                discovery_generation=expected_generation,
            )
        )
        await asyncio.wait_for(venue_id_write_started.wait(), timeout=1.0)
        registry.publish(DiscoveryResult((replacement_market,)))
        self.assertTrue(registry.ready)
        release_venue_id_write.set()
        result = await submission

        self.assertFalse(transport_called)
        self.assertIsInstance(result.error, OrderSubmissionRejected)
        assert result.report is not None
        self.assertEqual(result.report.status, ExecutionStatus.CANCELLED)
        self.assertEqual(statuses[-1], OrderIntentStatus.CANCELLED)
        self.assertNotIn(OrderIntentStatus.UNKNOWN, statuses)

    async def test_sx_pre_transport_guard_rechecks_generation_after_semaphore_wait(self) -> None:
        transport_called = False
        waiting_for_transport = asyncio.Event()
        transport_semaphore = asyncio.Semaphore(0)
        statuses: list[OrderIntentStatus] = []

        class SxSemaphoreClient(FakeBinaryClient):
            def persists_order_id_before_submission(self) -> bool:
                return True

            async def buy_with_order_id_persistence(self, *args: Any, **kwargs: Any) -> str:
                nonlocal transport_called
                order_id = "digest-sx-semaphore"
                await kwargs["persist_order_id"](order_id)
                waiting_for_transport.set()
                async with transport_semaphore:
                    kwargs["pre_transport_guard"]()
                    transport_called = True
                return order_id

        async def create_order_intent(intent: Any) -> None:
            del intent

        async def update_order_intent(
            client_order_id: str,
            status: OrderIntentStatus,
            **kwargs: Any,
        ) -> None:
            del client_order_id, kwargs
            statuses.append(status)

        async def record_runtime_balance_state(snapshot: Any) -> None:
            del snapshot

        repository = SimpleNamespace(
            create_order_intent=create_order_intent,
            update_order_intent=update_order_intent,
            record_runtime_balance_state=record_runtime_balance_state,
        )
        old_market = make_market()
        replacement_market = replace(
            old_market,
            symbol="replacement-market",
            polymarket_token_id="replacement-poly-token",
            predict_fun_token_id="replacement-predict-token",
        )
        registry = ActiveMarketRegistry([old_market])
        expected_generation = registry.generation
        client = SxSemaphoreClient()
        router = ExecutionRouter(
            make_config(False),
            client,
            FakeBinaryClient(),
            FakeTelegram(),
            repository=cast(Any, repository),
        )
        router.set_entry_snapshot_readiness(
            lambda generation: generation is not None
            and registry.ready
            and generation == registry.generation
        )

        submission = asyncio.create_task(
            router._submit_entry_leg(  # noqa: SLF001
                client=client,
                market=old_market,
                venue_label="SX Bet",
                token_id="sx-token",
                side=BinarySide.YES,
                contracts=Decimal("10"),
                max_price=0.45,
                capital_usd=Decimal("4.5"),
                timeout_ms=3600,
                discovery_generation=expected_generation,
            )
        )
        await asyncio.wait_for(waiting_for_transport.wait(), timeout=1.0)
        registry.publish(DiscoveryResult((replacement_market,)))
        self.assertTrue(registry.ready)
        transport_semaphore.release()
        result = await submission

        self.assertFalse(transport_called)
        self.assertIsInstance(result.error, OrderSubmissionRejected)
        assert result.report is not None
        self.assertEqual(result.report.status, ExecutionStatus.CANCELLED)
        self.assertEqual(statuses[-1], OrderIntentStatus.CANCELLED)
        self.assertNotIn(OrderIntentStatus.UNKNOWN, statuses)

    async def test_pre_transport_guard_blocks_deadline_crossed_during_connector_wait(self) -> None:
        transport_called = False
        waiting_for_transport = asyncio.Event()
        transport_semaphore = asyncio.Semaphore(0)
        statuses: list[OrderIntentStatus] = []

        class WaitingClient(FakeBinaryClient):
            def persists_order_id_before_submission(self) -> bool:
                return True

            async def buy_with_order_id_persistence(self, *args: Any, **kwargs: Any) -> str:
                nonlocal transport_called
                order_id = "digest-deadline-wait"
                await kwargs["persist_order_id"](order_id)
                waiting_for_transport.set()
                async with transport_semaphore:
                    kwargs["pre_transport_guard"]()
                    transport_called = True
                return order_id

        async def create_order_intent(intent: Any) -> None:
            del intent

        async def update_order_intent(
            client_order_id: str,
            status: OrderIntentStatus,
            **kwargs: Any,
        ) -> None:
            del client_order_id, kwargs
            statuses.append(status)

        async def record_runtime_balance_state(snapshot: Any) -> None:
            del snapshot

        repository = SimpleNamespace(
            create_order_intent=create_order_intent,
            update_order_intent=update_order_intent,
            record_runtime_balance_state=record_runtime_balance_state,
        )
        client = WaitingClient()
        router = ExecutionRouter(
            make_config(False),
            client,
            FakeBinaryClient(),
            FakeTelegram(),
            repository=cast(Any, repository),
        )
        router._funded_canary_deadline_unix = time.time() + 60  # noqa: SLF001

        submission = asyncio.create_task(
            router._submit_entry_leg(  # noqa: SLF001
                client=client,
                market=make_market(),
                venue_label="Predict.fun",
                token_id="predict-token",
                side=BinarySide.YES,
                contracts=Decimal("10"),
                max_price=0.45,
                capital_usd=Decimal("4.5"),
                timeout_ms=3600,
            )
        )
        await asyncio.wait_for(waiting_for_transport.wait(), timeout=1.0)
        router._funded_canary_deadline_unix = time.time() - 1  # noqa: SLF001
        transport_semaphore.release()
        result = await submission

        self.assertFalse(transport_called)
        self.assertIsInstance(result.error, OrderSubmissionRejected)
        assert result.report is not None
        self.assertEqual(result.report.status, ExecutionStatus.CANCELLED)
        self.assertEqual(statuses[-1], OrderIntentStatus.CANCELLED)
        self.assertNotIn(OrderIntentStatus.UNKNOWN, statuses)
        self.assertTrue(router.is_paused)
        self.assertEqual(router._risk.pause_reason, "funded_canary_window_complete")  # noqa: SLF001

    async def test_definitive_pre_submission_rejection_is_cancelled_not_unknown(self) -> None:
        statuses: list[OrderIntentStatus] = []

        class RejectedClient(FakeBinaryClient):
            async def buy_with_order_id_persistence(self, *args: Any, **kwargs: Any) -> str:
                order_id = "digest-rejected"
                await kwargs["persist_order_id"](order_id)
                raise OrderSubmissionRejected("cutoff elapsed", order_id=order_id)

        async def create_order_intent(intent: Any) -> None:
            del intent

        async def update_order_intent(client_order_id: str, status: OrderIntentStatus, **kwargs: Any) -> None:
            del client_order_id, kwargs
            statuses.append(status)

        async def record_runtime_balance_state(snapshot: Any) -> None:
            del snapshot

        repository = SimpleNamespace(
            create_order_intent=create_order_intent,
            update_order_intent=update_order_intent,
            record_runtime_balance_state=record_runtime_balance_state,
        )
        client = RejectedClient()
        router = ExecutionRouter(
            make_config(False),
            client,
            FakeBinaryClient(),
            FakeTelegram(),
            repository=cast(Any, repository),
        )

        result = await router._submit_entry_leg(  # noqa: SLF001
            client=client,
            market=make_market(),
            venue_label="SX Bet",
            token_id="sx-token",
            side=BinarySide.YES,
            contracts=Decimal("10"),
            max_price=0.45,
            capital_usd=Decimal("4.5"),
            timeout_ms=3600,
        )

        self.assertIsInstance(result.error, OrderSubmissionRejected)
        assert result.report is not None
        self.assertEqual(result.report.status, ExecutionStatus.CANCELLED)
        self.assertEqual(statuses[-1], OrderIntentStatus.CANCELLED)
        self.assertNotIn(OrderIntentStatus.UNKNOWN, statuses)
        self.assertFalse(client.cancelled)
        self.assertFalse(router.is_paused)

    def test_open_execution_report_with_provisional_fill_stays_unresolved(self) -> None:
        report = ExecutionReport(
            order_id="order-open",
            status=ExecutionStatus.OPEN,
            amount_requested=Decimal("10"),
            amount_filled=Decimal("4"),
            remaining_amount=Decimal("6"),
            avg_price=Decimal("0.45"),
        )

        self.assertEqual(_intent_status_from_report(report), OrderIntentStatus.UNKNOWN)

    async def test_generic_connector_keeps_submitted_id_when_durable_write_fails(self) -> None:
        failed_once = False

        async def create_order_intent(intent: Any) -> None:
            del intent

        async def update_order_intent(client_order_id: str, status: Any, **kwargs: Any) -> None:
            nonlocal failed_once
            del client_order_id, status
            if kwargs.get("venue_order_id") and not failed_once:
                failed_once = True
                raise RuntimeError("simulated persistence failure")

        async def record_runtime_balance_state(snapshot: Any) -> None:
            del snapshot

        repository = SimpleNamespace(
            create_order_intent=create_order_intent,
            update_order_intent=update_order_intent,
            record_runtime_balance_state=record_runtime_balance_state,
        )
        client = FakeBinaryClient()
        router = ExecutionRouter(
            make_config(False),
            client,
            FakeBinaryClient(),
            FakeTelegram(),
            repository=cast(Any, repository),
        )

        result = await router._submit_entry_leg(  # noqa: SLF001
            client=client,
            market=make_market(),
            venue_label="Generic",
            token_id="generic-token",
            side=BinarySide.YES,
            contracts=Decimal("10"),
            max_price=0.45,
            capital_usd=Decimal("4.5"),
            timeout_ms=3600,
        )

        self.assertEqual(result.order_id, "buy-generic-token")
        self.assertIsInstance(result.error, RuntimeError)
        self.assertTrue(client.cancelled)

    async def test_invalid_zero_cost_position_does_not_stop_monitoring_cycle(self) -> None:
        first = FakeBinaryClient()
        second = FakeBinaryClient()
        ledger = PositionLedger()
        ledger.add(
            _open_position(
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
        self.assertEqual(report.avg_price, Decimal("0.42"))

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
        router = ExecutionRouter(
            make_config(False),
            poly,
            predict,
            telegram,
            chain_cost_estimator=_zero_chain_cost_estimator(),
        )
        signal = ArbitrageSignal(
            market=make_market(),
            plan=PositionPlan(*(Decimal(value) for value in ("100", "51", "100", "44.5", "100", "95.5"))),
            metrics=SpreadMetrics(0.11, 0.11, 11, 0, 0, 0.89),
            polymarket_price=0.51,
            predict_fun_price=0.445,
        )

        await asyncio.wait_for(router.handle_signal(signal), timeout=1.0)

        self.assertFalse(poly.bought)
        self.assertFalse(predict.bought)
        self.assertFalse(poly.sold)

    async def test_signed_preview_rejects_edge_that_only_exists_before_limit_slippage(self) -> None:
        first = FakeBinaryClient()
        second = FakeBinaryClient()
        first.ask = 0.48
        second.ask = 0.48
        config = make_config(False)
        config = replace(
            config,
            position_size_usd=20,
            max_order_size_usd=20,
            min_net_spread=0.03,
            min_entry_spread_pct=0.03,
            spread_guard_floor=0.03,
            spread_policy=replace(
                config.spread_policy,
                route_floors={"polymarket_predict": 0.03},
                adverse_move_p95_pct=0.0,
                adverse_move_p95_pct_by_route={"polymarket_predict": 0.0},
                safety_buffer_pct=0.0,
                min_expected_profit_usd=0.50,
            ),
        )
        signal = replace(
            make_verified_signal(0.04),
            polymarket_price=0.48,
            predict_fun_price=0.48,
        )
        router = ExecutionRouter(
            config,
            first,
            second,
            FakeTelegram(),
            chain_cost_estimator=_zero_chain_cost_estimator(),
        )

        with self.assertLogs("arbitrage_engine.execution", level="WARNING") as captured:
            await router.handle_signal(signal)

        rejection = next(record for record in captured.records if record.msg == "preflight_price_guard_rejected")
        rejection_extra: Any = rejection
        self.assertIn(
            "signed_preview_worst_case_net_spread_below_dynamic_threshold",
            rejection_extra._reason,
        )
        self.assertFalse(first.bought)
        self.assertFalse(second.bought)

    async def test_signed_preview_uses_guaranteed_fixed_stake_payout(self) -> None:
        class FixedStakePreviewClient(FakeBinaryClient):
            async def _preview_buy_from_book(self, *args: Any, **kwargs: Any) -> OrderPreview:
                preview = await super()._preview_buy_from_book(*args, **kwargs)
                return replace(
                    preview,
                    guaranteed_contracts=preview.requested_contracts * Decimal("0.8"),
                    maximum_notional_usd=preview.notional_usd,
                    maximum_fee_usd=Decimal(0),
                )

        first = FixedStakePreviewClient()
        second = FakeBinaryClient()
        first.ask = 0.40
        second.ask = 0.40
        config = make_config(False)
        config = replace(
            config,
            spread_policy=replace(
                config.spread_policy,
                route_floors={"polymarket_predict": 0.10},
                adverse_move_p95_pct=0.0,
                adverse_move_p95_pct_by_route={"polymarket_predict": 0.0},
                safety_buffer_pct=0.0,
            ),
        )
        router = ExecutionRouter(
            config,
            first,
            second,
            FakeTelegram(),
            chain_cost_estimator=_zero_chain_cost_estimator(),
        )

        with self.assertLogs("arbitrage_engine.execution", level="WARNING") as captured:
            await router.handle_signal(make_signal(0.20))

        rejection = next(record for record in captured.records if record.msg == "preflight_price_guard_rejected")
        rejection_extra: Any = rejection
        self.assertIn(
            "signed_preview_worst_case_net_spread_below_dynamic_threshold",
            rejection_extra._reason,
        )
        self.assertFalse(first.bought)
        self.assertFalse(second.bought)

    async def test_minimum_profit_guard_rejects_both_legs_at_preflight(self) -> None:
        first = FakeBinaryClient()
        first.ask = 0.49
        second = FakeBinaryClient()
        second.ask = 0.49
        config = make_config(False)
        config = replace(
            config,
            position_size_usd=20,
            max_order_size_usd=20,
            min_net_spread=0.01,
            min_entry_spread_pct=0.01,
            spread_guard_floor=0.01,
            spread_policy=replace(
                config.spread_policy,
                route_floors={"polymarket_predict": 0.01},
                safety_buffer_pct=0.0,
                min_expected_profit_usd=0.50,
            ),
        )
        router = ExecutionRouter(config, first, second, FakeTelegram())

        await router.handle_signal(make_signal(0.02))

        self.assertFalse(first.bought)
        self.assertFalse(second.bought)
        self.assertFalse(first.sold)
        self.assertFalse(second.sold)

    async def test_preflight_liquidity_analysis_logs_pass_payload(self) -> None:
        poly = FakeBinaryClient()
        predict = FakeBinaryClient()
        accepted_routes: list[str] = []
        router = ExecutionRouter(
            replace(make_config(False), position_size_usd=20, min_retry_spread_pct=0.05, min_net_spread=0.05),
            poly,
            predict,
            FakeTelegram(),
        )
        router.set_accepted_preflight_observer(accepted_routes.append)

        with self.assertLogs("arbitrage_engine.execution", level="INFO") as captured:
            allowed = await router._preflight_price_guard(make_signal())

        self.assertTrue(allowed)
        assert allowed is not None
        self.assertIn("all_in_cost_usd", allowed.evidence["economics"])
        record = next(record for record in captured.records if record.msg == "preflight_liquidity_analysis")
        record_extra: Any = record
        self.assertEqual(record_extra._route, "polymarket_predict")
        self.assertEqual(record_extra._target_notional_per_leg_usd, 10.0)
        self.assertAlmostEqual(record_extra._first_best_ask, 0.42)
        self.assertAlmostEqual(record_extra._first_avg_fill, 0.42)
        self.assertAlmostEqual(record_extra._second_avg_fill, 0.42)
        self.assertGreater(record_extra._current_net_spread, 0.05)
        self.assertEqual(accepted_routes, ["polymarket_predict"])

    async def test_preflight_liquidity_rejected_logs_reason_for_insufficient_depth(self) -> None:
        class ThinBookClient(FakeBinaryClient):
            async def watch_order_book(self, token_id: str) -> OrderBook:
                self.watch_tokens.append(token_id)
                return OrderBook(
                    bids=[OrderBookLevel(self.bid, 1000)],
                    asks=[OrderBookLevel(self.ask, 5)],
                    timestamp=self.book_timestamp,
                )

        poly = ThinBookClient()
        predict = ThinBookClient()
        router = ExecutionRouter(
            replace(make_config(False), position_size_usd=20, min_retry_spread_pct=0.05, min_net_spread=0.05),
            poly,
            predict,
            FakeTelegram(),
        )

        with self.assertLogs("arbitrage_engine.execution", level="WARNING") as captured:
            allowed = await router._preflight_price_guard(make_signal())

        self.assertFalse(allowed)
        record = next(record for record in captured.records if record.msg == "preflight_liquidity_rejected")
        record_extra: Any = record
        self.assertEqual(record_extra._target_notional_per_leg_usd, 10.0)
        self.assertEqual(record_extra._reason, "top_of_book_depth_below_no_impact_buffer")

    async def test_preflight_signed_preview_rejection_logs_safe_leg_specific_blocker(self) -> None:
        poly = CountingPreviewClient(fail_on_signature_call=1)
        predict = CountingPreviewClient(fail_on_signature_call=1)
        router = ExecutionRouter(
            replace(make_config(False), position_size_usd=20, min_retry_spread_pct=0.05, min_net_spread=0.05),
            poly,
            predict,
            FakeTelegram(),
        )

        with self.assertLogs("arbitrage_engine.execution", level="WARNING") as captured:
            allowed = await router._preflight_price_guard(make_signal())

        self.assertFalse(allowed)
        record = next(record for record in captured.records if record.msg == "preflight_price_guard_rejected")
        record_extra: Any = record
        self.assertEqual(record_extra._route, "polymarket_predict")
        self.assertIn("signed_pre_submit_preview_rejected", record_extra._reason)
        self.assertIn("first_leg_preview:signature_preview_unavailable", record_extra._reason)
        self.assertEqual(record_extra._first_preview_blockers, "signature_preview_unavailable")
        self.assertFalse(record_extra._first_preview_executable)
        self.assertFalse(record_extra._first_preview_signing_validated)
        self.assertEqual(record_extra._second_preview_blockers, "signature_preview_unavailable")
        self.assertFalse(record_extra._second_preview_executable)
        self.assertIn("second_leg_preview:signature_preview_unavailable", record_extra._reason)
        self.assertNotIn("test-signed-preview", record_extra._reason)
        self.assertNotIn("test-signed-preview", record_extra._first_preview_blockers)

    async def test_preflight_rejects_even_sub_epsilon_positive_signed_price_impact(self) -> None:
        class TinyImpactClient(FakeBinaryClient):
            async def preview_buy(self, *args: Any, **kwargs: Any) -> OrderPreview:
                preview = await super().preview_buy(*args, **kwargs)
                return replace(preview, price_impact_pct=Decimal("1e-19"))

        first = TinyImpactClient()
        second = TinyImpactClient()
        router = ExecutionRouter(
            replace(make_config(False), position_size_usd=20, min_retry_spread_pct=0.05, min_net_spread=0.05),
            first,
            second,
            FakeTelegram(),
        )

        with self.assertLogs("arbitrage_engine.execution", level="WARNING") as captured:
            allowed = await router._preflight_price_guard(make_signal())

        self.assertIsNone(allowed)
        rejection = next(record for record in captured.records if record.msg == "preflight_price_guard_rejected")
        rejection_extra: Any = rejection
        self.assertIn("first_leg_signed_preview_price_impact_nonzero", rejection_extra._reason)
        self.assertIn("second_leg_signed_preview_price_impact_nonzero", rejection_extra._reason)

    def test_safe_preview_blockers_does_not_invent_signature_failure_or_expose_unknown_text(self) -> None:
        preview = OrderPreview(
            venue="Test",
            token_id="test-token",
            side=BinarySide.YES,
            requested_contracts=Decimal("10"),
            limit_price=Decimal("0.50"),
            average_price=Decimal("0.50"),
            notional_usd=Decimal("5"),
            available_depth_usd=Decimal("1"),
            price_impact_pct=Decimal(0),
            expected_fee_usd=Decimal(0),
            fee_quote=VenueFeeQuote("Test", 0, "zero_fee", source="test", verified=True),
            constraints=MarketConstraints(0, Decimal("0.01"), Decimal("0.01"), Decimal("1")),
            signing_validated=False,
            blockers=("insufficient_executable_depth", "private_key=do-not-log"),
        )

        blockers = _safe_preview_blockers(preview)

        self.assertEqual(blockers, ("insufficient_executable_depth", "unknown_preview_blocker"))
        self.assertNotIn("signature_preview_unavailable", blockers)
        self.assertNotIn("do-not-log", ",".join(blockers))

    async def test_partial_second_leg_unwinds_only_unmatched_delta(self) -> None:
        poly = FakeBinaryClient()
        poly.fill_results = [True, True]
        predict = FakeBinaryClient()
        predict.partial_fill_results = [40.0]
        ledger = PositionLedger()
        router = ExecutionRouter(make_config(False), poly, predict, FakeTelegram(), ledger)

        await router.handle_signal(make_signal())

        self.assertTrue(predict.cancelled)
        submitted = poly.order_amounts["buy-poly-token"]
        self.assertEqual(poly.sell_contracts, [submitted - 40.0])
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
                plan=PositionPlan(*(Decimal(value) for value in ("100", "42", "100", "47", "100", "89"))),
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
            _open_position(
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
        self.assertEqual(telegram.messages, 2)

    async def test_engine_evaluates_predict_and_sx_route_families_in_same_cycle(self) -> None:
        poly = FakeBinaryClient()
        poly.ask = 0.40
        predict = FakeBinaryClient()
        predict.ask = 0.45
        sx = FakeBinaryClient()
        sx.ask = 0.45
        myriad = FakeBinaryClient()
        myriad.ask = 0.44
        telegram = FakeTelegram()
        ledger = PositionLedger()
        predict_market = replace(make_market(), myriad_market_id="123", myriad_side=BinarySide.NO)
        sx_market = replace(
            make_market(datetime.now(UTC) + timedelta(hours=1)),
            symbol="ETH-USD",
            polymarket_token_id="poly-sx",
            predict_fun_token_id="sx-token",
            predict_fun_market_id="0xsxmarket",
            venue_b_label="SX Bet",
            myriad_market_id="456",
            myriad_side=BinarySide.NO,
        )
        config = make_config(True)
        config = replace(
            config,
            enable_sx_bet=True,
            sx_bet=replace(config.sx_bet, enabled=True),
            myriad_markets=replace(config.myriad_markets, enabled=True),
            markets=[predict_market, sx_market],
        )
        poly_predict = ExecutionRouter(config, poly, predict, telegram, ledger)
        poly_sx = ExecutionRouter(
            config,
            poly,
            sx,
            telegram,
            ledger,
            second_leg_label="SX Bet",
            second_leg_fill_timeout_ms=config.sx_bet_fill_timeout_ms,
        )
        poly_myriad = ExecutionRouter(config, poly, myriad, telegram, ledger, second_leg_label="Myriad")
        predict_myriad = ExecutionRouter(
            config,
            predict,
            myriad,
            telegram,
            ledger,
            first_leg_label="Predict.fun",
            second_leg_label="Myriad",
            first_leg_fill_timeout_ms=config.predict_fun_fill_timeout_ms,
            second_leg_fill_timeout_ms=config.myriad_fill_timeout_ms,
        )
        sx_myriad = ExecutionRouter(
            config,
            sx,
            myriad,
            telegram,
            ledger,
            first_leg_label="SX Bet",
            second_leg_label="Myriad",
            first_leg_fill_timeout_ms=config.sx_bet_fill_timeout_ms,
            second_leg_fill_timeout_ms=config.myriad_fill_timeout_ms,
        )
        engine = ArbitrageEngine(
            replace(
                config,
                routes=replace(
                    config.routes,
                    polymarket_myriad=True,
                    polymarket_predict=True,
                    predict_myriad=True,
                    polymarket_sx=True,
                    sx_myriad=True,
                ),
            ),
            poly,
            predict,
            poly_predict,
            sx_bet=sx,
            sx_execution=poly_sx,
            myriad=myriad,
            myriad_execution=poly_myriad,
            predict_myriad_execution=predict_myriad,
            sx_myriad_execution=sx_myriad,
        )

        await engine.run_once()

        self.assertIn("predict-token", predict.watch_tokens)
        self.assertIn("sx-token", sx.watch_tokens)
        self.assertIn("123:NO", myriad.watch_tokens)
        self.assertIn("123:YES", myriad.watch_tokens)
        self.assertIn("456:YES", myriad.watch_tokens)
        self.assertTrue(any(tokens == {"poly-token", "poly-sx"} for tokens in poly.synced_targets))
        self.assertTrue(any(tokens == {"predict-token"} for tokens in predict.synced_targets))
        self.assertTrue(any(tokens == {"sx-token"} for tokens in sx.synced_targets))
        self.assertEqual(telegram.messages, 4)

    async def test_sx_route_family_keeps_own_fill_timeout_knob(self) -> None:
        config = replace(make_config(True), sx_bet_fill_timeout_ms=6789)
        poly = FakeBinaryClient()
        sx = FakeBinaryClient()
        myriad = FakeBinaryClient()
        poly_sx_router = ExecutionRouter(
            config,
            poly,
            sx,
            FakeTelegram(),
            second_leg_label="SX Bet",
            second_leg_fill_timeout_ms=config.sx_bet_fill_timeout_ms,
        )
        sx_myriad_router = ExecutionRouter(
            config,
            sx,
            myriad,
            FakeTelegram(),
            first_leg_label="SX Bet",
            second_leg_label="Myriad",
            first_leg_fill_timeout_ms=config.sx_bet_fill_timeout_ms,
            second_leg_fill_timeout_ms=config.myriad_fill_timeout_ms,
        )

        self.assertEqual(poly_sx_router._second_leg_fill_timeout_ms, 6789)
        self.assertEqual(sx_myriad_router._first_leg_fill_timeout_ms, 6789)
        self.assertEqual(sx_myriad_router._second_leg_fill_timeout_ms, config.myriad_fill_timeout_ms)

    async def test_predict_myriad_auto_close_watches_route_specific_books(self) -> None:
        predict = FakeBinaryClient()
        myriad = FakeBinaryClient()
        telegram = FakeTelegram()
        config = make_config(True)
        market = MarketSpec(
            symbol="Predict-Myriad",
            target_label="YES",
            venue_a_label="Predict.fun",
            venue_b_label="Myriad",
            polymarket_token_id="predict-token",
            polymarket_side=BinarySide.NO,
            predict_fun_token_id="123:YES",
            predict_fun_side=BinarySide.YES,
            myriad_market_id="123",
            myriad_side=BinarySide.NO,
            expires_at=datetime.now(UTC) + timedelta(minutes=30),
        )
        position = _open_position(
            market=market,
            polymarket_contracts=100,
            polymarket_entry_price=0.45,
            predict_fun_contracts=100,
            predict_fun_entry_price=0.47,
            opened_at=datetime.now(UTC),
            polymarket_order_id="predict-entry",
            predict_fun_order_id="myriad-entry",
        )
        ledger = PositionLedger()
        ledger.add(position)
        router = ExecutionRouter(
            config,
            predict,
            myriad,
            telegram,
            ledger,
            first_leg_label="Predict.fun",
            second_leg_label="Myriad",
            first_leg_fill_timeout_ms=config.predict_fun_fill_timeout_ms,
            second_leg_fill_timeout_ms=config.myriad_fill_timeout_ms,
        )
        manager = PositionManager(
            config=config,
            polymarket=FakeBinaryClient(),
            predict_fun=predict,
            execution=None,
            myriad=myriad,
            predict_myriad_execution=router,
            ledger=ledger,
        )

        await manager.run_once()

        self.assertEqual(predict.watch_tokens, ["predict-token"])
        self.assertEqual(myriad.watch_tokens, ["123:YES"])
        self.assertEqual(telegram.messages, 1)

    async def test_shadow_signal_requires_three_consecutive_signed_preflights(self) -> None:
        poly = CountingPreviewClient()
        predict = CountingPreviewClient()
        telegram = FakeTelegram()
        observed: list[tuple[str, str]] = []
        config = replace(
            make_config(True),
            position_size_usd=20,
            min_net_spread=0.05,
            shadow_preflight_samples=3,
            shadow_preflight_sample_interval_seconds=0,
            shadow_preflight_cooldown_seconds=0,
        )
        router = ExecutionRouter(
            config,
            poly,
            predict,
            telegram,
            shadow_preflight_observer=lambda route, outcome: observed.append((route, outcome)),
        )

        await router.handle_signal(make_verified_signal())

        self.assertEqual(poly.preview_signature_calls, 3)
        self.assertEqual(predict.preview_signature_calls, 3)
        self.assertEqual(observed, [("polymarket_predict", "evidence_passed")])
        self.assertEqual(telegram.messages, 1)
        self.assertFalse(poly.bought)
        self.assertFalse(predict.bought)

    async def test_shadow_signal_persists_exact_release_evidence_after_all_samples(self) -> None:
        repository = SimpleNamespace(record_shadow_preflight_evidence=AsyncMock())
        config = replace(
            make_config(True),
            position_size_usd=20,
            min_net_spread=0.05,
            shadow_preflight_samples=3,
            shadow_preflight_sample_interval_seconds=0,
            shadow_preflight_cooldown_seconds=0,
            runtime_instance_id="quote_arb",
        )
        with patch.dict("os.environ", {"CI_VERIFIED_COMMIT_SHA": "a" * 40}):
            router = ExecutionRouter(
                config,
                CountingPreviewClient(),
                CountingPreviewClient(),
                FakeTelegram(),
                repository=cast(Any, repository),
            )

        await router.handle_signal(make_verified_signal())

        repository.record_shadow_preflight_evidence.assert_awaited_once()
        evidence = repository.record_shadow_preflight_evidence.await_args.args[0]
        self.assertEqual(evidence["release_sha"], "a" * 40)
        self.assertEqual(evidence["runtime_instance_id"], "quote_arb")
        self.assertEqual(evidence["route"], "polymarket_predict")
        self.assertEqual(evidence["completed_samples"], 3)
        self.assertTrue(all(sample["signed_preview_validated"] for sample in evidence["samples"]))
        self.assertTrue(
            all(Decimal(sample["economics"]["expected_profit_usd"]) >= Decimal("0.5") for sample in evidence["samples"])
        )

    async def test_shadow_signal_does_not_persist_unverified_route_evidence(self) -> None:
        repository = SimpleNamespace(record_shadow_preflight_evidence=AsyncMock())
        poly = CountingPreviewClient()
        predict = CountingPreviewClient()
        observed: list[tuple[str, str]] = []
        config = replace(
            make_config(True),
            position_size_usd=20,
            min_net_spread=0.05,
            shadow_preflight_samples=3,
            shadow_preflight_sample_interval_seconds=0,
            shadow_preflight_cooldown_seconds=0,
        )
        router = ExecutionRouter(
            config,
            poly,
            predict,
            FakeTelegram(),
            repository=cast(Any, repository),
            shadow_preflight_observer=lambda route, outcome: observed.append((route, outcome)),
        )
        signal = make_signal()
        signal = replace(
            signal,
            market=replace(
                signal.market,
                mapping_status=MappingStatus.CANDIDATE,
                verified_routes=frozenset(),
            ),
        )

        await router.handle_signal(signal)

        repository.record_shadow_preflight_evidence.assert_not_awaited()
        self.assertEqual(poly.preview_signature_calls, 3)
        self.assertEqual(predict.preview_signature_calls, 3)
        self.assertEqual(observed, [("polymarket_predict", "route_not_verified")])

    async def test_shadow_signal_rejects_when_any_signed_preflight_sample_fails(self) -> None:
        poly = CountingPreviewClient(fail_on_signature_call=2)
        predict = CountingPreviewClient()
        telegram = FakeTelegram()
        observed: list[tuple[str, str]] = []
        config = replace(
            make_config(True),
            position_size_usd=20,
            min_net_spread=0.05,
            shadow_preflight_samples=3,
            shadow_preflight_sample_interval_seconds=0,
            shadow_preflight_cooldown_seconds=0,
        )
        router = ExecutionRouter(
            config,
            poly,
            predict,
            telegram,
            shadow_preflight_observer=lambda route, outcome: observed.append((route, outcome)),
        )

        await router.handle_signal(make_signal())

        self.assertEqual(poly.preview_signature_calls, 2)
        self.assertEqual(predict.preview_signature_calls, 2)
        self.assertEqual(observed, [("polymarket_predict", "sample_rejected")])
        self.assertEqual(telegram.messages, 0)
        self.assertFalse(poly.bought)
        self.assertFalse(predict.bought)

    async def test_shadow_preflight_cooldown_prevents_repeated_signed_work(self) -> None:
        poly = CountingPreviewClient()
        predict = CountingPreviewClient()
        observed: list[tuple[str, str]] = []
        config = replace(
            make_config(True),
            position_size_usd=20,
            min_net_spread=0.05,
            shadow_preflight_samples=3,
            shadow_preflight_sample_interval_seconds=0,
            shadow_preflight_cooldown_seconds=300,
        )
        router = ExecutionRouter(
            config,
            poly,
            predict,
            FakeTelegram(),
            shadow_preflight_observer=lambda route, outcome: observed.append((route, outcome)),
        )

        await router.handle_signal(make_verified_signal())
        await router.handle_signal(make_verified_signal())

        self.assertEqual(poly.preview_signature_calls, 3)
        self.assertEqual(predict.preview_signature_calls, 3)
        self.assertEqual(
            observed,
            [
                ("polymarket_predict", "evidence_passed"),
                ("polymarket_predict", "cooldown_skipped"),
            ],
        )

    async def test_sx_myriad_auto_close_watches_route_specific_books(self) -> None:
        sx = FakeBinaryClient()
        myriad = FakeBinaryClient()
        telegram = FakeTelegram()
        config = make_config(True)
        market = MarketSpec(
            symbol="SX-Myriad",
            target_label="YES",
            venue_a_label="SX Bet",
            venue_b_label="Myriad",
            polymarket_token_id="sx-token",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="456:NO",
            predict_fun_side=BinarySide.NO,
            myriad_market_id="456",
            myriad_side=BinarySide.YES,
            expires_at=datetime.now(UTC) + timedelta(minutes=30),
        )
        position = _open_position(
            market=market,
            polymarket_contracts=100,
            polymarket_entry_price=0.45,
            predict_fun_contracts=100,
            predict_fun_entry_price=0.47,
            opened_at=datetime.now(UTC),
            polymarket_order_id="sx-entry",
            predict_fun_order_id="myriad-entry",
        )
        ledger = PositionLedger()
        ledger.add(position)
        router = ExecutionRouter(
            config,
            sx,
            myriad,
            telegram,
            ledger,
            first_leg_label="SX Bet",
            second_leg_label="Myriad",
            first_leg_fill_timeout_ms=config.sx_bet_fill_timeout_ms,
            second_leg_fill_timeout_ms=config.myriad_fill_timeout_ms,
        )
        manager = PositionManager(
            config=config,
            polymarket=FakeBinaryClient(),
            predict_fun=None,
            execution=None,
            sx_bet=sx,
            myriad=myriad,
            sx_myriad_execution=router,
            ledger=ledger,
        )

        await manager.run_once()

        self.assertEqual(sx.watch_tokens, ["sx-token"])
        self.assertEqual(myriad.watch_tokens, ["456:NO"])
        self.assertEqual(telegram.messages, 1)

    async def test_predict_sx_auto_close_watches_route_specific_books(self) -> None:
        predict = FakeBinaryClient()
        sx = FakeBinaryClient()
        telegram = FakeTelegram()
        config = make_config(True)
        market = MarketSpec(
            symbol="Predict-SX",
            target_label="YES",
            venue_a_label="Predict.fun",
            venue_b_label="SX Bet",
            polymarket_token_id="predict-token",
            polymarket_side=BinarySide.NO,
            polymarket_market_id="predict-market",
            predict_fun_token_id="sx-token",
            predict_fun_side=BinarySide.YES,
            predict_fun_market_id="0xsxmarket",
            expires_at=datetime.now(UTC) + timedelta(minutes=30),
        )
        position = _open_position(
            market=market,
            polymarket_contracts=100,
            polymarket_entry_price=0.45,
            predict_fun_contracts=100,
            predict_fun_entry_price=0.47,
            opened_at=datetime.now(UTC),
            polymarket_order_id="predict-entry",
            predict_fun_order_id="sx-entry",
        )
        ledger = PositionLedger()
        ledger.add(position)
        router = ExecutionRouter(
            config,
            predict,
            sx,
            telegram,
            ledger,
            first_leg_label="Predict.fun",
            second_leg_label="SX Bet",
            first_leg_fill_timeout_ms=config.predict_fun_fill_timeout_ms,
            second_leg_fill_timeout_ms=config.sx_bet_fill_timeout_ms,
        )
        manager = PositionManager(
            config=config,
            polymarket=FakeBinaryClient(),
            predict_fun=predict,
            execution=None,
            sx_bet=sx,
            predict_sx_execution=router,
            ledger=ledger,
        )

        await manager.run_once()

        self.assertEqual(predict.watch_tokens, ["predict-token"])
        self.assertEqual(sx.watch_tokens, ["sx-token"])
        self.assertEqual(telegram.messages, 1)

    async def test_predict_myriad_production_entry_and_exit_use_route_specific_tokens(self) -> None:
        predict = FakeBinaryClient()
        myriad = FakeBinaryClient()
        predict.fill_result = True
        myriad.fill_result = True
        telegram = FakeTelegram()
        config = replace(make_config(True), execution_mode=ExecutionMode.CANARY, _execution_mode_explicit=True)
        ledger = PositionLedger()
        router = ExecutionRouter(
            config,
            predict,
            myriad,
            telegram,
            ledger,
            first_leg_label="Predict.fun",
            second_leg_label="Myriad",
            first_leg_fill_timeout_ms=config.predict_fun_fill_timeout_ms,
            second_leg_fill_timeout_ms=config.myriad_fill_timeout_ms,
        )
        market = MarketSpec(
            symbol="Predict-Myriad",
            target_label="YES",
            venue_a_label="Predict.fun",
            venue_b_label="Myriad",
            polymarket_token_id="predict-token",
            polymarket_side=BinarySide.NO,
            predict_fun_token_id="123:YES",
            predict_fun_side=BinarySide.YES,
            myriad_market_id="123",
            myriad_side=BinarySide.NO,
            expires_at=datetime.now(UTC) + timedelta(minutes=30),
        )
        signal = ArbitrageSignal(
            market=market,
            plan=PositionPlan(
                polymarket_contracts=Decimal("10"),
                polymarket_capital_usd=Decimal("4.5"),
                predict_fun_contracts=Decimal("10"),
                predict_fun_capital_usd=Decimal("5.5"),
                payout_contracts=Decimal("10"),
                total_cost_usd=Decimal("10"),
                polymarket_fee_usd=Decimal("0"),
                predict_fun_fee_usd=Decimal("0"),
            ),
            metrics=SpreadMetrics(
                gross_spread=0.10,
                net_spread=0.06,
                expected_net_profit_usd=0.6,
                polymarket_slippage=0.0,
                predict_fun_slippage=0.0,
                combined_cost_per_payout=0.94,
            ),
            polymarket_price=0.45,
            predict_fun_price=0.55,
        )

        await router._execute_production(signal)  # noqa: SLF001
        position = ledger.all()[0]
        await router._close_position_legs(  # noqa: SLF001
            position,
            polymarket_exit_price=Decimal("0.56"),
            predict_fun_exit_price=Decimal("0.44"),
        )

        self.assertEqual(predict.buy_tokens, ["predict-token"])
        self.assertEqual(myriad.buy_tokens, ["123:YES"])
        self.assertEqual(predict.sell_tokens, ["predict-token"])
        self.assertEqual(myriad.sell_tokens, ["123:YES"])

    async def test_polymarket_sx_production_entry_and_exit_use_route_specific_tokens(self) -> None:
        poly = FakeBinaryClient()
        sx = FakeBinaryClient()
        poly.fill_result = True
        sx.fill_result = True
        telegram = FakeTelegram()
        config = replace(make_config(True), execution_mode=ExecutionMode.CANARY, _execution_mode_explicit=True)
        ledger = PositionLedger()
        router = ExecutionRouter(
            config,
            poly,
            sx,
            telegram,
            ledger,
            second_leg_label="SX Bet",
            second_leg_fill_timeout_ms=config.sx_bet_fill_timeout_ms,
        )
        market = MarketSpec(
            symbol="Poly-SX",
            target_label="YES",
            venue_a_label="Polymarket",
            venue_b_label="SX Bet",
            polymarket_token_id="poly-token",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="sx-token",
            predict_fun_side=BinarySide.NO,
            predict_fun_market_id="0xsxmarket",
            expires_at=datetime.now(UTC) + timedelta(minutes=30),
        )
        signal = ArbitrageSignal(
            market=market,
            plan=PositionPlan(
                polymarket_contracts=Decimal("10"),
                polymarket_capital_usd=Decimal("4.5"),
                predict_fun_contracts=Decimal("10"),
                predict_fun_capital_usd=Decimal("5.5"),
                payout_contracts=Decimal("10"),
                total_cost_usd=Decimal("10"),
                polymarket_fee_usd=Decimal("0"),
                predict_fun_fee_usd=Decimal("0"),
            ),
            metrics=SpreadMetrics(
                gross_spread=0.10,
                net_spread=0.06,
                expected_net_profit_usd=0.6,
                polymarket_slippage=0.0,
                predict_fun_slippage=0.0,
                combined_cost_per_payout=0.94,
            ),
            polymarket_price=0.45,
            predict_fun_price=0.55,
        )

        await router._execute_production(signal)  # noqa: SLF001
        position = ledger.all()[0]
        await router._close_position_legs(  # noqa: SLF001
            position,
            polymarket_exit_price=Decimal("0.56"),
            predict_fun_exit_price=Decimal("0.44"),
        )

        self.assertEqual(poly.buy_tokens, ["poly-token"])
        self.assertEqual(sx.buy_tokens, ["sx-token"])
        self.assertEqual(poly.sell_tokens, ["poly-token"])
        self.assertEqual(sx.sell_tokens, ["sx-token"])

    async def test_sx_partial_ce_unwind_is_persisted_for_manual_review_without_retry(self) -> None:
        class ResidualExitClient(FakeBinaryClient):
            async def wait_filled(self, order_id: str, timeout_ms: int) -> ExecutionReport:
                if order_id.startswith("sell-"):
                    raise OrderResidualExposure(
                        "partial CE refund created residual opposite exposure",
                        report=ExecutionReport.from_amounts(
                            order_id,
                            Decimal("10"),
                            Decimal("5"),
                            ExecutionStatus.PARTIAL,
                            Decimal("0.39"),
                        ),
                        residual_contracts=Decimal("5"),
                        residual_side=BinarySide.YES,
                    )
                return await super().wait_filled(order_id, timeout_ms)

        poly = FakeBinaryClient()
        sx = ResidualExitClient()
        poly.fill_result = True
        sx.fill_result = True
        ledger = PositionLedger()
        config = replace(make_config(True), execution_mode=ExecutionMode.CANARY, _execution_mode_explicit=True)
        router = ExecutionRouter(
            config,
            poly,
            sx,
            FakeTelegram(),
            ledger,
            second_leg_label="SX Bet",
            second_leg_fill_timeout_ms=config.sx_bet_fill_timeout_ms,
        )
        market = MarketSpec(
            symbol="Poly-SX-Residual",
            target_label="YES",
            venue_a_label="Polymarket",
            venue_b_label="SX Bet",
            polymarket_token_id="poly-token",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="sx-token",
            predict_fun_side=BinarySide.NO,
            predict_fun_market_id="0xsxmarket",
            expires_at=datetime.now(UTC) + timedelta(minutes=30),
        )
        signal = ArbitrageSignal(
            market=market,
            plan=PositionPlan(
                polymarket_contracts=Decimal("10"),
                polymarket_capital_usd=Decimal("4.5"),
                predict_fun_contracts=Decimal("10"),
                predict_fun_capital_usd=Decimal("5.5"),
                payout_contracts=Decimal("10"),
                total_cost_usd=Decimal("10"),
            ),
            metrics=SpreadMetrics(0.10, 0.06, 0.6, 0.0, 0.0, 0.94),
            polymarket_price=0.45,
            predict_fun_price=0.55,
        )

        await router._execute_production(signal)  # noqa: SLF001
        await router._close_position_legs(  # noqa: SLF001
            ledger.all()[0],
            polymarket_exit_price=Decimal("0.56"),
            predict_fun_exit_price=Decimal("0.44"),
        )

        pending = ledger.all()[0]
        self.assertEqual(pending.status, "manual_review")
        self.assertEqual(pending.predict_fun_closed_contracts, Decimal("5"))
        self.assertEqual(pending.predict_fun_residual_exposure_contracts, Decimal("5"))
        self.assertEqual(pending.predict_fun_residual_exit_order_ids, ("sell-sx-token",))
        self.assertEqual(len(pending.predict_fun_residual_exit_snapshots), 1)
        self.assertEqual(
            pending.predict_fun_residual_exit_snapshots[0].exit_proceeds_usd,
            Decimal("1.95"),
        )
        self.assertTrue(router._risk.is_paused())  # noqa: SLF001
        await router.retry_partial_exit(pending)
        self.assertEqual(sx.sell_calls, 1)

    async def test_sx_residual_from_unknown_exit_reconciliation_is_not_masked(self) -> None:
        class UnknownThenResidualClient(FakeBinaryClient):
            def __init__(self) -> None:
                super().__init__()
                self.sell_waits = 0

            async def wait_filled(self, order_id: str, timeout_ms: int) -> ExecutionReport:
                if order_id.startswith("sell-"):
                    self.sell_waits += 1
                    if self.sell_waits == 1:
                        raise TimeoutError("initial SX exit outcome unknown")
                    raise OrderResidualExposure(
                        "reconciled CE fill created residual opposite exposure",
                        report=ExecutionReport.from_amounts(
                            order_id,
                            Decimal("10"),
                            Decimal("5"),
                            ExecutionStatus.PARTIAL,
                            Decimal("0.39"),
                        ),
                        residual_contracts=Decimal("5"),
                        residual_side=BinarySide.NO,
                    )
                return await super().wait_filled(order_id, timeout_ms)

        sx = UnknownThenResidualClient()
        config = replace(make_config(True), execution_mode=ExecutionMode.CANARY, _execution_mode_explicit=True)
        create_order_intent = AsyncMock()

        async def update_order_intent(
            client_order_id: str,
            status: OrderIntentStatus,
            **kwargs: Any,
        ) -> None:
            del client_order_id, kwargs
            if status is OrderIntentStatus.MANUAL_REVIEW:
                raise RuntimeError("simulated database outage during manual-review persistence")

        repository = SimpleNamespace(
            create_order_intent=create_order_intent,
            update_order_intent=AsyncMock(side_effect=update_order_intent),
        )
        router = ExecutionRouter(
            config,
            FakeBinaryClient(),
            sx,
            FakeTelegram(),
            second_leg_label="SX Bet",
            second_leg_fill_timeout_ms=config.sx_bet_fill_timeout_ms,
            repository=cast(Any, repository),
        )

        result = await router._submit_exit_leg(  # noqa: SLF001
            client=sx,
            market=make_verified_market(),
            venue_label="SX Bet",
            already_closed=False,
            token_id="sx-token",
            side=BinarySide.YES,
            contracts=Decimal("10"),
            min_price=Decimal("0.39"),
            timeout_ms=config.sx_bet_fill_timeout_ms,
        )

        self.assertIsInstance(result.error, OrderResidualExposure)
        self.assertEqual(result.report.amount_filled if result.report else None, Decimal("5"))
        self.assertEqual(sx.sell_calls, 1)
        self.assertEqual(sx.sell_waits, 2)
        self.assertTrue(router._risk.is_paused())  # noqa: SLF001

    async def test_sx_myriad_production_entry_and_exit_use_route_specific_tokens(self) -> None:
        sx = FakeBinaryClient()
        myriad = FakeBinaryClient()
        sx.fill_result = True
        myriad.fill_result = True
        telegram = FakeTelegram()
        config = replace(make_config(True), execution_mode=ExecutionMode.CANARY, _execution_mode_explicit=True)
        ledger = PositionLedger()
        router = ExecutionRouter(
            config,
            sx,
            myriad,
            telegram,
            ledger,
            first_leg_label="SX Bet",
            second_leg_label="Myriad",
            first_leg_fill_timeout_ms=config.sx_bet_fill_timeout_ms,
            second_leg_fill_timeout_ms=config.myriad_fill_timeout_ms,
        )
        market = MarketSpec(
            symbol="SX-Myriad",
            target_label="YES",
            venue_a_label="SX Bet",
            venue_b_label="Myriad",
            polymarket_token_id="sx-token",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="123:YES",
            predict_fun_side=BinarySide.YES,
            myriad_market_id="123",
            myriad_side=BinarySide.NO,
            expires_at=datetime.now(UTC) + timedelta(minutes=30),
        )
        signal = ArbitrageSignal(
            market=market,
            plan=PositionPlan(
                polymarket_contracts=Decimal("10"),
                polymarket_capital_usd=Decimal("4.5"),
                predict_fun_contracts=Decimal("10"),
                predict_fun_capital_usd=Decimal("5.5"),
                payout_contracts=Decimal("10"),
                total_cost_usd=Decimal("10"),
                polymarket_fee_usd=Decimal("0"),
                predict_fun_fee_usd=Decimal("0"),
            ),
            metrics=SpreadMetrics(
                gross_spread=0.10,
                net_spread=0.06,
                expected_net_profit_usd=0.6,
                polymarket_slippage=0.0,
                predict_fun_slippage=0.0,
                combined_cost_per_payout=0.94,
            ),
            polymarket_price=0.45,
            predict_fun_price=0.55,
        )

        await router._execute_production(signal)  # noqa: SLF001
        position = ledger.all()[0]
        await router._close_position_legs(  # noqa: SLF001
            position,
            polymarket_exit_price=Decimal("0.56"),
            predict_fun_exit_price=Decimal("0.44"),
        )

        self.assertEqual(sx.buy_tokens, ["sx-token"])
        self.assertEqual(myriad.buy_tokens, ["123:YES"])
        self.assertEqual(sx.sell_tokens, ["sx-token"])
        self.assertEqual(myriad.sell_tokens, ["123:YES"])

    async def test_predict_sx_production_entry_and_exit_use_route_specific_tokens(self) -> None:
        predict = FakeBinaryClient()
        sx = FakeBinaryClient()
        predict.fill_result = True
        sx.fill_result = True
        telegram = FakeTelegram()
        config = replace(make_config(True), execution_mode=ExecutionMode.CANARY, _execution_mode_explicit=True)
        ledger = PositionLedger()
        router = ExecutionRouter(
            config,
            predict,
            sx,
            telegram,
            ledger,
            first_leg_label="Predict.fun",
            second_leg_label="SX Bet",
            first_leg_fill_timeout_ms=config.predict_fun_fill_timeout_ms,
            second_leg_fill_timeout_ms=config.sx_bet_fill_timeout_ms,
        )
        market = MarketSpec(
            symbol="Predict-SX",
            target_label="YES",
            venue_a_label="Predict.fun",
            venue_b_label="SX Bet",
            polymarket_token_id="predict-token",
            polymarket_side=BinarySide.NO,
            polymarket_market_id="predict-market",
            predict_fun_token_id="sx-token",
            predict_fun_side=BinarySide.YES,
            predict_fun_market_id="0xsxmarket",
            expires_at=datetime.now(UTC) + timedelta(minutes=30),
        )
        signal = ArbitrageSignal(
            market=market,
            plan=PositionPlan(
                polymarket_contracts=Decimal("10"),
                polymarket_capital_usd=Decimal("4.5"),
                predict_fun_contracts=Decimal("10"),
                predict_fun_capital_usd=Decimal("5.5"),
                payout_contracts=Decimal("10"),
                total_cost_usd=Decimal("10"),
                polymarket_fee_usd=Decimal("0"),
                predict_fun_fee_usd=Decimal("0"),
            ),
            metrics=SpreadMetrics(
                gross_spread=0.10,
                net_spread=0.06,
                expected_net_profit_usd=0.6,
                polymarket_slippage=0.0,
                predict_fun_slippage=0.0,
                combined_cost_per_payout=0.94,
            ),
            polymarket_price=0.45,
            predict_fun_price=0.55,
        )

        await router._execute_production(signal)  # noqa: SLF001
        position = ledger.all()[0]
        await router._close_position_legs(  # noqa: SLF001
            position,
            polymarket_exit_price=Decimal("0.56"),
            predict_fun_exit_price=Decimal("0.44"),
        )

        self.assertEqual(predict.buy_tokens, ["predict-token"])
        self.assertEqual(sx.buy_tokens, ["sx-token"])
        self.assertEqual(predict.sell_tokens, ["predict-token"])
        self.assertEqual(sx.sell_tokens, ["sx-token"])

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
        position = _open_position(
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
            _open_position(
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
            _open_position(
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
        optimistic_debits: dict[str, Decimal | float] = {}
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
        position = _open_position(
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
        router = ExecutionRouter(
            make_config(False),
            first,
            second,
            FakeTelegram(),
            chain_cost_estimator=_zero_chain_cost_estimator(),
        )
        task = asyncio.create_task(router.handle_signal(make_signal()))
        await asyncio.wait_for(both_waiting.wait(), timeout=1.0)

        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

        self.assertTrue(first.cancelled)
        self.assertTrue(second.cancelled)
        self.assertEqual(router.ledger.all()[0].status, "entry_pending")


if __name__ == "__main__":
    unittest.main()
