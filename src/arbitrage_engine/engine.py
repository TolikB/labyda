from __future__ import annotations

import asyncio
import logging
from dataclasses import replace

from .config import AppConfig
from .connectors.base import BinaryMarketClient, PolymarketClient, PredictFunClient
from .execution import ExecutionRouter
from .models import AmmPool, ArbitrageSignal, BinarySide, MarketSpec, OrderBook
from .position_manager import PositionManager
from .quant import build_position_plan, calculate_spread_metrics

LOGGER = logging.getLogger(__name__)


class ArbitrageEngine:
    def __init__(
        self,
        config: AppConfig,
        polymarket: PolymarketClient,
        predict_fun: PredictFunClient | None,
        execution: ExecutionRouter | None,
        myriad: PredictFunClient | None = None,
        myriad_execution: ExecutionRouter | None = None,
        predict_myriad_execution: ExecutionRouter | None = None,
        position_manager: PositionManager | None = None,
    ) -> None:
        self._config = config
        self._polymarket = polymarket
        self._predict_fun = predict_fun
        self._execution = execution
        self._myriad = myriad
        self._myriad_execution = myriad_execution
        self._predict_myriad_execution = predict_myriad_execution
        self._position_manager = position_manager or PositionManager(
            config=config,
            polymarket=polymarket,
            predict_fun=predict_fun,
            execution=execution,
            myriad=myriad,
            myriad_execution=myriad_execution,
            predict_myriad_execution=predict_myriad_execution,
        )

    async def run_forever(self) -> None:
        while True:
            try:
                await self.run_once()
            except Exception:
                LOGGER.exception("engine_cycle_failed")
                await asyncio.sleep(1.0)
            await asyncio.sleep(self._config.poll_interval_ms / 1000)

    async def run_once(self) -> None:
        await self._position_manager.run_once()
        if self._execution is not None and not self._config.is_test and not await self._execution.ensure_balances():
            return
        if (
            self._config.myriad_markets.enabled
            and self._myriad_execution is not None
            and not self._config.is_test
            and not await self._myriad_execution.ensure_balances()
        ):
            return
        tasks: list[asyncio.Task[None]] = []
        for market in self._config.markets:
            if self._predict_fun is not None and self._execution is not None and market.predict_fun_token_id:
                tasks.append(
                    asyncio.create_task(
                        self._evaluate_polymarket_pair(
                        market=market,
                        first_leg=self._polymarket,
                        second_leg=self._predict_fun,
                        execution=self._execution,
                        first_token_id=market.polymarket_token_id,
                        first_side=market.polymarket_side,
                        second_token_id=market.predict_fun_token_id,
                        second_side=market.predict_fun_side,
                        first_label="Polymarket",
                        second_label="Predict.fun",
                        max_slippage_pct=self._config.predict_fun.max_slippage_pct,
                        first_amm_pool=None,
                        second_amm_pool=market.predict_fun_amm_pool,
                        )
                    )
                )
            if self._config.myriad_markets.enabled and self._myriad is not None and self._myriad_execution is not None:
                if not market.myriad_market_id:
                    continue
                tasks.append(
                    asyncio.create_task(
                        self._evaluate_polymarket_pair(
                            market=replace(
                                market,
                                venue_a_label="Polymarket",
                                predict_fun_token_id=f"{market.myriad_market_id}:{market.myriad_side.value}",
                                predict_fun_side=market.myriad_side,
                                venue_b_label="Myriad",
                            ),
                            first_leg=self._polymarket,
                            second_leg=self._myriad,
                            execution=self._myriad_execution,
                            first_token_id=market.polymarket_token_id,
                            first_side=market.polymarket_side,
                            second_token_id=f"{market.myriad_market_id}:{market.myriad_side.value}",
                            second_side=market.myriad_side,
                            first_label="Polymarket",
                            second_label="Myriad",
                            max_slippage_pct=self._config.myriad_markets.max_slippage_pct,
                            first_amm_pool=None,
                            second_amm_pool=None,
                        )
                    )
                )
                if self._predict_fun is not None and self._predict_myriad_execution is not None and market.predict_fun_token_id:
                    tasks.append(
                        asyncio.create_task(
                            self._evaluate_polymarket_pair(
                                market=replace(
                                    market,
                                    venue_a_label="Predict.fun",
                                    venue_b_label="Myriad",
                                    polymarket_token_id=market.predict_fun_token_id,
                                    polymarket_side=market.predict_fun_side,
                                    predict_fun_token_id=f"{market.myriad_market_id}:{market.myriad_side.value}",
                                    predict_fun_side=market.myriad_side,
                                    condition_id=None,
                                    tick_size=None,
                                ),
                                first_leg=self._predict_fun,
                                second_leg=self._myriad,
                                execution=self._predict_myriad_execution,
                                first_token_id=market.predict_fun_token_id,
                                first_side=market.predict_fun_side,
                                second_token_id=f"{market.myriad_market_id}:{market.myriad_side.value}",
                                second_side=market.myriad_side,
                                first_label="Predict.fun",
                                second_label="Myriad",
                                max_slippage_pct=min(
                                    self._config.predict_fun.max_slippage_pct,
                                    self._config.myriad_markets.max_slippage_pct,
                                ),
                                first_amm_pool=market.predict_fun_amm_pool,
                                second_amm_pool=None,
                            )
                        )
                    )
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                LOGGER.exception("market_route_evaluation_failed", exc_info=result)

    async def _evaluate_polymarket_pair(
        self,
        *,
        market: MarketSpec,
        first_leg: BinaryMarketClient,
        second_leg: BinaryMarketClient,
        execution: ExecutionRouter,
        first_token_id: str,
        first_side: BinarySide,
        second_token_id: str,
        second_side: BinarySide,
        first_label: str,
        second_label: str,
        max_slippage_pct: float,
        first_amm_pool: AmmPool | None,
        second_amm_pool: AmmPool | None,
    ) -> None:
        if not first_token_id or not second_token_id:
            return
        first_book: OrderBook | None = None
        second_book: OrderBook | None = None
        if first_amm_pool is None and second_amm_pool is None:
            first_book, second_book = await asyncio.gather(
                first_leg.watch_order_book(first_token_id),
                second_leg.watch_order_book(second_token_id),
            )
        elif first_amm_pool is None:
            first_book = await first_leg.watch_order_book(first_token_id)
        elif second_amm_pool is None:
            second_book = await second_leg.watch_order_book(second_token_id)
        else:
            raise ValueError("at least one routed leg must expose an order book")
        try:
            metrics = calculate_spread_metrics(
                polymarket_book=first_book,
                predict_fun_book=second_book,
                max_order_size_usd=self._target_leg_notional_usd(),
                min_net_spread=self._config.min_net_spread,
                max_slippage_pct=max_slippage_pct,
                polymarket_amm_pool=first_amm_pool,
                polymarket_side=first_side,
                predict_fun_amm_pool=second_amm_pool,
                predict_fun_side=second_side,
            )
        except ValueError as exc:
            LOGGER.info(
                "liquidity_guard_rejected_market",
                extra={"_symbol": market.symbol, "_venue": f"{first_label}<->{second_label}", "_reason": str(exc)},
            )
            return
        if metrics.net_spread <= self._config.min_net_spread:
            return
        try:
            plan = build_position_plan(
                polymarket_book=first_book,
                predict_fun_book=second_book,
                max_order_size_usd=self._target_leg_notional_usd(),
                max_slippage_pct=max_slippage_pct,
                polymarket_amm_pool=first_amm_pool,
                polymarket_side=first_side,
                predict_fun_amm_pool=second_amm_pool,
                predict_fun_side=second_side,
            )
        except ValueError as exc:
            LOGGER.info(
                "liquidity_guard_rejected_market",
                extra={"_symbol": market.symbol, "_venue": f"{first_label}<->{second_label}", "_reason": str(exc)},
            )
            return
        signal = ArbitrageSignal(
            market=market,
            plan=plan,
            metrics=metrics,
            polymarket_price=plan.polymarket_capital_usd / plan.polymarket_contracts,
            predict_fun_price=plan.predict_fun_capital_usd / plan.predict_fun_contracts,
        )
        await execution.handle_signal(signal)

    def _target_leg_notional_usd(self) -> float:
        return self._config.position_size_usd / 2.0
