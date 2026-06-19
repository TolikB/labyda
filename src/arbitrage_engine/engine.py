from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import replace
from collections.abc import Coroutine
from decimal import Decimal
from typing import Any

from .config import AppConfig
from .connectors.base import (
    BinaryMarketClient,
    OrderBookStaleException,
    OrderBookUnavailableException,
    PolymarketClient,
    PredictFunClient,
)
from .execution import ExecutionRouter
from .models import AmmPool, ArbitrageSignal, BinarySide, MarketSpec, OrderBook, opposite_binary_side
from .position_manager import PositionManager
from .quant import build_position_plan, calculate_spread_metrics
from .telegram import TelegramNotifier

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
        market_locks: dict[str, asyncio.Lock] | None = None,
        telegram: TelegramNotifier | None = None,
    ) -> None:
        self._config = config
        self._polymarket = polymarket
        self._predict_fun = predict_fun
        self._execution = execution
        self._myriad = myriad
        self._myriad_execution = myriad_execution
        self._predict_myriad_execution = predict_myriad_execution
        self._market_locks = market_locks if market_locks is not None else {}
        self._telegram = telegram
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
        heartbeat_task = asyncio.create_task(self._monitor_market_data_heartbeat())
        try:
            while True:
                try:
                    await self.run_once()
                except Exception:
                    LOGGER.exception("engine_cycle_failed")
                    await asyncio.sleep(1.0)
                await asyncio.sleep(self._config.poll_interval_ms / 1000)
        finally:
            heartbeat_task.cancel()
            await asyncio.gather(heartbeat_task, return_exceptions=True)

    async def _monitor_market_data_heartbeat(self) -> None:
        streaming_clients: tuple[tuple[str, BinaryMarketClient | None], ...] = (
            ("Polymarket", self._polymarket),
            ("Myriad", self._myriad),
        )
        while True:
            await asyncio.sleep(self._config.websocket_heartbeat_interval_seconds)
            for venue_label, client in streaming_clients:
                if client is None:
                    continue
                age = client.market_data_age_seconds()
                if age is None or age <= self._config.websocket_stale_after_seconds:
                    continue
                LOGGER.warning(
                    "websocket_market_data_stale_reconnecting",
                    extra={"_venue": venue_label, "_age_seconds": age},
                )
                try:
                    await client.reconnect_market_data()
                    if self._telegram is not None:
                        await self._telegram.send_html(
                            f"⚠️ WebSocket connection lost on {venue_label}. Reconnecting..."
                        )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    LOGGER.exception("websocket_market_data_reconnect_failed", extra={"_venue": venue_label})

    async def run_once(self) -> None:
        await self._position_manager.run_once()
        routers = (self._execution, self._myriad_execution, self._predict_myriad_execution)
        if any(router is not None and router.is_paused for router in routers):
            return
        live_execution = not self._config.is_test and not self._config.shadow_mode
        if self._execution is not None and live_execution and not await self._execution.ensure_balances():
            return
        if (
            self._config.myriad_markets.enabled
            and self._myriad_execution is not None
            and live_execution
            and not await self._myriad_execution.ensure_balances()
        ):
            return
        evaluations: list[Coroutine[Any, Any, None]] = []
        for market in self._config.markets:
            if self._predict_fun is not None and self._execution is not None and market.predict_fun_token_id:
                evaluations.append(
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
            if self._config.myriad_markets.enabled and self._myriad is not None and self._myriad_execution is not None:
                if not market.myriad_market_id:
                    continue
                evaluations.append(
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
                if self._predict_fun is not None and self._predict_myriad_execution is not None and market.predict_fun_token_id:
                    predict_myriad_side = opposite_binary_side(market.predict_fun_side)
                    evaluations.append(
                        self._evaluate_polymarket_pair(
                                market=replace(
                                    market,
                                    venue_a_label="Predict.fun",
                                    venue_b_label="Myriad",
                                    polymarket_token_id=market.predict_fun_token_id,
                                    polymarket_side=market.predict_fun_side,
                                    predict_fun_token_id=f"{market.myriad_market_id}:{predict_myriad_side.value}",
                                    predict_fun_side=predict_myriad_side,
                                    condition_id=None,
                                    tick_size=None,
                                    neg_risk=market.predict_fun_neg_risk,
                                ),
                                first_leg=self._predict_fun,
                                second_leg=self._myriad,
                                execution=self._predict_myriad_execution,
                                first_token_id=market.predict_fun_token_id,
                                first_side=market.predict_fun_side,
                                second_token_id=f"{market.myriad_market_id}:{predict_myriad_side.value}",
                                second_side=predict_myriad_side,
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
        results: list[None | BaseException] = []
        limit = self._config.max_concurrent_market_evaluations
        next_index = 0
        try:
            for start in range(0, len(evaluations), limit):
                batch = await asyncio.gather(*evaluations[start : start + limit], return_exceptions=True)
                results.extend(batch)
                next_index = min(start + limit, len(evaluations))
        finally:
            for evaluation in evaluations[next_index:]:
                evaluation.close()
        for result in results:
            if isinstance(result, OrderBookStaleException):
                LOGGER.debug("market_route_skipped_stale_orderbook", extra={"_reason": str(result)})
            elif isinstance(result, OrderBookUnavailableException):
                LOGGER.debug("market_route_skipped_unavailable_orderbook", extra={"_reason": str(result)})
            elif isinstance(result, Exception):
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
        now = time.time()
        stale_books = [
            (label, max(0.0, now - book.timestamp))
            for label, book in ((first_label, first_book), (second_label, second_book))
            if book is not None and now - book.timestamp > self._config.max_orderbook_age_seconds
        ]
        if stale_books:
            LOGGER.debug(
                "signal_evaluation_stale_book_rejected",
                extra={
                    "_symbol": market.symbol,
                    "_ages": {label: age for label, age in stale_books},
                    "_max_allowed": self._config.max_orderbook_age_seconds,
                },
            )
            return
        effective_first_amm = first_amm_pool or _amm_pool_from_book(first_book)
        effective_second_amm = second_amm_pool or _amm_pool_from_book(second_book)
        try:
            metrics = calculate_spread_metrics(
                polymarket_book=first_book,
                predict_fun_book=second_book,
                max_order_size_usd=self._target_leg_notional_usd(),
                min_net_spread=self._config.min_net_spread,
                max_slippage_pct=max_slippage_pct,
                polymarket_amm_pool=effective_first_amm,
                polymarket_side=first_side,
                predict_fun_amm_pool=effective_second_amm,
                predict_fun_side=second_side,
                polymarket_fee_pct=self._venue_fee_pct(first_label, market),
                predict_fun_fee_pct=self._venue_fee_pct(second_label, market),
                max_price_impact=self._config.max_production_price_impact,
            )
        except ValueError as exc:
            LOGGER.debug(
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
                polymarket_amm_pool=effective_first_amm,
                polymarket_side=first_side,
                predict_fun_amm_pool=effective_second_amm,
                predict_fun_side=second_side,
                polymarket_fee_pct=self._venue_fee_pct(first_label, market),
                predict_fun_fee_pct=self._venue_fee_pct(second_label, market),
                max_price_impact=self._config.max_production_price_impact,
            )
        except ValueError as exc:
            LOGGER.debug(
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
            raw_books={
                first_label: _book_debug_payload(first_book, first_token_id, first_side),
                second_label: _book_debug_payload(second_book, second_token_id, second_side),
            },
        )
        await execution.handle_signal(signal)

    def _target_leg_notional_usd(self) -> float:
        return self._config.position_size_usd / 2.0

    def _venue_fee_pct(self, venue_label: str, market: MarketSpec) -> float:
        if venue_label == "Polymarket":
            return self._config.polymarket.trading_fee_pct
        if venue_label == "Predict.fun":
            fee_rate_bps = (
                market.predict_fun_fee_rate_bps
                if market.predict_fun_fee_rate_bps is not None
                else self._config.predict_fun.fee_rate_bps
            )
            return float(Decimal(fee_rate_bps) / Decimal(10_000))
        if venue_label == "Myriad":
            return self._config.myriad_markets.trading_fee_pct
        raise ValueError(f"Unsupported venue label: {venue_label}")


def _book_debug_payload(book: OrderBook | None, token_id: str, side: BinarySide) -> dict[str, object]:
    if book is None:
        return {"token_id": token_id, "side": side.value, "book": None}
    return {
        "token_id": token_id,
        "side": side.value,
        "timestamp": book.timestamp,
        "bids": [{"price": level.price, "size": level.size} for level in book.bids],
        "asks": [{"price": level.price, "size": level.size} for level in book.asks],
        "source_payload": book.raw_payload,
    }


def _amm_pool_from_book(book: OrderBook | None) -> AmmPool | None:
    if book is None or not isinstance(book.raw_payload, dict):
        return None
    raw_pool = book.raw_payload.get("amm_pool")
    if not isinstance(raw_pool, dict):
        return None
    try:
        return AmmPool(
            yes_reserve=float(raw_pool["yes_reserve"]),
            no_reserve=float(raw_pool["no_reserve"]),
            fee_pct=float(raw_pool.get("fee_pct", 0.0)),
        )
    except (KeyError, TypeError, ValueError):
        return None
