from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, replace
from decimal import Decimal
from functools import partial
from math import ceil
from typing import Any

from .chain_cost import LiveChainCostEstimator, LiveChainCostUnavailable
from .config import AppConfig
from .connectors.base import (
    BinaryMarketClient,
    OrderBookStaleException,
    OrderBookUnavailableException,
)
from .execution import ExecutionRouter
from .market_mapping import is_live_mapping_eligible, route_key
from .models import (
    AmmPool,
    ArbitrageSignal,
    BinarySide,
    ExecutionMode,
    MarketDataStatus,
    MarketSpec,
    OrderBook,
    VenueFeeQuote,
    myriad_execution_token_for_route,
)
from .position_manager import PositionManager
from .quant import build_position_plan, calculate_spread_metrics, executable_depth_usd
from .telegram import TelegramNotifier

LOGGER = logging.getLogger(__name__)


def _book_observation_key(book: OrderBook | None) -> tuple[object, ...]:
    if book is None:
        return (None, None, None)
    return (book.timestamp, book.sequence, book.checksum)


def _amm_observation_key(pool: AmmPool | None) -> tuple[object, ...]:
    if pool is None:
        return (None, None, None)
    return (pool.yes_reserve, pool.no_reserve, pool.fee_pct)


@dataclass(frozen=True)
class _PlannedEvaluation:
    route: str
    run: Callable[[float], Coroutine[Any, Any, None]]
    targets: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class _ExecutableObservation:
    observed_at: float
    net_spread: float


class ArbitrageEngine:
    def __init__(
        self,
        config: AppConfig,
        polymarket: BinaryMarketClient,
        predict_fun: BinaryMarketClient | None,
        execution: ExecutionRouter | None,
        sx_bet: BinaryMarketClient | None = None,
        sx_execution: ExecutionRouter | None = None,
        myriad: BinaryMarketClient | None = None,
        myriad_execution: ExecutionRouter | None = None,
        predict_myriad_execution: ExecutionRouter | None = None,
        predict_sx_execution: ExecutionRouter | None = None,
        sx_myriad_execution: ExecutionRouter | None = None,
        position_manager: PositionManager | None = None,
        market_locks: dict[str, asyncio.Lock] | None = None,
        telegram: TelegramNotifier | None = None,
        market_provider: Callable[[], tuple[MarketSpec, ...]] | None = None,
        signal_evaluation_observer: Callable[[str, str, float | None], None] | None = None,
        market_economics_observer: Callable[[str, dict[str, float]], None] | None = None,
        calibration_observer: Callable[[str, float | None], None] | None = None,
        chain_cost_estimator: LiveChainCostEstimator | None = None,
    ) -> None:
        self._config = config
        self._polymarket = polymarket
        self._predict_fun = predict_fun
        self._execution = execution
        self._sx_bet = sx_bet
        self._sx_execution = sx_execution
        self._myriad = myriad
        self._myriad_execution = myriad_execution
        self._predict_myriad_execution = predict_myriad_execution
        self._predict_sx_execution = predict_sx_execution
        self._sx_myriad_execution = sx_myriad_execution
        self._market_locks = market_locks if market_locks is not None else {}
        self._telegram = telegram
        static_markets = tuple(self._config.markets)
        self._market_provider = market_provider or (lambda: static_markets)
        self._signal_evaluation_observer = signal_evaluation_observer
        self._market_economics_observer = market_economics_observer
        self._calibration_observer = calibration_observer
        self._chain_cost_estimator = chain_cost_estimator or LiveChainCostEstimator(config)
        self._calibration_history: dict[tuple[str, str], deque[tuple[float, float]]] = {}
        self._calibration_last_observation: dict[tuple[str, str], tuple[object, ...]] = {}
        self._background_tasks: set[asyncio.Task[Any]] = set()
        self._evaluation_cursors_by_route: dict[str, int] = {}
        self._active_evaluation_cursors_by_route: dict[str, int] = {}
        self._route_evaluation_cursor = 0
        self._held_evaluation_keys_by_route: dict[str, tuple[tuple[tuple[str, str], ...], ...]] = {}
        self._evaluation_window_expires_at_by_route: dict[str, float] = {}
        self._recent_executable_evaluations: dict[
            tuple[str, tuple[tuple[str, str], ...]],
            _ExecutableObservation,
        ] = {}
        self._planned_market_snapshot: tuple[MarketSpec, ...] | None = None
        self._planned_evaluations: tuple[_PlannedEvaluation, ...] = ()
        self._entry_market_data_targets: dict[str, set[str]] = {}
        self._synced_market_data_targets: dict[str, set[str]] = {}
        self._position_manager = position_manager or PositionManager(
            config=config,
            polymarket=polymarket,
            predict_fun=predict_fun,
            execution=execution,
            sx_bet=sx_bet,
            sx_execution=sx_execution,
            myriad=myriad,
            myriad_execution=myriad_execution,
            predict_myriad_execution=predict_myriad_execution,
            predict_sx_execution=predict_sx_execution,
            sx_myriad_execution=sx_myriad_execution,
        )

    async def close(self) -> None:
        await self._chain_cost_estimator.close()

    def set_signal_evaluation_observer(self, observer: Callable[[str, str, float | None], None] | None) -> None:
        """Expose aggregate evaluation outcomes without coupling strategy code to Prometheus."""
        self._signal_evaluation_observer = observer

    def _record_signal_evaluation(self, route: str, outcome: str, net_spread: float | None = None) -> None:
        if self._signal_evaluation_observer is not None:
            self._signal_evaluation_observer(route, outcome, net_spread)

    def set_market_economics_observer(
        self,
        observer: Callable[[str, dict[str, float]], None] | None,
    ) -> None:
        self._market_economics_observer = observer

    def _record_market_economics(self, route: str, values: dict[str, float]) -> None:
        if self._market_economics_observer is not None:
            self._market_economics_observer(route, values)

    def set_calibration_observer(self, observer: Callable[[str, float | None], None] | None) -> None:
        self._calibration_observer = observer

    def _record_route_calibration(
        self,
        route: str,
        market_key: str,
        net_spread: float,
        first_book: OrderBook | None,
        second_book: OrderBook | None,
        first_amm_pool: AmmPool | None,
        second_amm_pool: AmmPool | None,
    ) -> None:
        history_key = (route, market_key)
        observation_key = (
            *_book_observation_key(first_book),
            *_book_observation_key(second_book),
            *_amm_observation_key(first_amm_pool),
            *_amm_observation_key(second_amm_pool),
        )
        if self._calibration_last_observation.get(history_key) == observation_key:
            if self._calibration_observer is not None:
                self._calibration_observer(route, None)
            return
        self._calibration_last_observation[history_key] = observation_key
        now = time.monotonic()
        horizon = self._execution_latency_horizon_seconds(route)
        history = self._calibration_history.setdefault(history_key, deque())
        cutoff = now - horizon
        reference_spread: float | None = None
        for observed_at, observed_spread in reversed(history):
            if observed_at <= cutoff:
                reference_spread = observed_spread
                break
        retention_cutoff = now - max(30.0, horizon * 2.0)
        while history and history[0][0] < retention_cutoff:
            history.popleft()
        history.append((now, net_spread))
        adverse_move = None if reference_spread is None else max(0.0, reference_spread - net_spread)
        if self._calibration_observer is not None:
            self._calibration_observer(route, adverse_move)

    def _execution_latency_horizon_seconds(self, route: str) -> float:
        labels = route.split("_")
        timeouts = {
            "polymarket": self._config.polymarket_fill_timeout_ms,
            "predict": self._config.predict_fun_fill_timeout_ms,
            "sx": self._config.sx_bet_fill_timeout_ms,
            "myriad": self._config.myriad_fill_timeout_ms,
        }
        return max(0.001, sum(timeouts.get(label, 0) for label in labels) / 1000.0)

    async def run_forever(self, shutdown_event: asyncio.Event | None = None) -> None:
        heartbeat_task = asyncio.create_task(self._monitor_market_data_heartbeat())
        try:
            while shutdown_event is None or not shutdown_event.is_set():
                delay = self._config.poll_interval_ms / 1000
                try:
                    await self.run_once()
                except Exception:
                    LOGGER.exception("engine_cycle_failed")
                    delay = 1.0
                if shutdown_event is not None and shutdown_event.is_set():
                    break
                if shutdown_event is None:
                    await asyncio.sleep(delay)
                    continue
                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=delay)
                except TimeoutError:
                    pass
        finally:
            heartbeat_task.cancel()
            await asyncio.gather(heartbeat_task, return_exceptions=True)
            background = list(self._background_tasks)
            for task in background:
                task.cancel()
            if background:
                await asyncio.gather(*background, return_exceptions=True)

    async def _monitor_market_data_heartbeat(self) -> None:
        streaming_clients: tuple[tuple[str, BinaryMarketClient | None], ...] = (
            ("Polymarket", self._polymarket),
            ("Myriad", self._myriad),
        )
        alerting: set[str] = set()
        while True:
            await asyncio.sleep(self._config.websocket_heartbeat_interval_seconds)
            for venue_label, client in streaming_clients:
                if client is None or not client.has_active_market_data_targets():
                    continue
                age = client.market_data_age_seconds()
                stream_connected = client.market_data_stream_connected()
                fallback_stream_healthy = age is None or age <= self._config.websocket_stale_after_seconds
                stream_healthy = stream_connected if stream_connected is not None else fallback_stream_healthy
                if stream_healthy:
                    if venue_label in alerting and client.market_data_ready():
                        alerting.remove(venue_label)
                        self._notify_telegram(f"✅ WebSocket market data restored on {venue_label}.")
                    continue
                LOGGER.warning(
                    "websocket_market_data_disconnected_reconnecting",
                    extra={
                        "_venue": venue_label,
                        "_age_seconds": age,
                        "_stream_connected": stream_connected,
                    },
                )
                try:
                    await client.reconnect_market_data()
                    if venue_label not in alerting:
                        alerting.add(venue_label)
                        self._notify_telegram(f"⚠️ WebSocket connection lost on {venue_label}. Reconnecting...")
                except asyncio.CancelledError:
                    raise
                except Exception:
                    LOGGER.exception("websocket_market_data_reconnect_failed", extra={"_venue": venue_label})

    def _notify_telegram(self, message: str) -> None:
        telegram = self._telegram
        if telegram is None:
            return

        async def _send() -> None:
            await telegram.send_html(message)

        task = asyncio.create_task(_send())
        self._background_tasks.add(task)

        def _cleanup(completed: asyncio.Task[Any]) -> None:
            self._background_tasks.discard(completed)
            try:
                completed.result()
            except asyncio.CancelledError:
                return
            except Exception:
                LOGGER.exception("telegram_background_send_failed")

        task.add_done_callback(_cleanup)

    async def run_once(self) -> None:
        # Position reconciliation must retain its targets even when entry scanning is paused.
        self._sync_market_data_targets(self._entry_market_data_targets)
        await self._position_manager.run_once()
        if self._config.execution_mode.submits_orders and self._has_paused_execution_router():
            return
        live_execution = self._config.execution_mode.submits_orders
        if live_execution:
            for router in self._execution_routers():
                if router is not None and not await router.ensure_balances():
                    return
        market_snapshot = self._market_provider()
        plan_cache_hit = market_snapshot is self._planned_market_snapshot
        evaluations = list(self._planned_evaluations) if plan_cache_hit else []
        eligibility_mode = self._mapping_eligibility_mode()
        for market in () if plan_cache_hit else market_snapshot:
            if (
                getattr(self._config.routes, "polymarket_predict", False)
                and self._predict_fun is not None
                and self._execution is not None
                and market.predict_fun_token_id
                and market.venue_b_label == "Predict.fun"
                and is_live_mapping_eligible(market, eligibility_mode, "polymarket_predict")
            ):
                evaluations.append(
                    self._plan_polymarket_pair(
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
                        max_slippage_pct=self._second_venue_slippage_pct("Predict.fun"),
                        first_amm_pool=None,
                        second_amm_pool=market.predict_fun_amm_pool,
                    )
                )
            if (
                getattr(self._config.routes, "polymarket_sx", False)
                and self._sx_bet is not None
                and self._sx_execution is not None
                and market.predict_fun_token_id
                and market.venue_b_label == "SX Bet"
                and is_live_mapping_eligible(market, eligibility_mode, "polymarket_sx")
            ):
                evaluations.append(
                    self._plan_polymarket_pair(
                        market=market,
                        first_leg=self._polymarket,
                        second_leg=self._sx_bet,
                        execution=self._sx_execution,
                        first_token_id=market.polymarket_token_id,
                        first_side=market.polymarket_side,
                        second_token_id=market.predict_fun_token_id,
                        second_side=market.predict_fun_side,
                        first_label="Polymarket",
                        second_label="SX Bet",
                        max_slippage_pct=self._second_venue_slippage_pct("SX Bet"),
                        first_amm_pool=None,
                        second_amm_pool=market.predict_fun_amm_pool,
                    )
                )
            if (
                self._config.routes.polymarket_myriad
                and self._myriad is not None
                and self._myriad_execution is not None
                and market.myriad_market_id
                and is_live_mapping_eligible(market, eligibility_mode, "polymarket_myriad")
            ):
                evaluations.append(
                    self._plan_polymarket_pair(
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
            if (
                getattr(self._config.routes, "predict_myriad", False)
                and self._predict_fun is not None
                and self._myriad is not None
                and self._predict_myriad_execution is not None
                and market.predict_fun_token_id
                and market.myriad_market_id
                and market.venue_b_label == "Predict.fun"
                and is_live_mapping_eligible(market, eligibility_mode, "predict_myriad")
            ):
                predict_myriad_token = myriad_execution_token_for_route(market, "predict_myriad")
                if predict_myriad_token is None:
                    continue
                predict_myriad_side = BinarySide(predict_myriad_token.rsplit(":", 1)[1])
                evaluations.append(
                    self._plan_polymarket_pair(
                        market=replace(
                            market,
                            venue_a_label="Predict.fun",
                            venue_b_label="Myriad",
                            polymarket_token_id=market.predict_fun_token_id,
                            polymarket_side=market.predict_fun_side,
                            predict_fun_token_id=predict_myriad_token,
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
                        second_token_id=predict_myriad_token,
                        second_side=predict_myriad_side,
                        first_label="Predict.fun",
                        second_label="Myriad",
                        max_slippage_pct=min(
                            self._second_venue_slippage_pct("Predict.fun"),
                            self._config.myriad_markets.max_slippage_pct,
                        ),
                        first_amm_pool=market.predict_fun_amm_pool,
                        second_amm_pool=None,
                    )
                )
            if (
                getattr(self._config.routes, "predict_sx", False)
                and self._predict_fun is not None
                and self._sx_bet is not None
                and self._predict_sx_execution is not None
                and market.venue_a_label == "Predict.fun"
                and market.venue_b_label == "SX Bet"
                and market.polymarket_token_id
                and market.predict_fun_token_id
                and is_live_mapping_eligible(market, eligibility_mode, "predict_sx")
            ):
                evaluations.append(
                    self._plan_polymarket_pair(
                        market=market,
                        first_leg=self._predict_fun,
                        second_leg=self._sx_bet,
                        execution=self._predict_sx_execution,
                        first_token_id=market.polymarket_token_id,
                        first_side=market.polymarket_side,
                        second_token_id=market.predict_fun_token_id,
                        second_side=market.predict_fun_side,
                        first_label="Predict.fun",
                        second_label="SX Bet",
                        max_slippage_pct=min(
                            self._second_venue_slippage_pct("Predict.fun"),
                            self._second_venue_slippage_pct("SX Bet"),
                        ),
                        first_amm_pool=market.predict_fun_amm_pool,
                        second_amm_pool=None,
                    )
                )
            if (
                getattr(self._config.routes, "sx_myriad", False)
                and self._sx_bet is not None
                and self._myriad is not None
                and self._sx_myriad_execution is not None
                and market.predict_fun_token_id
                and market.myriad_market_id
                and market.venue_b_label == "SX Bet"
                and is_live_mapping_eligible(market, eligibility_mode, "sx_myriad")
            ):
                sx_myriad_token = myriad_execution_token_for_route(market, "sx_myriad")
                if sx_myriad_token is None:
                    continue
                sx_myriad_side = BinarySide(sx_myriad_token.rsplit(":", 1)[1])
                evaluations.append(
                    self._plan_polymarket_pair(
                        market=replace(
                            market,
                            venue_a_label="SX Bet",
                            venue_b_label="Myriad",
                            polymarket_token_id=market.predict_fun_token_id,
                            polymarket_side=market.predict_fun_side,
                            predict_fun_token_id=sx_myriad_token,
                            predict_fun_side=sx_myriad_side,
                            condition_id=None,
                            tick_size=None,
                            neg_risk=market.predict_fun_neg_risk,
                        ),
                        first_leg=self._sx_bet,
                        second_leg=self._myriad,
                        execution=self._sx_myriad_execution,
                        first_token_id=market.predict_fun_token_id,
                        first_side=market.predict_fun_side,
                        second_token_id=sx_myriad_token,
                        second_side=sx_myriad_side,
                        first_label="SX Bet",
                        second_label="Myriad",
                        max_slippage_pct=min(
                            self._second_venue_slippage_pct("SX Bet"),
                            self._config.myriad_markets.max_slippage_pct,
                        ),
                        first_amm_pool=market.predict_fun_amm_pool,
                        second_amm_pool=None,
                    )
                )
        if not plan_cache_hit:
            self._planned_market_snapshot = market_snapshot
            self._planned_evaluations = tuple(evaluations)
        limit = self._config.max_concurrent_market_evaluations
        active_evaluations, target_evaluations = self._select_evaluation_window(evaluations, limit)
        self._entry_market_data_targets = self._targets_for_evaluations(target_evaluations)
        self._sync_market_data_targets(self._entry_market_data_targets)
        await self._prime_market_data_targets()
        chain_costs = await self._route_chain_costs(active_evaluations)
        results = await asyncio.gather(
            *(
                evaluation.run(chain_costs[evaluation.route])
                for evaluation in active_evaluations
                if evaluation.route in chain_costs
            ),
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, OrderBookStaleException):
                LOGGER.debug("market_route_skipped_stale_orderbook", extra={"_reason": str(result)})
            elif isinstance(result, OrderBookUnavailableException):
                LOGGER.debug("market_route_skipped_unavailable_orderbook", extra={"_reason": str(result)})
            elif isinstance(result, Exception):
                LOGGER.exception("market_route_evaluation_failed", exc_info=result)

    async def _route_chain_costs(self, evaluations: list[_PlannedEvaluation]) -> dict[str, float]:
        routes = tuple(dict.fromkeys(evaluation.route for evaluation in evaluations))

        async def _quote(route: str) -> tuple[str, float | None]:
            try:
                quote = await self._chain_cost_estimator.estimate(
                    route,
                    require_live=self._config.spread_policy.require_live_gas_estimate,
                )
            except LiveChainCostUnavailable as exc:
                self._record_signal_evaluation(route, "chain_cost_unavailable")
                LOGGER.warning(
                    "route_chain_cost_unavailable",
                    extra={"_route": route, "_reason": str(exc)},
                )
                return route, None
            reserved_cost = float(quote.reserved_cost_usd)
            self._record_market_economics(route, {"chain_cost_usd": reserved_cost})
            return route, reserved_cost

        quotes = await asyncio.gather(*(_quote(route) for route in routes))
        return {route: cost for route, cost in quotes if cost is not None}

    def _sync_market_data_targets(self, active_targets: dict[str, set[str]] | None = None) -> None:
        active_targets = (
            {venue: set(tokens) for venue, tokens in active_targets.items()}
            if active_targets is not None
            else self._active_market_data_targets()
        )
        market_data_targets = getattr(self._position_manager, "market_data_targets", None)
        open_position_targets = market_data_targets() if callable(market_data_targets) else {}
        for venue, tokens in open_position_targets.items():
            active_targets.setdefault(venue, set()).update(tokens)
        for venue, client in (
            ("Polymarket", self._polymarket),
            ("Predict.fun", self._predict_fun),
            ("SX Bet", self._sx_bet),
            ("Myriad", self._myriad),
        ):
            venue_targets = active_targets.get(venue, set())
            if venue_targets == self._synced_market_data_targets.get(venue, set()):
                continue
            self._sync_client_targets(client, venue_targets)
            self._synced_market_data_targets[venue] = set(venue_targets)

    async def _prime_market_data_targets(self) -> None:
        clients = (self._polymarket, self._predict_fun, self._sx_bet, self._myriad)
        prime_calls: list[Coroutine[Any, Any, None]] = []
        for client in clients:
            if client is None:
                continue
            prime_targets = getattr(client, "prime_market_data_targets", None)
            if callable(prime_targets):
                prime_calls.append(prime_targets())
        results = await asyncio.gather(
            *prime_calls,
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, BaseException):
                LOGGER.warning("market_data_target_prime_failed", exc_info=result)

    def _select_evaluation_window(
        self,
        evaluations: list[_PlannedEvaluation],
        limit: int,
    ) -> tuple[list[_PlannedEvaluation], list[_PlannedEvaluation]]:
        if not evaluations:
            self._evaluation_cursors_by_route.clear()
            self._active_evaluation_cursors_by_route.clear()
            self._route_evaluation_cursor = 0
            self._held_evaluation_keys_by_route.clear()
            self._evaluation_window_expires_at_by_route.clear()
            self._recent_executable_evaluations.clear()
            return [], []
        count = min(max(1, limit), len(evaluations))
        evaluations_by_route: dict[str, list[_PlannedEvaluation]] = {}
        for evaluation in evaluations:
            evaluations_by_route.setdefault(evaluation.route, []).append(evaluation)
        routes = list(evaluations_by_route)
        route_start = self._route_evaluation_cursor % len(routes)
        ordered_routes = routes[route_start:] + routes[:route_start]
        slots_by_route = {route: 0 for route in routes}
        allocated = 0
        while allocated < count:
            made_progress = False
            for route in ordered_routes:
                route_evaluations = evaluations_by_route[route]
                for _ in range(self._config.market_evaluation_weight_for(route)):
                    if slots_by_route[route] >= len(route_evaluations):
                        break
                    slots_by_route[route] += 1
                    allocated += 1
                    made_progress = True
                    if allocated == count:
                        break
                if allocated == count:
                    break
            if not made_progress:
                break
        self._route_evaluation_cursor = (route_start + 1) % len(routes)
        active_routes = set(routes)
        for state in (
            self._evaluation_cursors_by_route,
            self._active_evaluation_cursors_by_route,
            self._held_evaluation_keys_by_route,
            self._evaluation_window_expires_at_by_route,
        ):
            for route in set(state) - active_routes:
                state.pop(route, None)

        now = time.monotonic()
        available_evaluation_keys = {
            (route, evaluation.targets)
            for route, route_evaluations in evaluations_by_route.items()
            for evaluation in route_evaluations
        }
        for key, observation in tuple(self._recent_executable_evaluations.items()):
            route = key[0]
            priority_ttl = self._config.market_data_executable_priority_for(route)
            if (
                priority_ttl <= 0
                or key not in available_evaluation_keys
                or now - observation.observed_at > priority_ttl
            ):
                self._recent_executable_evaluations.pop(key, None)
        target_evaluations_by_route: dict[str, list[_PlannedEvaluation]] = {}
        active_evaluations_by_route: dict[str, list[_PlannedEvaluation]] = {}
        for route in routes:
            route_evaluations = evaluations_by_route[route]
            active_count = slots_by_route[route]
            if active_count == 0:
                self._held_evaluation_keys_by_route.pop(route, None)
                self._evaluation_window_expires_at_by_route.pop(route, None)
                self._active_evaluation_cursors_by_route.pop(route, None)
                target_evaluations_by_route[route] = []
                active_evaluations_by_route[route] = []
                continue
            prefetch_count = min(
                len(route_evaluations),
                active_count * self._config.market_data_prefetch_multiplier_for(route),
            )
            held = self._restore_held_route_window(route, route_evaluations, prefetch_count, now)
            if held is None:
                held = self._build_prioritized_route_window(
                    route,
                    route_evaluations,
                    prefetch_count,
                    now,
                )
                self._held_evaluation_keys_by_route[route] = tuple(evaluation.targets for evaluation in held)
                self._evaluation_window_expires_at_by_route[route] = (
                    now + self._config.market_data_target_hold_for(route)
                )
                self._active_evaluation_cursors_by_route[route] = 0
            target_evaluations_by_route[route] = held
            active_evaluations_by_route[route] = self._select_prioritized_active_evaluations(
                route,
                held,
                active_count,
                now,
            )

        active_evaluations: list[_PlannedEvaluation] = []
        consumed_by_route = {route: 0 for route in routes}
        while len(active_evaluations) < count:
            for route in ordered_routes:
                consumed = consumed_by_route[route]
                route_active = active_evaluations_by_route[route]
                if consumed >= len(route_active):
                    continue
                active_evaluations.append(route_active[consumed])
                consumed_by_route[route] += 1
                if len(active_evaluations) == count:
                    break
        target_evaluations = [
            evaluation
            for route in ordered_routes
            for evaluation in target_evaluations_by_route[route]
        ]
        return active_evaluations, target_evaluations

    def _build_prioritized_route_window(
        self,
        route: str,
        evaluations: list[_PlannedEvaluation],
        count: int,
        now: float,
    ) -> list[_PlannedEvaluation]:
        recent = self._recent_executable_candidates(route, evaluations, now)
        reserve_exploration = bool(recent) and len(recent) < len(evaluations)
        exploration_count = self._exploration_count(route, count) if reserve_exploration else 0
        priority_count = min(len(recent), max(0, count - exploration_count))
        priority = recent[:priority_count]
        priority_targets = {evaluation.targets for evaluation in priority}
        remaining = count - len(priority)
        exploration = self._select_rotating_exploration(
            route,
            evaluations,
            priority_targets,
            remaining,
            self._evaluation_cursors_by_route,
        )
        return [*priority, *exploration]

    def _select_prioritized_active_evaluations(
        self,
        route: str,
        held: list[_PlannedEvaluation],
        count: int,
        now: float,
    ) -> list[_PlannedEvaluation]:
        recent = self._recent_executable_candidates(route, held, now)
        reserve_exploration = bool(recent) and len(recent) < len(held)
        exploration_count = self._exploration_count(route, count) if reserve_exploration else 0
        priority_count = min(len(recent), max(0, count - exploration_count))
        priority = recent[:priority_count]
        priority_targets = {evaluation.targets for evaluation in priority}
        remaining = count - len(priority)
        exploration = self._select_rotating_exploration(
            route,
            held,
            priority_targets,
            remaining,
            self._active_evaluation_cursors_by_route,
        )
        return [*priority, *exploration]

    @staticmethod
    def _select_rotating_exploration(
        route: str,
        evaluations: list[_PlannedEvaluation],
        excluded_targets: set[tuple[tuple[str, str], ...]],
        count: int,
        cursors: dict[str, int],
    ) -> list[_PlannedEvaluation]:
        if count <= 0 or not evaluations:
            return []
        cursor = cursors.get(route, 0) % len(evaluations)
        selected: list[_PlannedEvaluation] = []
        examined = 0
        while examined < len(evaluations) and len(selected) < count:
            evaluation = evaluations[(cursor + examined) % len(evaluations)]
            examined += 1
            if evaluation.targets in excluded_targets:
                continue
            selected.append(evaluation)
        cursors[route] = (cursor + examined) % len(evaluations)
        return selected

    def _recent_executable_candidates(
        self,
        route: str,
        evaluations: list[_PlannedEvaluation],
        now: float,
    ) -> list[_PlannedEvaluation]:
        priority_ttl = self._config.market_data_executable_priority_for(route)
        if priority_ttl <= 0:
            return []
        candidates = [
            evaluation
            for evaluation in evaluations
            if (observation := self._recent_executable_evaluations.get((route, evaluation.targets)))
            is not None
            and now - observation.observed_at <= priority_ttl
        ]
        return sorted(
            candidates,
            key=lambda evaluation: (
                self._recent_executable_evaluations[(route, evaluation.targets)].net_spread,
                self._recent_executable_evaluations[(route, evaluation.targets)].observed_at,
            ),
            reverse=True,
        )

    def _exploration_count(self, route: str, count: int) -> int:
        fraction = self._config.market_data_exploration_fraction_for(route)
        return min(count, max(1, ceil(count * fraction)))

    def _mark_recent_executable(
        self,
        route: str,
        targets: tuple[tuple[str, str], ...],
        net_spread: float,
    ) -> None:
        self._recent_executable_evaluations[(route, targets)] = _ExecutableObservation(
            observed_at=time.monotonic(),
            net_spread=net_spread,
        )

    def _restore_held_route_window(
        self,
        route: str,
        evaluations: list[_PlannedEvaluation],
        expected_count: int,
        now: float,
    ) -> list[_PlannedEvaluation] | None:
        keys = self._held_evaluation_keys_by_route.get(route, ())
        if len(keys) != expected_count or now >= self._evaluation_window_expires_at_by_route.get(route, 0.0):
            return None
        evaluations_by_key: dict[tuple[tuple[str, str], ...], deque[_PlannedEvaluation]] = {}
        for evaluation in evaluations:
            evaluations_by_key.setdefault(evaluation.targets, deque()).append(evaluation)
        held: list[_PlannedEvaluation] = []
        for key in keys:
            matches = evaluations_by_key.get(key)
            if not matches:
                return None
            held.append(matches.popleft())
        return held

    @staticmethod
    def _targets_for_evaluations(evaluations: list[_PlannedEvaluation]) -> dict[str, set[str]]:
        targets: dict[str, set[str]] = {}
        for evaluation in evaluations:
            for venue, token_id in evaluation.targets:
                if token_id:
                    targets.setdefault(venue, set()).add(token_id)
        return targets

    def _active_market_data_targets(self) -> dict[str, set[str]]:
        targets: dict[str, set[str]] = {}
        eligibility_mode = self._mapping_eligibility_mode()
        for market in self._market_provider():
            if (
                getattr(self._config.routes, "polymarket_predict", False)
                and self._predict_fun is not None
                and self._execution is not None
                and market.polymarket_token_id
                and market.predict_fun_token_id
                and market.venue_b_label == "Predict.fun"
                and is_live_mapping_eligible(market, eligibility_mode, "polymarket_predict")
            ):
                targets.setdefault("Polymarket", set()).add(market.polymarket_token_id)
                targets.setdefault("Predict.fun", set()).add(market.predict_fun_token_id)
            if (
                getattr(self._config.routes, "polymarket_sx", False)
                and self._sx_bet is not None
                and self._sx_execution is not None
                and market.polymarket_token_id
                and market.predict_fun_token_id
                and market.venue_b_label == "SX Bet"
                and is_live_mapping_eligible(market, eligibility_mode, "polymarket_sx")
            ):
                targets.setdefault("Polymarket", set()).add(market.polymarket_token_id)
                targets.setdefault("SX Bet", set()).add(market.predict_fun_token_id)
            if (
                self._config.routes.polymarket_myriad
                and self._myriad is not None
                and self._myriad_execution is not None
                and market.polymarket_token_id
                and market.myriad_market_id
                and is_live_mapping_eligible(market, eligibility_mode, "polymarket_myriad")
            ):
                targets.setdefault("Polymarket", set()).add(market.polymarket_token_id)
                targets.setdefault("Myriad", set()).add(f"{market.myriad_market_id}:{market.myriad_side.value}")
            if (
                getattr(self._config.routes, "predict_myriad", False)
                and self._predict_fun is not None
                and self._myriad is not None
                and self._predict_myriad_execution is not None
                and market.predict_fun_token_id
                and market.myriad_market_id
                and market.venue_b_label == "Predict.fun"
                and is_live_mapping_eligible(market, eligibility_mode, "predict_myriad")
            ):
                targets.setdefault("Predict.fun", set()).add(market.predict_fun_token_id)
                predict_myriad_token = myriad_execution_token_for_route(market, "predict_myriad")
                if predict_myriad_token:
                    targets.setdefault("Myriad", set()).add(predict_myriad_token)
            if (
                getattr(self._config.routes, "predict_sx", False)
                and self._predict_fun is not None
                and self._sx_bet is not None
                and self._predict_sx_execution is not None
                and market.venue_a_label == "Predict.fun"
                and market.venue_b_label == "SX Bet"
                and market.polymarket_token_id
                and market.predict_fun_token_id
                and is_live_mapping_eligible(market, eligibility_mode, "predict_sx")
            ):
                targets.setdefault("Predict.fun", set()).add(market.polymarket_token_id)
                targets.setdefault("SX Bet", set()).add(market.predict_fun_token_id)
            if (
                getattr(self._config.routes, "sx_myriad", False)
                and self._sx_bet is not None
                and self._myriad is not None
                and self._sx_myriad_execution is not None
                and market.predict_fun_token_id
                and market.myriad_market_id
                and market.venue_b_label == "SX Bet"
                and is_live_mapping_eligible(market, eligibility_mode, "sx_myriad")
            ):
                targets.setdefault("SX Bet", set()).add(market.predict_fun_token_id)
                sx_myriad_token = myriad_execution_token_for_route(market, "sx_myriad")
                if sx_myriad_token:
                    targets.setdefault("Myriad", set()).add(sx_myriad_token)
        return targets

    def _sync_client_targets(self, client: BinaryMarketClient | None, token_ids: set[str]) -> None:
        if client is None:
            return
        sync_targets = getattr(client, "sync_market_data_targets", None)
        if callable(sync_targets):
            sync_targets(token_ids)

    def _has_paused_execution_router(self) -> bool:
        return any(router is not None and router.is_paused for router in self._execution_routers())

    def _execution_routers(self) -> tuple[ExecutionRouter | None, ...]:
        return (
            self._execution,
            self._sx_execution,
            self._myriad_execution,
            self._predict_myriad_execution,
            self._predict_sx_execution,
            self._sx_myriad_execution,
        )

    def _mapping_eligibility_mode(self) -> ExecutionMode:
        if not self._config.execution_mode.submits_orders and self._has_paused_execution_router():
            return ExecutionMode.CANARY
        return self._config.execution_mode

    def _plan_polymarket_pair(
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
    ) -> _PlannedEvaluation:
        active_route = route_key(first_label, second_label)
        return _PlannedEvaluation(
            route=active_route,
            run=partial(
                self._evaluate_polymarket_pair,
                market=market,
                first_leg=first_leg,
                second_leg=second_leg,
                execution=execution,
                first_token_id=first_token_id,
                first_side=first_side,
                second_token_id=second_token_id,
                second_side=second_side,
                first_label=first_label,
                second_label=second_label,
                max_slippage_pct=max_slippage_pct,
                first_amm_pool=first_amm_pool,
                second_amm_pool=second_amm_pool,
            ),
            targets=((first_label, first_token_id), (second_label, second_token_id)),
        )

    async def _evaluate_polymarket_pair(
        self,
        fixed_chain_cost_usd: float,
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
        active_route = route_key(first_label, second_label)
        if not is_live_mapping_eligible(market, self._mapping_eligibility_mode(), active_route):
            self._record_signal_evaluation(active_route, "mapping_ineligible")
            LOGGER.warning(
                "market_route_rejected_unverified_mapping",
                extra={
                    "_symbol": market.symbol,
                    "_mapping_status": market.mapping_status.value,
                    "_route": active_route,
                },
            )
            return
        if not first_token_id or not second_token_id:
            self._record_signal_evaluation(active_route, "missing_token")
            return
        first_book: OrderBook | None = None
        second_book: OrderBook | None = None
        try:
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
        except OrderBookStaleException:
            self._record_signal_evaluation(active_route, "stale_book")
            raise
        except OrderBookUnavailableException:
            self._record_signal_evaluation(active_route, "unavailable_book")
            raise
        now = time.time()
        invalid_books = [
            label
            for label, book in ((first_label, first_book), (second_label, second_book))
            if book is not None and book.status is not MarketDataStatus.VALID
        ]
        if invalid_books:
            self._record_signal_evaluation(active_route, "invalid_book")
            LOGGER.warning(
                "signal_evaluation_invalid_book_rejected",
                extra={"_symbol": market.symbol, "_venues": invalid_books},
            )
            return
        stale_books = [
            (label, max(0.0, now - book.timestamp))
            for label, client, token_id, book in (
                (first_label, first_leg, first_token_id, first_book),
                (second_label, second_leg, second_token_id, second_book),
            )
            if book is not None
            and not client.is_order_book_execution_fresh(
                token_id,
                book,
                self._config.max_orderbook_age_seconds,
            )
        ]
        if stale_books:
            self._record_signal_evaluation(active_route, "stale_book")
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
        fee_quotes = await self._fee_quotes(
            first_leg,
            second_leg,
            first_label,
            second_label,
            first_token_id,
            second_token_id,
            market.condition_id,
        )
        if fee_quotes is None:
            self._record_signal_evaluation(active_route, "constraints_unavailable")
            return
        first_fee_quote, second_fee_quote = fee_quotes
        target_notional = self._target_leg_notional_usd()
        required_depth = target_notional * self._config.spread_policy.depth_buffer
        dynamic_threshold = max(
            self._config.min_net_spread,
            self._config.spread_policy.threshold_for(active_route),
        )
        try:
            metrics = calculate_spread_metrics(
                polymarket_book=first_book,
                predict_fun_book=second_book,
                max_order_size_usd=target_notional,
                min_net_spread=dynamic_threshold,
                max_slippage_pct=max_slippage_pct,
                polymarket_amm_pool=effective_first_amm,
                polymarket_side=first_side,
                predict_fun_amm_pool=effective_second_amm,
                predict_fun_side=second_side,
                polymarket_fee_quote=first_fee_quote,
                predict_fun_fee_quote=second_fee_quote,
                required_executable_depth_usd=required_depth,
                fixed_chain_cost_usd=fixed_chain_cost_usd,
                max_price_impact=self._config.max_production_price_impact,
            )
        except ValueError as exc:
            self._record_signal_evaluation(active_route, "liquidity_rejected")
            LOGGER.debug(
                "liquidity_guard_rejected_market",
                extra={"_symbol": market.symbol, "_venue": f"{first_label}<->{second_label}", "_reason": str(exc)},
            )
            return
        self._mark_recent_executable(
            active_route,
            ((first_label, first_token_id), (second_label, second_token_id)),
            metrics.net_spread,
        )
        # Calibration measures executable market-data quality, not strategy
        # eligibility. Low-edge samples are still valid latency observations.
        calibration_market_key = f"{first_label}:{first_token_id}|{second_label}:{second_token_id}"
        self._record_route_calibration(
            active_route,
            calibration_market_key,
            metrics.net_spread,
            first_book,
            second_book,
            effective_first_amm,
            effective_second_amm,
        )
        if metrics.net_spread <= dynamic_threshold:
            self._record_signal_evaluation(active_route, "below_min_net_spread", metrics.net_spread)
            return
        try:
            plan = build_position_plan(
                polymarket_book=first_book,
                predict_fun_book=second_book,
                max_order_size_usd=target_notional,
                max_slippage_pct=max_slippage_pct,
                polymarket_amm_pool=effective_first_amm,
                polymarket_side=first_side,
                predict_fun_amm_pool=effective_second_amm,
                predict_fun_side=second_side,
                polymarket_fee_quote=first_fee_quote,
                predict_fun_fee_quote=second_fee_quote,
                required_executable_depth_usd=required_depth,
                max_price_impact=self._config.max_production_price_impact,
            )
        except ValueError as exc:
            self._record_signal_evaluation(active_route, "plan_rejected", metrics.net_spread)
            LOGGER.debug(
                "liquidity_guard_rejected_market",
                extra={"_symbol": market.symbol, "_venue": f"{first_label}<->{second_label}", "_reason": str(exc)},
            )
            return
        variable_cost = float(plan.polymarket_fee_usd + plan.predict_fun_fee_usd)
        adverse_move = self._config.spread_policy.adverse_move_p95_pct_by_route.get(
            active_route,
            self._config.spread_policy.adverse_move_p95_pct,
        )
        self._record_market_economics(
            active_route,
            {
                "first_executable_depth_usd": (
                    float(executable_depth_usd(first_book)) if first_book is not None else target_notional
                ),
                "second_executable_depth_usd": (
                    float(executable_depth_usd(second_book)) if second_book is not None else target_notional
                ),
                "fee_cost_usd": variable_cost,
                "chain_cost_usd": fixed_chain_cost_usd,
                "expected_profit_usd": metrics.expected_net_profit_usd,
                "dynamic_threshold": dynamic_threshold,
                "adverse_move_reserve": adverse_move + self._config.spread_policy.safety_buffer_pct,
            },
        )
        minimum_profit = max(
            self._config.spread_policy.min_expected_profit_usd,
            variable_cost * 2,
        )
        if metrics.expected_net_profit_usd < minimum_profit:
            self._record_signal_evaluation(active_route, "profit_floor_rejected", metrics.net_spread)
            LOGGER.debug(
                "signal_expected_profit_rejected",
                extra={
                    "_route": active_route,
                    "_symbol": market.symbol,
                    "_expected_profit_usd": metrics.expected_net_profit_usd,
                    "_minimum_profit_usd": minimum_profit,
                    "_dynamic_threshold": dynamic_threshold,
                    "_required_depth_usd": required_depth,
                },
            )
            return
        signal = ArbitrageSignal(
            market=market,
            plan=plan,
            metrics=metrics,
            polymarket_price=float(plan.polymarket_capital_usd / plan.polymarket_contracts),
            predict_fun_price=float(plan.predict_fun_capital_usd / plan.predict_fun_contracts),
            raw_books={
                first_label: _book_debug_payload(first_book, first_token_id, first_side),
                second_label: _book_debug_payload(second_book, second_token_id, second_side),
            },
        )
        self._record_signal_evaluation(active_route, "eligible_signal", metrics.net_spread)
        if execution.is_paused:
            LOGGER.info(
                "eligible_signal_observed_while_execution_paused",
                extra={"_route": active_route, "_symbol": market.symbol, "_net_spread": metrics.net_spread},
            )
            return
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
        if venue_label == "SX Bet":
            fee_rate_bps = self._config.sx_bet.taker_fee_bps
            if market.venue_a_label != "Predict.fun" and market.predict_fun_fee_rate_bps is not None:
                fee_rate_bps = market.predict_fun_fee_rate_bps
            return float(Decimal(fee_rate_bps) / Decimal(10_000))
        if venue_label == "Myriad":
            return self._config.myriad_markets.trading_fee_pct
        raise ValueError(f"Unsupported venue label: {venue_label}")

    async def _fee_quotes(
        self,
        first_leg: BinaryMarketClient,
        second_leg: BinaryMarketClient,
        first_label: str,
        second_label: str,
        first_token_id: str,
        second_token_id: str,
        polymarket_condition_id: str | None,
    ) -> tuple[VenueFeeQuote, VenueFeeQuote] | None:
        try:
            first_constraints, second_constraints = await asyncio.gather(
                first_leg.get_market_constraints(
                    first_token_id,
                    polymarket_condition_id if first_label == "Polymarket" else None,
                ),
                second_leg.get_market_constraints(
                    second_token_id,
                    polymarket_condition_id if second_label == "Polymarket" else None,
                ),
            )
        except Exception:
            LOGGER.exception("signal_fee_constraints_lookup_failed")
            return None
        if first_constraints is None or second_constraints is None:
            return None
        first_quote, second_quote = await asyncio.gather(
            first_leg.get_fee_quote(first_token_id, Decimal("0.5"), first_constraints),
            second_leg.get_fee_quote(second_token_id, Decimal("0.5"), second_constraints),
        )
        if (
            first_quote is None
            or second_quote is None
            or not first_quote.verified
            or not second_quote.verified
        ):
            return None
        return first_quote, second_quote

    def _second_venue_slippage_pct(self, venue_label: str) -> float:
        if venue_label == "SX Bet":
            return self._config.sx_bet.max_slippage_pct
        return self._config.predict_fun.max_slippage_pct


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
