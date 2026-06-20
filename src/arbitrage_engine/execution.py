from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

from .config import AppConfig
from .connectors.base import BinaryMarketClient
from .connectors.web3_base import TransactionTimeoutException
from .models import (
    ArbitrageSignal,
    BinarySide,
    ExecutionReport,
    ExitSignal,
    MarketSpec,
    MarketDataStatus,
    OpenPosition,
    OrderIntent,
    OrderIntentStatus,
    PositionPlan,
    SpreadMetrics,
    position_key,
)
from .positions import PositionLedger
from .quant import (
    calculate_binary_position_profit,
    calculate_realized_position_profit,
    calculate_spread_metrics,
    is_binary_signal_allowed,
)
from .risk import GlobalRiskController
from .telegram import TelegramNotifier, format_exit_message
from .utils.ids import uuid7

if TYPE_CHECKING:
    from .database import ProductionRepository

LOGGER = logging.getLogger(__name__)
@dataclass(frozen=True)
class ExitLegResult:
    order_id: str
    report: ExecutionReport | None
    error: Exception | None = None


@dataclass(frozen=True)
class EntryLegResult:
    order_id: str
    report: ExecutionReport | None
    error: Exception | None = None
    submit_started_ns: int | None = None
    acknowledged_ns: int | None = None


class ExecutionRouter:
    def __init__(
        self,
        config: AppConfig,
        polymarket: BinaryMarketClient,
        predict_fun: BinaryMarketClient,
        telegram: TelegramNotifier,
        ledger: PositionLedger | None = None,
        first_leg_label: str = "Polymarket",
        second_leg_label: str = "Predict.fun",
        first_leg_fill_timeout_ms: int | None = None,
        second_leg_fill_timeout_ms: int | None = None,
        market_locks: dict[str, asyncio.Lock] | None = None,
        capacity_lock: asyncio.Lock | None = None,
        pending_markets: set[str] | None = None,
        balance_cache: dict[str, float] | None = None,
        capital_reservations: dict[str, float] | None = None,
        optimistic_debits: dict[str, float] | None = None,
        state_path: str | Path | None = None,
        risk_controller: GlobalRiskController | None = None,
        repository: ProductionRepository | None = None,
    ) -> None:
        self._config = config
        self._first_leg = polymarket
        self._second_leg = predict_fun
        self._telegram = telegram
        self._ledger = ledger or PositionLedger()
        self._last_signal_alert_at: dict[str, datetime] = {}
        self._last_exit_alert_at: dict[str, datetime] = {}
        self._first_leg_label = first_leg_label
        self._second_leg_label = second_leg_label
        self._first_leg_fill_timeout_ms = first_leg_fill_timeout_ms or config.polymarket_fill_timeout_ms
        self._second_leg_fill_timeout_ms = second_leg_fill_timeout_ms or config.predict_fun_fill_timeout_ms
        self._balance_cache = balance_cache if balance_cache is not None else {}
        self._capital_reservations = capital_reservations if capital_reservations is not None else {}
        self._optimistic_debits = optimistic_debits if optimistic_debits is not None else {}
        self._balance_updater_task: asyncio.Task[None] | None = None
        self._last_low_balance_alert_at = 0.0
        self._consecutive_api_errors = 0
        self._market_locks = market_locks if market_locks is not None else {}
        self._capacity_lock = capacity_lock or asyncio.Lock()
        self._pending_markets = pending_markets if pending_markets is not None else set()
        self._risk = risk_controller or GlobalRiskController(
            config.max_daily_loss_usd,
            config.max_consecutive_api_errors,
            state_path,
        )
        self._repository = repository
        self._active_orders: dict[tuple[int, str], BinaryMarketClient] = {}
        self._order_timestamps: deque[float] = deque()
        self._risk.register_pause_callback(self._cancel_active_orders_and_clear_pending)

    @property
    def ledger(self) -> PositionLedger:
        return self._ledger

    @property
    def is_paused(self) -> bool:
        return self._risk.is_paused()

    def net_exit_values(self, market: MarketSpec, first_price: float, second_price: float) -> tuple[float, float]:
        return (
            first_price * (1.0 - self._venue_fee_pct(self._first_leg_label, market)),
            second_price * (1.0 - self._venue_fee_pct(self._second_leg_label, market)),
        )

    def gross_entry_values(self, market: MarketSpec, first_price: float, second_price: float) -> tuple[float, float]:
        return (
            first_price * (1.0 + self._venue_fee_pct(self._first_leg_label, market)),
            second_price * (1.0 + self._venue_fee_pct(self._second_leg_label, market)),
        )

    async def start(self) -> None:
        if not self._config.execution_mode.submits_orders:
            return
        await self._refresh_balances()
        if self._balance_updater_task is None or self._balance_updater_task.done():
            self._balance_updater_task = asyncio.create_task(self._run_balance_updater())

    async def close(self) -> None:
        if self._balance_updater_task is not None:
            self._balance_updater_task.cancel()
            await asyncio.gather(self._balance_updater_task, return_exceptions=True)
            self._balance_updater_task = None
        await self._telegram.close()

    async def ensure_balances(self) -> bool:
        if not self._balance_cache:
            await self.start()
        first_balance = self._effective_balance(self._first_leg_label)
        second_balance = self._effective_balance(self._second_leg_label)
        required = self._config.position_size_usd / 2.0
        ok = first_balance >= required and second_balance >= required
        now = time.monotonic()
        if not ok and now - self._last_low_balance_alert_at >= 600:
            self._last_low_balance_alert_at = now
            await self._telegram.send_html(
                "⚠️ <b>ARBITRAGE ENGINE STOPPED</b>\n"
                f"Недостатній баланс: {self._first_leg_label} ${first_balance:.2f}, "
                f"{self._second_leg_label} ${second_balance:.2f}. Required per leg: ${required:.2f}."
            )
        return ok

    async def handle_signal(self, signal: ArbitrageSignal) -> None:
        signal_received_ns = time.perf_counter_ns()
        if self._risk.is_paused():
            LOGGER.error(
                "execution_circuit_open",
                extra={"_symbol": signal.market.symbol, "_reason": self._risk.pause_reason},
            )
            return
        if not is_binary_signal_allowed(signal.metrics, self._config.min_net_spread):
            LOGGER.info(
                "binary_signal_rejected",
                extra={
                    "_combined_cost": signal.metrics.combined_cost_per_payout,
                    "_net_spread": signal.metrics.net_spread,
                },
            )
            return
        market_key = signal.market.symbol
        market_lock = self._market_locks.setdefault(market_key, asyncio.Lock())
        async with market_lock:
            await self._handle_signal_locked(signal, market_key, signal_received_ns)

    async def _handle_signal_locked(self, signal: ArbitrageSignal, market_key: str, signal_received_ns: int) -> None:
        reserved = False
        capital_reserved = False
        async with self._capacity_lock:
            if self._risk.is_paused():
                return
            if self._ledger.has(position_key(signal.market)) or self._has_open_market(market_key):
                LOGGER.info("signal_skipped_existing_position", extra={"_symbol": signal.market.symbol})
                return
            active_markets = {position.market.symbol for position in self._ledger.all()} | self._pending_markets
            active_count = len(active_markets)
            if active_count >= self._config.max_open_positions:
                LOGGER.warning(
                    "signal_skipped_max_open_positions",
                    extra={"_symbol": signal.market.symbol, "_limit": self._config.max_open_positions},
                )
                return
            if market_key in self._pending_markets:
                LOGGER.info("signal_skipped_pending_market", extra={"_symbol": signal.market.symbol})
                return
            if not self._risk_limits_allow(signal):
                return
            self._pending_markets.add(market_key)
            reserved = True
        try:
            if not self._config.execution_mode.submits_orders:
                if self._should_send_signal_alert(signal):
                    await self._telegram.send_signal(
                        signal,
                        is_test=True,
                        min_net_spread=self._config.min_net_spread,
                    )
                LOGGER.info(
                    "dry_run_signal",
                    extra={"_symbol": signal.market.symbol, "_net_spread": signal.metrics.net_spread},
                )
                return
            if not await self._reserve_signal_capital(signal):
                return
            capital_reserved = True
            reserved_ns = time.perf_counter_ns()
            if not await self._market_constraints_guard(signal):
                return
            if not await self._preflight_price_guard(signal):
                return
            if self._risk.is_paused():
                LOGGER.warning("signal_aborted_global_risk_pause", extra={"_symbol": signal.market.symbol})
                return
            if self._should_send_signal_alert(signal):
                await self._telegram.send_signal(
                    signal,
                    is_test=False,
                    min_net_spread=self._config.min_net_spread,
                )

            errors_before_execution = self._consecutive_api_errors
            try:
                self._record_order_attempts(2)
                await self._execute_production(signal, signal_received_ns, reserved_ns)
            except Exception:
                await self._record_api_error()
                raise
            else:
                if self._consecutive_api_errors == errors_before_execution:
                    self._consecutive_api_errors = 0
                    await self._risk.reset_api_errors()
        finally:
            if reserved or capital_reserved:
                async with self._capacity_lock:
                    if capital_reserved:
                        self._release_signal_capital(signal)
                    self._pending_markets.discard(market_key)

    def _has_open_market(self, market_key: str) -> bool:
        return any(position.market.symbol == market_key for position in self._ledger.all())

    def _risk_limits_allow(self, signal: ArbitrageSignal) -> bool:
        positions = self._ledger.all()
        total_notional = sum(
            position.polymarket_contracts * position.polymarket_entry_price
            + position.predict_fun_contracts * position.predict_fun_entry_price
            for position in positions
        )
        if total_notional + signal.plan.total_cost_usd > self._config.max_total_notional_usd:
            LOGGER.warning("risk_total_notional_rejected", extra={"_symbol": signal.market.symbol})
            return False
        if signal.plan.total_cost_usd > self._config.max_market_exposure_usd:
            LOGGER.warning("risk_market_exposure_rejected", extra={"_symbol": signal.market.symbol})
            return False
        venue_exposure: dict[str, float] = {}
        for position in positions:
            venue_exposure[position.market.venue_a_label] = venue_exposure.get(position.market.venue_a_label, 0.0) + (
                position.polymarket_contracts * position.polymarket_entry_price
            )
            venue_exposure[position.market.venue_b_label] = venue_exposure.get(position.market.venue_b_label, 0.0) + (
                position.predict_fun_contracts * position.predict_fun_entry_price
            )
        required = {
            self._first_leg_label: signal.plan.polymarket_capital_usd,
            self._second_leg_label: signal.plan.predict_fun_capital_usd,
        }
        if any(
            venue_exposure.get(venue, 0.0) + amount > self._config.max_venue_exposure_usd
            for venue, amount in required.items()
        ):
            LOGGER.warning("risk_venue_exposure_rejected", extra={"_symbol": signal.market.symbol})
            return False
        unresolved = sum(
            position.polymarket_contracts * position.polymarket_entry_price
            + position.predict_fun_contracts * position.predict_fun_entry_price
            for position in positions
            if position.status in {"entry_pending", "unwind_pending", "partial_exit_pending", "manual_review"}
        )
        if unresolved > self._config.max_unresolved_exposure_usd:
            LOGGER.error("risk_unresolved_exposure_rejected", extra={"_exposure_usd": unresolved})
            return False
        now = time.monotonic()
        while self._order_timestamps and now - self._order_timestamps[0] >= 60.0:
            self._order_timestamps.popleft()
        if len(self._order_timestamps) + 2 > self._config.max_orders_per_minute:
            LOGGER.warning("risk_order_rate_rejected", extra={"_orders_last_minute": len(self._order_timestamps)})
            return False
        return True

    def _record_order_attempts(self, count: int) -> None:
        now = time.monotonic()
        self._order_timestamps.extend(now for _ in range(count))

    async def _execute_production(
        self,
        signal: ArbitrageSignal,
        signal_received_ns: int | None = None,
        reserved_ns: int | None = None,
    ) -> None:
        await self._save_entry_pending(signal)
        raw_first, raw_second = await asyncio.gather(
            self._submit_entry_leg(
                client=self._first_leg,
                market=signal.market,
                venue_label=self._first_leg_label,
                token_id=signal.market.polymarket_token_id,
                side=signal.market.polymarket_side,
                contracts=signal.plan.polymarket_contracts,
                max_price=signal.polymarket_price,
                capital_usd=signal.plan.polymarket_capital_usd + signal.plan.polymarket_fee_usd,
                timeout_ms=self._first_leg_fill_timeout_ms,
                condition_id=signal.market.condition_id,
                tick_size=signal.market.tick_size,
                neg_risk=signal.market.neg_risk,
            ),
            self._submit_entry_leg(
                client=self._second_leg,
                market=signal.market,
                venue_label=self._second_leg_label,
                token_id=signal.market.predict_fun_token_id,
                side=signal.market.predict_fun_side,
                contracts=signal.plan.predict_fun_contracts,
                max_price=signal.predict_fun_price,
                capital_usd=signal.plan.predict_fun_capital_usd + signal.plan.predict_fun_fee_usd,
                timeout_ms=self._second_leg_fill_timeout_ms,
                neg_risk=signal.market.predict_fun_neg_risk if self._second_leg_label == "Predict.fun" else None,
            ),
            return_exceptions=True,
        )
        first = self._normalize_entry_result(raw_first, self._first_leg_label)
        second = self._normalize_entry_result(raw_second, self._second_leg_label)
        self._log_pipeline_latency(signal, first, second, signal_received_ns, reserved_ns)
        if first.error is not None or second.error is not None:
            await self._record_api_error()

        first_filled = first.report.amount_filled if first.report is not None else 0.0
        second_filled = second.report.amount_filled if second.report is not None else 0.0
        first_entry_price = (
            first.report.avg_price if first.report is not None and first.report.avg_price > 0 else signal.polymarket_price
        )
        second_entry_price = (
            second.report.avg_price
            if second.report is not None and second.report.avg_price > 0
            else signal.predict_fun_price
        )
        matched = min(first_filled, second_filled)
        unmatched_first = max(0.0, first_filled - matched)
        unmatched_second = max(0.0, second_filled - matched)
        first_unwound, second_unwound = await asyncio.gather(
            self._try_unwind_first_leg(signal, unmatched_first) if unmatched_first > 1e-9 else _zero_async(),
            self._try_unwind_second_leg(signal, unmatched_second) if unmatched_second > 1e-9 else _zero_async(),
        )
        pending_first = max(0.0, unmatched_first - first_unwound)
        pending_second = max(0.0, unmatched_second - second_unwound)

        if unmatched_first > 1e-9 or unmatched_second > 1e-9:
            await self._telegram.send_html(
                "⚠️ <b>PARALLEL ENTRY IMBALANCE</b>\n"
                f"{self._first_leg_label} unmatched: {unmatched_first:.6f}; unwound: {first_unwound:.6f}.\n"
                f"{self._second_leg_label} unmatched: {unmatched_second:.6f}; unwound: {second_unwound:.6f}."
            )

        if pending_first > 1e-9 or pending_second > 1e-9:
            await self._save_unwind_pending(
                signal,
                first.order_id,
                second.order_id,
                matched,
                pending_first,
                pending_second,
                first_entry_price,
                second_entry_price,
            )
            return
        if matched <= 1e-9:
            await self._remove_position(position_key(signal.market))
            LOGGER.warning("parallel_entry_no_matched_fill", extra={"_symbol": signal.market.symbol})
            return

        position = self._open_position_from_amounts(
            signal,
            first.order_id,
            second.order_id,
            matched,
            first_entry_price,
            second_entry_price,
        )
        await self._add_position(position)
        LOGGER.info(
            "binary_signal_executed",
            extra={"_first_order_id": first.order_id, "_second_order_id": second.order_id},
        )
        await self._telegram.send_position_opened(signal, position)

    async def _submit_entry_leg(
        self,
        *,
        client: BinaryMarketClient,
        market: MarketSpec,
        venue_label: str,
        token_id: str,
        side: BinarySide,
        contracts: float,
        max_price: float,
        capital_usd: float,
        timeout_ms: int,
        condition_id: str | None = None,
        tick_size: str | None = None,
        neg_risk: bool | None = None,
    ) -> EntryLegResult:
        order_id = "failed-before-order"
        client_order_id = str(uuid7())
        submit_started_ns = time.perf_counter_ns()
        acknowledged_ns: int | None = None
        final_intent_status: OrderIntentStatus | None = None
        if self._repository is not None:
            await self._repository.create_order_intent(
                OrderIntent(
                    client_order_id=client_order_id,
                    route=f"{self._first_leg_label}:{self._second_leg_label}",
                    market_key=position_key(market),
                    venue=venue_label,
                    token_id=token_id,
                    binary_side=side,
                    action="BUY",
                    quantity=Decimal(str(contracts)),
                    limit_price=Decimal(str(max_price)),
                )
            )
            await self._repository.update_order_intent(client_order_id, OrderIntentStatus.SUBMITTING)
        try:
            order_id = await client.buy(
                token_id=token_id,
                side=side,
                contracts=contracts,
                max_price=max_price,
                condition_id=condition_id,
                tick_size=tick_size,
                neg_risk=neg_risk,
            )
            acknowledged_ns = time.perf_counter_ns()
            if self._repository is not None:
                await self._repository.update_order_intent(
                    client_order_id,
                    OrderIntentStatus.ACKNOWLEDGED,
                    venue_order_id=order_id,
                )
            active_key = (id(client), order_id)
            self._active_orders[active_key] = client
            report = await client.wait_filled(order_id, timeout_ms)
            if not report.is_filled:
                report = await self._cancel_and_reconcile(client, order_id, report)
            report = replace(
                report,
                client_order_id=client_order_id,
                venue_order_id=order_id,
            )
            if self._repository is not None:
                final_intent_status = _intent_status_from_report(report)
                await self._repository.update_order_intent(
                    client_order_id,
                    final_intent_status,
                    venue_order_id=order_id,
                )
                if final_intent_status is OrderIntentStatus.UNKNOWN:
                    await self._risk.pause(
                        f"unknown order outcome: {venue_label} client_order_id={client_order_id}"
                    )
            self._debit_reported_fill(venue_label, market, report, max_price, capital_usd)
            return EntryLegResult(order_id, report, None, submit_started_ns, acknowledged_ns)
        except asyncio.CancelledError:
            if self._repository is not None:
                final_intent_status = OrderIntentStatus.UNKNOWN
                await self._repository.update_order_intent(
                    client_order_id,
                    OrderIntentStatus.UNKNOWN,
                    venue_order_id=None if order_id == "failed-before-order" else order_id,
                    error="entry submission cancelled during shutdown",
                )
            if order_id != "failed-before-order":
                try:
                    await client.cancel_order(order_id)
                except Exception:
                    LOGGER.exception("entry_cancel_during_shutdown_failed", extra={"_order_id": order_id})
            raise
        except Exception as exc:
            if isinstance(exc, TransactionTimeoutException):
                await self._telegram.send_html(
                    "🚨 <b>NONCE/TRANSACTION TIMEOUT</b>\n"
                    f"Venue: {venue_label}; order: {order_id}; timeout: {timeout_ms}ms; reason: {exc}."
                )
            reconciled_report: ExecutionReport | None = None
            if order_id != "failed-before-order":
                try:
                    await client.cancel_order(order_id)
                except Exception:
                    LOGGER.exception("entry_cancel_after_error_failed", extra={"_order_id": order_id})
                try:
                    reconciled_report = await client.wait_filled(order_id, self._config.cancel_reconcile_timeout_ms)
                except Exception:
                    LOGGER.exception("entry_reconcile_after_error_failed", extra={"_order_id": order_id})
            if reconciled_report is not None:
                reconciled_report = replace(
                    reconciled_report,
                    client_order_id=client_order_id,
                    venue_order_id=order_id,
                )
                self._debit_reported_fill(venue_label, market, reconciled_report, max_price, capital_usd)
            if self._repository is not None:
                final_intent_status = (
                    _intent_status_from_report(reconciled_report)
                    if reconciled_report is not None
                    else OrderIntentStatus.UNKNOWN
                )
                await self._repository.update_order_intent(
                    client_order_id,
                    final_intent_status,
                    venue_order_id=None if order_id == "failed-before-order" else order_id,
                    error=str(exc),
                )
                if final_intent_status is OrderIntentStatus.UNKNOWN:
                    await self._risk.pause(
                        f"unknown order outcome: {venue_label} client_order_id={client_order_id}"
                    )
            return EntryLegResult(order_id, reconciled_report, exc, submit_started_ns, acknowledged_ns)
        finally:
            if order_id != "failed-before-order":
                self._active_orders.pop((id(client), order_id), None)
                forget_order = getattr(client, "forget_order", None)
                if callable(forget_order) and (
                    self._repository is None
                    or final_intent_status
                    in {OrderIntentStatus.FILLED, OrderIntentStatus.CANCELLED}
                ):
                    forget_order(order_id)

    async def _cancel_and_reconcile(
        self,
        client: BinaryMarketClient,
        order_id: str,
        previous: ExecutionReport,
    ) -> ExecutionReport:
        try:
            await client.cancel_order(order_id)
        except Exception:
            LOGGER.exception("entry_cancel_failed_reconciling", extra={"_order_id": order_id})
        try:
            current = await client.wait_filled(order_id, self._config.cancel_reconcile_timeout_ms)
        except Exception:
            LOGGER.exception("entry_post_cancel_reconcile_failed", extra={"_order_id": order_id})
            return previous
        return current if current.amount_filled >= previous.amount_filled else previous

    def _debit_reported_fill(
        self,
        venue_label: str,
        market: MarketSpec,
        report: ExecutionReport,
        fallback_price: float,
        reserved_capital_usd: float,
    ) -> None:
        if not report.has_fill:
            return
        price = report.avg_price if report.avg_price > 0 else fallback_price
        actual = min(
            reserved_capital_usd,
            report.amount_filled * price * (1.0 + self._venue_fee_pct(venue_label, market)),
        )
        self._debit_cached_balance(venue_label, actual)

    @staticmethod
    def _normalize_entry_result(result: EntryLegResult | BaseException, venue_label: str) -> EntryLegResult:
        if isinstance(result, BaseException):
            if isinstance(result, asyncio.CancelledError):
                raise result
            LOGGER.error("entry_leg_failed", extra={"_venue": venue_label, "_error": str(result)})
            return EntryLegResult("failed-before-order", None, result if isinstance(result, Exception) else None)
        if result.error is not None:
            LOGGER.error("entry_leg_failed", extra={"_venue": venue_label, "_error": str(result.error)})
        return result

    async def handle_exit_signal(self, signal: ExitSignal) -> None:
        if not self._config.execution_mode.submits_orders:
            key = position_key(signal.position.market)
            now = datetime.now(timezone.utc)
            last_sent = self._last_exit_alert_at.get(key)
            if last_sent is None or (now - last_sent).total_seconds() >= self._config.signal_alert_cooldown_seconds:
                self._last_exit_alert_at[key] = now
                await self._telegram.send_html(format_exit_message(signal, is_test=True))
            return

        await self._close_position_legs(
            signal.position,
            polymarket_exit_price=signal.polymarket_exit_price,
            predict_fun_exit_price=signal.predict_fun_exit_price,
        )

    async def retry_partial_exit(self, position: OpenPosition) -> None:
        if position.status != "partial_exit_pending":
            return
        poly_price = position.polymarket_exit_price
        predict_price = position.predict_fun_exit_price
        if not position.polymarket_closed:
            poly_price = (await self._first_leg.watch_order_book(position.market.polymarket_token_id)).best_bid.price
        if not position.predict_fun_closed:
            predict_price = (await self._second_leg.watch_order_book(position.market.predict_fun_token_id)).best_bid.price
        await self._close_position_legs(
            position,
            polymarket_exit_price=poly_price or 0.01,
            predict_fun_exit_price=predict_price or 0.01,
        )

    async def retry_pending_unwind(self, position: OpenPosition) -> None:
        if position.status != "unwind_pending":
            return
        signal = _signal_from_unwind_position(position)
        first_pending = position.unmatched_first_contracts
        second_pending = position.unmatched_second_contracts
        first_filled, second_filled = await asyncio.gather(
            self._try_unwind_first_leg(signal, first_pending) if first_pending > 1e-9 else _zero_async(),
            self._try_unwind_second_leg(signal, second_pending) if second_pending > 1e-9 else _zero_async(),
        )
        attempts = position.polymarket_unwind_attempts + 1
        remaining_first = max(0.0, first_pending - first_filled)
        remaining_second = max(0.0, second_pending - second_filled)
        polymarket_contracts = max(0.0, position.polymarket_contracts - first_filled)
        predict_fun_contracts = max(0.0, position.predict_fun_contracts - second_filled)
        if remaining_first <= 1e-9 and remaining_second <= 1e-9:
            matched = min(polymarket_contracts, predict_fun_contracts)
            if matched > 1e-9:
                await self._add_position(
                    replace(
                        position,
                        polymarket_contracts=matched,
                        predict_fun_contracts=matched,
                        status="open",
                        polymarket_unwind_attempts=attempts,
                        unmatched_first_contracts=0.0,
                        unmatched_second_contracts=0.0,
                    )
                )
            else:
                await self._remove_position(position_key(position.market))
            await self._telegram.send_html(
                "✅ <b>[AUTO-UNWIND COMPLETED]</b>\n"
                f"Пара: {position.market.symbol}\n"
                f"Attempts: {attempts}\n"
                "Unhedged exposure was closed automatically."
            )
            return
        await self._add_position(
            replace(
                position,
                polymarket_contracts=polymarket_contracts,
                predict_fun_contracts=predict_fun_contracts,
                polymarket_unwind_attempts=attempts,
                unmatched_first_contracts=remaining_first,
                unmatched_second_contracts=remaining_second,
            )
        )

    async def _close_position_legs(
        self,
        position: OpenPosition,
        *,
        polymarket_exit_price: float,
        predict_fun_exit_price: float,
    ) -> None:
        poly_task = self._submit_exit_leg(
            client=self._first_leg,
            market=position.market,
            venue_label=self._first_leg_label,
            already_closed=position.polymarket_closed,
            token_id=position.market.polymarket_token_id,
            side=position.market.polymarket_side,
            contracts=max(0.0, position.polymarket_contracts - position.polymarket_closed_contracts),
            min_price=polymarket_exit_price,
            timeout_ms=self._first_leg_fill_timeout_ms,
            condition_id=position.market.condition_id,
            tick_size=position.market.tick_size,
            neg_risk=position.market.neg_risk,
        )
        predict_task = self._submit_exit_leg(
            client=self._second_leg,
            market=position.market,
            venue_label=self._second_leg_label,
            already_closed=position.predict_fun_closed,
            token_id=position.market.predict_fun_token_id,
            side=position.market.predict_fun_side,
            contracts=max(0.0, position.predict_fun_contracts - position.predict_fun_closed_contracts),
            min_price=predict_fun_exit_price,
            timeout_ms=self._second_leg_fill_timeout_ms,
            neg_risk=position.market.predict_fun_neg_risk if self._second_leg_label == "Predict.fun" else None,
        )
        raw_poly_result, raw_predict_result = await asyncio.gather(
            poly_task,
            predict_task,
            return_exceptions=True,
        )
        poly_result = self._normalize_exit_result(
            raw_poly_result,
            self._first_leg_label,
        )
        predict_result = self._normalize_exit_result(
            raw_predict_result,
            self._second_leg_label,
        )
        poly_exit_order_id, poly_report = poly_result.order_id, poly_result.report
        predict_exit_order_id, predict_report = predict_result.order_id, predict_result.report
        poly_new_fill = poly_report.amount_filled if poly_report is not None else 0.0
        predict_new_fill = predict_report.amount_filled if predict_report is not None else 0.0
        poly_closed_contracts = min(position.polymarket_contracts, position.polymarket_closed_contracts + poly_new_fill)
        predict_closed_contracts = min(position.predict_fun_contracts, position.predict_fun_closed_contracts + predict_new_fill)
        poly_fill_price = poly_report.avg_price if poly_report and poly_report.avg_price > 0 else polymarket_exit_price
        predict_fill_price = (
            predict_report.avg_price if predict_report and predict_report.avg_price > 0 else predict_fun_exit_price
        )
        poly_proceeds = position.polymarket_exit_proceeds_usd + poly_new_fill * poly_fill_price * (
            1.0 - self._venue_fee_pct(self._first_leg_label, position.market)
        )
        predict_proceeds = position.predict_fun_exit_proceeds_usd + predict_new_fill * predict_fill_price * (
            1.0 - self._venue_fee_pct(self._second_leg_label, position.market)
        )
        poly_filled = position.polymarket_closed or poly_closed_contracts >= position.polymarket_contracts - 1e-9
        predict_filled = position.predict_fun_closed or predict_closed_contracts >= position.predict_fun_contracts - 1e-9

        updated = replace(
            position,
            status="closed" if poly_filled and predict_filled else "partial_exit_pending",
            polymarket_closed=poly_filled,
            predict_fun_closed=predict_filled,
            polymarket_exit_price=(poly_proceeds / poly_closed_contracts if poly_closed_contracts > 1e-9 else None),
            predict_fun_exit_price=(
                predict_proceeds / predict_closed_contracts if predict_closed_contracts > 1e-9 else None
            ),
            polymarket_closed_contracts=poly_closed_contracts,
            predict_fun_closed_contracts=predict_closed_contracts,
            polymarket_exit_proceeds_usd=poly_proceeds,
            predict_fun_exit_proceeds_usd=predict_proceeds,
        )
        if not poly_filled or not predict_filled:
            await self._add_position(updated)
            await self._telegram.send_html(
                "🚨 <b>AUTO-CLOSE PARTIAL/FAILED</b>\n"
                f"{self._first_leg_label} exit filled: {poly_filled} ({poly_exit_order_id}).\n"
                f"{self._second_leg_label} exit filled: {predict_filled} ({predict_exit_order_id}).\n"
                "Only the remaining open leg will be retried automatically."
            )
            return

        await self._remove_position(position_key(position.market))
        first_entry_value, second_entry_value = self.gross_entry_values(
            position.market,
            position.polymarket_entry_price,
            position.predict_fun_entry_price,
        )
        entry_cost = (
            position.polymarket_contracts * first_entry_value
            + position.predict_fun_contracts * second_entry_value
        )
        profit_pct, profit_usd = calculate_realized_position_profit(
            entry_cost,
            updated.polymarket_exit_proceeds_usd + updated.predict_fun_exit_proceeds_usd,
        )
        close_signal = ExitSignal(
            position=updated,
            polymarket_exit_price=updated.polymarket_exit_price or polymarket_exit_price,
            predict_fun_exit_price=updated.predict_fun_exit_price or predict_fun_exit_price,
            profit_pct=profit_pct,
            profit_usd=profit_usd,
        )
        LOGGER.info(
            "binary_position_auto_closed",
            extra={"_poly_exit_order_id": poly_exit_order_id, "_predict_exit_order_id": predict_exit_order_id},
        )
        if profit_usd < 0 and await self._risk.record_realized_result(profit_usd):
            await self._telegram.send_html(
                "🚨 <b>GLOBAL DAILY LOSS HARD STOP</b>\n"
                f"Realized daily loss: ${self._risk.daily_loss_usd:.2f}; "
                f"limit: ${self._config.max_daily_loss_usd:.2f}. Manual resume required."
            )
        await self._telegram.send_html(format_exit_message(close_signal, is_test=False))

    async def _submit_exit_leg(
        self,
        *,
        client: BinaryMarketClient,
        market: MarketSpec,
        venue_label: str,
        already_closed: bool,
        token_id: str,
        side: BinarySide,
        contracts: float,
        min_price: float,
        timeout_ms: int,
        condition_id: str | None = None,
        tick_size: str | None = None,
        neg_risk: bool | None = None,
    ) -> ExitLegResult:
        if already_closed:
            return ExitLegResult("already-closed", None)
        order_id = "failed-before-order"
        client_order_id = str(uuid7())
        final_intent_status: OrderIntentStatus | None = None
        if self._repository is not None:
            await self._repository.create_order_intent(
                OrderIntent(
                    client_order_id=client_order_id,
                    route=f"{self._first_leg_label}:{self._second_leg_label}",
                    market_key=position_key(market),
                    venue=venue_label,
                    token_id=token_id,
                    binary_side=side,
                    action="SELL",
                    quantity=Decimal(str(contracts)),
                    limit_price=Decimal(str(min_price)),
                )
            )
            await self._repository.update_order_intent(client_order_id, OrderIntentStatus.SUBMITTING)
        try:
            order_id = await client.sell(
                token_id=token_id,
                side=side,
                contracts=contracts,
                min_price=min_price,
                condition_id=condition_id,
                tick_size=tick_size,
                neg_risk=neg_risk,
            )
            if self._repository is not None:
                await self._repository.update_order_intent(
                    client_order_id,
                    OrderIntentStatus.ACKNOWLEDGED,
                    venue_order_id=order_id,
                )
            self._active_orders[(id(client), order_id)] = client
            report = await client.wait_filled(order_id, timeout_ms)
            if not report.is_filled:
                report = await self._cancel_and_reconcile(client, order_id, report)
            report = replace(report, client_order_id=client_order_id, venue_order_id=order_id)
            if self._repository is not None:
                final_intent_status = _intent_status_from_report(report)
                await self._repository.update_order_intent(
                    client_order_id,
                    final_intent_status,
                    venue_order_id=order_id,
                )
                if final_intent_status is OrderIntentStatus.UNKNOWN:
                    await self._risk.pause(
                        f"unknown order outcome: {venue_label} client_order_id={client_order_id}"
                    )
            return ExitLegResult(order_id, report)
        except asyncio.CancelledError:
            if self._repository is not None:
                final_intent_status = OrderIntentStatus.UNKNOWN
                await self._repository.update_order_intent(
                    client_order_id,
                    OrderIntentStatus.UNKNOWN,
                    venue_order_id=None if order_id == "failed-before-order" else order_id,
                    error="exit submission cancelled during shutdown",
                )
            if order_id != "failed-before-order":
                try:
                    await client.cancel_order(order_id)
                except Exception:
                    LOGGER.exception("exit_cancel_during_shutdown_failed", extra={"_order_id": order_id})
            raise
        except Exception as exc:
            if isinstance(exc, TransactionTimeoutException):
                await self._telegram.send_html(
                    "🚨 <b>NONCE/TRANSACTION TIMEOUT</b>\n"
                    f"Order: {order_id}; timeout: {timeout_ms}ms; reason: {exc}."
                )
            reconciled_report: ExecutionReport | None = None
            if order_id != "failed-before-order":
                try:
                    await client.cancel_order(order_id)
                except Exception:
                    LOGGER.exception("exit_cancel_after_error_failed", extra={"_order_id": order_id})
                try:
                    reconciled_report = await client.wait_filled(
                        order_id,
                        self._config.cancel_reconcile_timeout_ms,
                    )
                except Exception:
                    LOGGER.exception("exit_reconcile_after_error_failed", extra={"_order_id": order_id})
            if reconciled_report is not None:
                reconciled_report = replace(
                    reconciled_report,
                    client_order_id=client_order_id,
                    venue_order_id=order_id,
                )
            if self._repository is not None:
                final_intent_status = (
                    _intent_status_from_report(reconciled_report)
                    if reconciled_report is not None
                    else OrderIntentStatus.UNKNOWN
                )
                await self._repository.update_order_intent(
                    client_order_id,
                    final_intent_status,
                    venue_order_id=None if order_id == "failed-before-order" else order_id,
                    error=str(exc),
                )
                if final_intent_status is OrderIntentStatus.UNKNOWN:
                    await self._risk.pause(
                        f"unknown order outcome: {venue_label} client_order_id={client_order_id}"
                    )
            return ExitLegResult(order_id, reconciled_report, exc)
        finally:
            if order_id != "failed-before-order":
                self._active_orders.pop((id(client), order_id), None)
                forget_order = getattr(client, "forget_order", None)
                if callable(forget_order) and (
                    self._repository is None
                    or final_intent_status
                    in {OrderIntentStatus.FILLED, OrderIntentStatus.CANCELLED}
                ):
                    forget_order(order_id)

    @staticmethod
    def _normalize_exit_result(
        result: ExitLegResult | BaseException,
        venue_label: str,
    ) -> ExitLegResult:
        if isinstance(result, BaseException):
            if isinstance(result, asyncio.CancelledError):
                raise result
            LOGGER.error("exit_leg_failed", extra={"_venue": venue_label, "_error": str(result)})
            return ExitLegResult("failed-before-order", None, result if isinstance(result, Exception) else None)
        if result.error is not None:
            LOGGER.error("exit_leg_failed", extra={"_venue": venue_label, "_error": str(result.error)})
        return result

    def _should_send_signal_alert(self, signal: ArbitrageSignal) -> bool:
        key = _signal_key(signal)
        now = datetime.now(timezone.utc)
        last_sent = self._last_signal_alert_at.get(key)
        if last_sent is not None:
            elapsed = (now - last_sent).total_seconds()
            if elapsed < self._config.signal_alert_cooldown_seconds:
                return False
        self._last_signal_alert_at[key] = now
        return True

    async def _reserve_signal_capital(self, signal: ArbitrageSignal) -> bool:
        if not self._balance_cache:
            await self.start()
        required_first = signal.plan.polymarket_capital_usd + signal.plan.polymarket_fee_usd
        required_second = signal.plan.predict_fun_capital_usd + signal.plan.predict_fun_fee_usd
        async with self._capacity_lock:
            first_available = self._effective_balance(self._first_leg_label) - self._capital_reservations.get(
                self._first_leg_label, 0.0
            )
            second_available = self._effective_balance(self._second_leg_label) - self._capital_reservations.get(
                self._second_leg_label, 0.0
            )
            if first_available < required_first or second_available < required_second:
                LOGGER.info(
                    "signal_skipped_insufficient_balance",
                    extra={
                        "_symbol": signal.market.symbol,
                        "_first_available": first_available,
                        "_first_required": required_first,
                        "_second_available": second_available,
                        "_second_required": required_second,
                    },
                )
                return False
            self._capital_reservations[self._first_leg_label] = (
                self._capital_reservations.get(self._first_leg_label, 0.0) + required_first
            )
            self._capital_reservations[self._second_leg_label] = (
                self._capital_reservations.get(self._second_leg_label, 0.0) + required_second
            )
            return True

    def _release_signal_capital(self, signal: ArbitrageSignal) -> None:
        releases = {
            self._first_leg_label: signal.plan.polymarket_capital_usd + signal.plan.polymarket_fee_usd,
            self._second_leg_label: signal.plan.predict_fun_capital_usd + signal.plan.predict_fun_fee_usd,
        }
        for label, amount in releases.items():
            remaining = max(0.0, self._capital_reservations.get(label, 0.0) - amount)
            if remaining <= 1e-9:
                self._capital_reservations.pop(label, None)
            else:
                self._capital_reservations[label] = remaining

    async def _run_balance_updater(self) -> None:
        while True:
            await asyncio.sleep(self._config.balance_refresh_interval_seconds)
            await self._refresh_balances()

    async def _refresh_balances(self) -> None:
        try:
            first_balance, second_balance = await asyncio.gather(
                self._first_leg.get_cash_balance(),
                self._second_leg.get_cash_balance(),
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception("balance_cache_refresh_failed")
            return
        self._apply_balance_refresh(self._first_leg_label, first_balance)
        self._apply_balance_refresh(self._second_leg_label, second_balance)
        minimum = self._config.min_venue_balance_usd
        now = time.monotonic()
        effective_first = self._effective_balance(self._first_leg_label)
        effective_second = self._effective_balance(self._second_leg_label)
        if min(effective_first, effective_second) < minimum and now - self._last_low_balance_alert_at >= 600:
            self._last_low_balance_alert_at = now
            await self._telegram.send_html(
                "⚠️ <b>LOW VENUE BALANCE</b>\n"
                f"{self._first_leg_label}: ${effective_first:.2f}; "
                f"{self._second_leg_label}: ${effective_second:.2f}; minimum: ${minimum:.2f}."
            )

    async def _record_api_error(self) -> None:
        self._consecutive_api_errors += 1
        if await self._risk.record_api_error():
            await self._telegram.send_html(
                "🚨 <b>GLOBAL EXECUTION CIRCUIT BREAKER OPEN</b>\n"
                f"Consecutive API errors: {self._risk.consecutive_api_errors}; "
                f"reason: {self._risk.pause_reason}. Manual resume required."
            )

    async def _preflight_price_guard(self, signal: ArbitrageSignal) -> bool:
        try:
            first_book, second_book = await asyncio.gather(
                self._first_leg.watch_order_book(signal.market.polymarket_token_id),
                self._second_leg.watch_order_book(signal.market.predict_fun_token_id),
            )
        except Exception:
            LOGGER.exception("preflight_orderbook_check_failed", extra={"_symbol": signal.market.symbol})
            return False

        if not first_book.asks or not second_book.asks:
            LOGGER.warning("preflight_price_guard_empty_book", extra={"_symbol": signal.market.symbol})
            return False
        if first_book.status is not MarketDataStatus.VALID or second_book.status is not MarketDataStatus.VALID:
            LOGGER.error("preflight_price_guard_invalid_book_rejected", extra={"_symbol": signal.market.symbol})
            return False
        now = time.time()
        first_age = max(0.0, now - first_book.timestamp)
        second_age = max(0.0, now - second_book.timestamp)
        if max(first_age, second_age) > self._config.max_orderbook_age_seconds:
            LOGGER.error(
                "preflight_price_guard_stale_book_rejected",
                extra={
                    "_symbol": signal.market.symbol,
                    "_first_age_sec": first_age,
                    "_second_age_sec": second_age,
                    "_max_allowed": self._config.max_orderbook_age_seconds,
                },
            )
            return False
        first_limit = signal.polymarket_price * (1.0 + self._venue_slippage_cap(self._first_leg_label))
        second_limit = signal.predict_fun_price * (1.0 + self._venue_slippage_cap(self._second_leg_label))
        try:
            refreshed_metrics = calculate_spread_metrics(
                polymarket_book=first_book,
                predict_fun_book=second_book,
                max_order_size_usd=self._config.position_size_usd / 2.0,
                min_net_spread=self._config.min_retry_spread_pct,
                max_slippage_pct=min(
                    self._venue_slippage_cap(self._first_leg_label),
                    self._venue_slippage_cap(self._second_leg_label),
                ),
                polymarket_side=signal.market.polymarket_side,
                predict_fun_side=signal.market.predict_fun_side,
                polymarket_fee_pct=self._venue_fee_pct(self._first_leg_label, signal.market),
                predict_fun_fee_pct=self._venue_fee_pct(self._second_leg_label, signal.market),
                max_price_impact=self._config.max_production_price_impact,
            )
        except ValueError as exc:
            LOGGER.warning(
                "preflight_full_depth_quote_rejected",
                extra={"_symbol": signal.market.symbol, "_reason": str(exc)},
            )
            return False
        current_spread = refreshed_metrics.net_spread
        if (
            first_book.best_ask.price > first_limit
            or second_book.best_ask.price > second_limit
            or current_spread < self._config.min_retry_spread_pct
        ):
            LOGGER.warning(
                "preflight_price_guard_rejected",
                extra={
                    "_symbol": signal.market.symbol,
                    "_first_price": first_book.best_ask.price,
                    "_first_limit": first_limit,
                    "_second_price": second_book.best_ask.price,
                    "_second_limit": second_limit,
                    "_current_spread": current_spread,
                    "_spread_floor": self._config.min_retry_spread_pct,
                },
            )
            await self._telegram.send_html(
                "⚠️ <b>SPREAD GUARD REJECTED</b>\n"
                f"Market: {signal.market.symbol}\n"
                f"{self._first_leg_label}: {first_book.best_ask.price:.6f} / limit {first_limit:.6f}\n"
                f"{self._second_leg_label}: {second_book.best_ask.price:.6f} / limit {second_limit:.6f}\n"
                f"Spread: {current_spread:.4%} / floor {self._config.min_retry_spread_pct:.4%}."
            )
            return False
        return True

    async def _market_constraints_guard(self, signal: ArbitrageSignal) -> bool:
        try:
            first_constraints, second_constraints = await asyncio.gather(
                self._first_leg.get_market_constraints(
                    signal.market.polymarket_token_id,
                    signal.market.condition_id,
                ),
                self._second_leg.get_market_constraints(signal.market.predict_fun_token_id),
            )
        except Exception:
            LOGGER.exception("market_constraints_lookup_failed", extra={"_symbol": signal.market.symbol})
            return False
        if first_constraints is None or second_constraints is None:
            LOGGER.error(
                "market_constraints_unknown_live_order_blocked",
                extra={"_symbol": signal.market.symbol},
            )
            return False
        checks = (
            (
                self._first_leg_label,
                first_constraints,
                signal.plan.polymarket_contracts,
                signal.plan.polymarket_capital_usd,
                self._venue_fee_pct(self._first_leg_label, signal.market),
            ),
            (
                self._second_leg_label,
                second_constraints,
                signal.plan.predict_fun_contracts,
                signal.plan.predict_fun_capital_usd,
                self._venue_fee_pct(self._second_leg_label, signal.market),
            ),
        )
        for venue, constraints, quantity, notional, modeled_fee in checks:
            if Decimal(str(quantity)) < constraints.lot_size or Decimal(str(notional)) < constraints.minimum_notional:
                LOGGER.warning(
                    "market_constraints_minimum_rejected",
                    extra={"_symbol": signal.market.symbol, "_venue": venue},
                )
                return False
            if constraints.fee_rate_bps > int(round(modeled_fee * 10_000)):
                LOGGER.warning(
                    "dynamic_fee_exceeds_signal_model",
                    extra={
                        "_symbol": signal.market.symbol,
                        "_venue": venue,
                        "_actual_fee_bps": constraints.fee_rate_bps,
                        "_modeled_fee_bps": int(round(modeled_fee * 10_000)),
                    },
                )
                return False
        return True

    async def _cancel_active_orders_and_clear_pending(self) -> None:
        active = list(self._active_orders.items())
        if active:
            await asyncio.gather(
                *(client.cancel_order(key[1]) for key, client in active),
                return_exceptions=True,
            )
        async with self._capacity_lock:
            self._active_orders.clear()
            self._pending_markets.clear()
            self._capital_reservations.clear()

    def _log_pipeline_latency(
        self,
        signal: ArbitrageSignal,
        first: EntryLegResult,
        second: EntryLegResult,
        signal_received_ns: int | None,
        reserved_ns: int | None,
    ) -> None:
        submit_times = [value for value in (first.submit_started_ns, second.submit_started_ns) if value is not None]
        if not submit_times:
            return
        first_submit = min(submit_times)
        submit_delta_ns = abs((first.submit_started_ns or first_submit) - (second.submit_started_ns or first_submit))
        extra: dict[str, object] = {
            "_symbol": signal.market.symbol,
            "_entry_submit_delta_us": submit_delta_ns / 1_000.0,
        }
        if signal_received_ns is not None and reserved_ns is not None:
            extra["_signal_to_reservation_us"] = (reserved_ns - signal_received_ns) / 1_000.0
            extra["_reservation_to_submit_us"] = (first_submit - reserved_ns) / 1_000.0
        if first.acknowledged_ns is not None and first.submit_started_ns is not None:
            extra["_first_exchange_ack_us"] = (first.acknowledged_ns - first.submit_started_ns) / 1_000.0
        if second.acknowledged_ns is not None and second.submit_started_ns is not None:
            extra["_second_exchange_ack_us"] = (second.acknowledged_ns - second.submit_started_ns) / 1_000.0
        LOGGER.info("execution_pipeline_latency", extra=extra)

    def _venue_slippage_cap(self, venue_label: str) -> float:
        if venue_label == "Polymarket":
            configured = self._config.polymarket.max_slippage_pct
        elif venue_label == "Predict.fun":
            configured = self._config.predict_fun.max_slippage_pct
        elif venue_label == "Myriad":
            configured = self._config.myriad_markets.max_slippage_pct
        else:
            raise ValueError(f"Unsupported venue label: {venue_label}")
        return min(configured, self._config.max_production_price_impact)

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

    def _debit_cached_balance(self, venue_label: str, amount_usd: float) -> None:
        self._optimistic_debits[venue_label] = self._optimistic_debits.get(venue_label, 0.0) + amount_usd

    def _effective_balance(self, venue_label: str) -> float:
        return max(
            0.0,
            self._balance_cache.get(venue_label, 0.0) - self._optimistic_debits.get(venue_label, 0.0),
        )

    def _apply_balance_refresh(self, venue_label: str, fetched_balance: float) -> None:
        previous = self._balance_cache.get(venue_label)
        if previous is not None and fetched_balance < previous - 1e-9:
            observed_debit = previous - fetched_balance
            remaining = max(0.0, self._optimistic_debits.get(venue_label, 0.0) - observed_debit)
            if remaining <= 1e-9:
                self._optimistic_debits.pop(venue_label, None)
            else:
                self._optimistic_debits[venue_label] = remaining
        self._balance_cache[venue_label] = fetched_balance

    async def _add_position(self, position: OpenPosition) -> None:
        key = position_key(position.market)
        if self._repository is not None:
            await self._repository.save_position(key, position)
        self._ledger.add(position)

    async def _remove_position(self, key: str) -> None:
        if self._repository is not None:
            await self._repository.remove_position(key)
        self._ledger.remove(key)

    async def _save_unwind_pending(
        self,
        signal: ArbitrageSignal,
        first_order_id: str,
        second_order_id: str,
        matched_amount: float,
        unmatched_first: float,
        unmatched_second: float,
        first_entry_price: float,
        second_entry_price: float,
    ) -> None:
        await self._add_position(
            OpenPosition(
                market=signal.market,
                polymarket_contracts=matched_amount + unmatched_first,
                polymarket_entry_price=first_entry_price,
                predict_fun_contracts=matched_amount + unmatched_second,
                predict_fun_entry_price=second_entry_price,
                opened_at=datetime.now(timezone.utc),
                polymarket_order_id=first_order_id,
                predict_fun_order_id=second_order_id,
                status="unwind_pending",
                polymarket_unwind_attempts=1,
                unmatched_first_contracts=unmatched_first,
                unmatched_second_contracts=unmatched_second,
            )
        )

    async def _save_entry_pending(self, signal: ArbitrageSignal) -> None:
        await self._add_position(
            OpenPosition(
                market=signal.market,
                polymarket_contracts=signal.plan.polymarket_contracts,
                polymarket_entry_price=signal.polymarket_price,
                predict_fun_contracts=signal.plan.predict_fun_contracts,
                predict_fun_entry_price=signal.predict_fun_price,
                opened_at=datetime.now(timezone.utc),
                polymarket_order_id="pending",
                predict_fun_order_id="pending",
                status="entry_pending",
            )
        )

    def _open_position_from_amounts(
        self,
        signal: ArbitrageSignal,
        first_order_id: str,
        second_order_id: str,
        matched_amount: float,
        first_entry_price: float,
        second_entry_price: float,
    ) -> OpenPosition:
        return OpenPosition(
            market=signal.market,
            polymarket_contracts=matched_amount,
            polymarket_entry_price=first_entry_price,
            predict_fun_contracts=matched_amount,
            predict_fun_entry_price=second_entry_price,
            opened_at=datetime.now(timezone.utc),
            polymarket_order_id=first_order_id,
            predict_fun_order_id=second_order_id,
        )

    async def _try_unwind_first_leg(self, signal: ArbitrageSignal, contracts: float | None = None) -> float:
        requested = contracts if contracts is not None else signal.plan.polymarket_contracts
        try:
            book = await self._first_leg.watch_order_book(signal.market.polymarket_token_id)
            if not book.bids:
                return 0.0
            target_unwind_price = max(0.01, book.best_bid.price - 0.01)
            result = await self._submit_exit_leg(
                client=self._first_leg,
                market=signal.market,
                venue_label=self._first_leg_label,
                already_closed=False,
                token_id=signal.market.polymarket_token_id,
                side=signal.market.polymarket_side,
                contracts=requested,
                min_price=target_unwind_price,
                timeout_ms=self._first_leg_fill_timeout_ms,
                condition_id=signal.market.condition_id,
                tick_size=signal.market.tick_size,
                neg_risk=signal.market.neg_risk,
            )
            if result.report is None:
                return 0.0
            unwind_report = result.report
            unwound = min(requested, unwind_report.amount_filled)
            await self._record_unwind_pnl(
                self._first_leg_label,
                signal.market,
                signal.polymarket_price,
                unwind_report.avg_price or target_unwind_price,
                unwound,
            )
            return unwound
        except Exception:
            LOGGER.exception("instant_unwind_failed", extra={"_symbol": signal.market.symbol})
            return 0.0

    async def _try_unwind_second_leg(self, signal: ArbitrageSignal, contracts: float) -> float:
        try:
            book = await self._second_leg.watch_order_book(signal.market.predict_fun_token_id)
            if not book.bids:
                return 0.0
            target_unwind_price = max(0.01, book.best_bid.price - 0.01)
            result = await self._submit_exit_leg(
                client=self._second_leg,
                market=signal.market,
                venue_label=self._second_leg_label,
                already_closed=False,
                token_id=signal.market.predict_fun_token_id,
                side=signal.market.predict_fun_side,
                contracts=contracts,
                min_price=target_unwind_price,
                timeout_ms=self._second_leg_fill_timeout_ms,
                neg_risk=(
                    signal.market.predict_fun_neg_risk if self._second_leg_label == "Predict.fun" else None
                ),
            )
            if result.report is None:
                return 0.0
            unwind_report = result.report
            unwound = min(contracts, unwind_report.amount_filled)
            await self._record_unwind_pnl(
                self._second_leg_label,
                signal.market,
                signal.predict_fun_price,
                unwind_report.avg_price or target_unwind_price,
                unwound,
            )
            return unwound
        except Exception:
            LOGGER.exception("instant_second_leg_unwind_failed", extra={"_symbol": signal.market.symbol})
            return 0.0

    async def _record_unwind_pnl(
        self,
        venue_label: str,
        market: MarketSpec,
        entry_price: float,
        exit_price: float,
        contracts: float,
    ) -> None:
        if contracts <= 0:
            return
        fee = self._venue_fee_pct(venue_label, market)
        profit_usd = contracts * (exit_price * (1.0 - fee) - entry_price * (1.0 + fee))
        if profit_usd < 0 and await self._risk.record_realized_result(profit_usd):
            await self._telegram.send_html(
                "🚨 <b>GLOBAL DAILY LOSS HARD STOP</b>\n"
                f"Emergency unwind opened hard stop; realized daily loss: ${self._risk.daily_loss_usd:.2f}."
            )

def _signal_key(signal: ArbitrageSignal) -> str:
    if signal.market.rules_fingerprint:
        return signal.market.rules_fingerprint
    if signal.market.polymarket_token_id and signal.market.predict_fun_token_id:
        return f"{signal.market.polymarket_token_id}:{signal.market.predict_fun_token_id}"
    return f"{signal.market.symbol}:{signal.market.target_label}"


def _intent_status_from_report(report: ExecutionReport) -> OrderIntentStatus:
    if report.is_filled:
        return OrderIntentStatus.FILLED
    if report.has_fill:
        return OrderIntentStatus.PARTIAL
    if report.status.value in {"CANCELLED", "EXPIRED"}:
        return OrderIntentStatus.CANCELLED
    # An order still reported OPEN after cancel/reconcile has an unresolved
    # outcome. It must not be retried until account-level reconciliation.
    return OrderIntentStatus.UNKNOWN


def _signal_from_unwind_position(position: OpenPosition) -> ArbitrageSignal:
    plan = PositionPlan(
        polymarket_contracts=position.polymarket_contracts,
        polymarket_capital_usd=position.polymarket_contracts * position.polymarket_entry_price,
        predict_fun_contracts=0.0,
        predict_fun_capital_usd=0.0,
        payout_contracts=position.polymarket_contracts,
        total_cost_usd=position.polymarket_contracts * position.polymarket_entry_price,
    )
    metrics = SpreadMetrics(0.0, 0.0, 0.0, 0.0, 0.0, position.polymarket_entry_price)
    return ArbitrageSignal(
        market=position.market,
        plan=plan,
        metrics=metrics,
        polymarket_price=position.polymarket_entry_price,
        predict_fun_price=0.0,
    )


async def _zero_async() -> float:
    return 0.0
