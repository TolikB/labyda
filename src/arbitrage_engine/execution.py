from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import replace
from datetime import datetime, timezone

from .config import AppConfig
from .connectors.base import BinaryMarketClient
from .models import (
    ArbitrageSignal,
    ExecutionReport,
    ExecutionStatus,
    ExitSignal,
    OpenPosition,
    PositionPlan,
    SpreadMetrics,
    position_key,
)
from .positions import PositionLedger
from .quant import calculate_binary_position_profit, is_binary_signal_allowed
from .telegram import TelegramNotifier, format_exit_message

LOGGER = logging.getLogger(__name__)
SPREAD_GUARD_FLOOR = 0.05
SPREAD_GUARD_INTERVAL_MS = 200


class SpreadGuardTriggered(RuntimeError):
    pass


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
    ) -> None:
        self._config = config
        self._first_leg = polymarket
        self._second_leg = predict_fun
        self._telegram = telegram
        self._ledger = ledger or PositionLedger()
        self._last_signal_alert_at: dict[str, datetime] = {}
        self._first_leg_label = first_leg_label
        self._second_leg_label = second_leg_label
        self._first_leg_fill_timeout_ms = first_leg_fill_timeout_ms or config.polymarket_fill_timeout_ms
        self._second_leg_fill_timeout_ms = second_leg_fill_timeout_ms or config.predict_fun_fill_timeout_ms

    @property
    def ledger(self) -> PositionLedger:
        return self._ledger

    async def close(self) -> None:
        await self._telegram.close()

    async def ensure_balances(self) -> bool:
        first_balance = await self._first_leg.get_cash_balance()
        second_balance = await self._second_leg.get_cash_balance()
        required = self._config.position_size_usd / 2.0
        ok = first_balance >= required and second_balance >= required
        if not ok:
            await self._telegram.send_html(
                "⚠️ <b>ARBITRAGE ENGINE STOPPED</b>\n"
                f"Недостатній баланс: {self._first_leg_label} ${first_balance:.2f}, "
                f"{self._second_leg_label} ${second_balance:.2f}. Required per leg: ${required:.2f}."
            )
        return ok

    async def handle_signal(self, signal: ArbitrageSignal) -> None:
        if not is_binary_signal_allowed(signal.metrics, self._config.min_net_spread):
            LOGGER.info(
                "binary_signal_rejected",
                extra={
                    "_combined_cost": signal.metrics.combined_cost_per_payout,
                    "_net_spread": signal.metrics.net_spread,
                },
            )
            return
        if self._ledger.has(position_key(signal.market)):
            LOGGER.info("signal_skipped_existing_position", extra={"_symbol": signal.market.symbol})
            return

        if self._should_send_signal_alert(signal):
            await self._telegram.send_signal(signal, is_test=self._config.is_test, min_net_spread=self._config.min_net_spread)

        if self._config.is_test:
            LOGGER.info("dry_run_signal", extra={"_symbol": signal.market.symbol, "_net_spread": signal.metrics.net_spread})
            return

        if not await self._has_capacity_for_signal(signal):
            return

        if not await self._preflight_price_guard(signal):
            return

        await self._execute_production(signal)

    async def _execute_production(self, signal: ArbitrageSignal) -> None:
        first_order_id = await self._first_leg.buy(
            token_id=signal.market.polymarket_token_id,
            side=signal.market.polymarket_side,
            contracts=signal.plan.polymarket_contracts,
            max_price=signal.polymarket_price,
            condition_id=signal.market.condition_id,
            tick_size=signal.market.tick_size,
            neg_risk=signal.market.neg_risk,
        )
        first_report = await self._first_leg.wait_filled(first_order_id, self._first_leg_fill_timeout_ms)
        if not first_report.is_filled:
            await self._first_leg.cancel_order(first_order_id)
            if first_report.has_fill:
                unwind_filled = await self._try_unwind_first_leg(signal, first_report.amount_filled)
                if not unwind_filled:
                    self._save_unwind_pending(signal, first_order_id, first_report.amount_filled, 0.0)
            LOGGER.warning(
                "first_leg_incomplete_cancelled",
                extra={
                    "_order_id": first_order_id,
                    "_venue": self._first_leg_label,
                    "_amount_filled": first_report.amount_filled,
                    "_remaining_amount": first_report.remaining_amount,
                },
            )
            return

        second_report: ExecutionReport | None = None
        second_order_id = ""
        try:
            second_order_id = await self._second_leg.buy(
                token_id=signal.market.predict_fun_token_id,
                side=signal.market.predict_fun_side,
                contracts=signal.plan.predict_fun_contracts,
                max_price=signal.predict_fun_price,
                neg_risk=signal.market.neg_risk,
            )
            second_report = await self._wait_second_leg_with_spread_guard(
                signal,
                second_order_id,
                self._second_leg_fill_timeout_ms,
            )
            if not second_report.is_filled:
                await self._second_leg.cancel_order(second_order_id)
                raise RuntimeError(f"{self._second_leg_label} order did not fill: {second_order_id}")
        except Exception:
            LOGGER.exception("second_leg_failed_after_first_leg_fill", extra={"_first_order_id": first_order_id})
            second_filled_amount = second_report.amount_filled if second_report is not None else 0.0
            unmatched_first = max(0.0, first_report.amount_filled - second_filled_amount)
            unwind_filled = unmatched_first <= 1e-9 or await self._try_unwind_first_leg(signal, unmatched_first)
            await self._telegram.send_html(
                f"⚠️ <b>{self._second_leg_label.upper()} LEG FAILED</b>\n"
                f"{self._first_leg_label} entry filled: {first_order_id}.\n"
                f"{self._second_leg_label} amount filled: {second_filled_amount:.6f}.\n"
                f"Unmatched amount: {unmatched_first:.6f}.\n"
                f"Automatic unwind filled: {unwind_filled}."
            )
            if not unwind_filled:
                self._save_unwind_pending(signal, first_order_id, first_report.amount_filled, second_filled_amount)
            elif second_filled_amount > 1e-9:
                position = self._open_position_from_amounts(
                    signal,
                    first_order_id,
                    second_order_id,
                    second_filled_amount,
                )
                self._ledger.add(position)
                await self._telegram.send_position_opened(signal, position)
            return

        position = OpenPosition(
            market=signal.market,
            polymarket_contracts=first_report.amount_filled,
            polymarket_entry_price=signal.polymarket_price,
            predict_fun_contracts=second_report.amount_filled,
            predict_fun_entry_price=signal.predict_fun_price,
            opened_at=datetime.now(timezone.utc),
            polymarket_order_id=first_order_id,
            predict_fun_order_id=second_order_id,
        )
        self._ledger.add(position)
        LOGGER.info(
            "binary_signal_executed",
            extra={"_first_order_id": first_order_id, "_second_order_id": second_order_id},
        )
        await self._telegram.send_position_opened(signal, position)

    async def handle_exit_signal(self, signal: ExitSignal) -> None:
        if self._config.is_test:
            await self._telegram.send_html(format_exit_message(signal, is_test=True))
            self._ledger.remove(position_key(signal.position.market))
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
        unwind_amount = position.unmatched_first_contracts or position.polymarket_contracts
        filled = await self._try_unwind_first_leg(signal, unwind_amount)
        attempts = position.polymarket_unwind_attempts + 1
        if filled:
            if position.predict_fun_contracts > 1e-9:
                self._ledger.add(
                    replace(
                        position,
                        polymarket_contracts=position.predict_fun_contracts,
                        status="open",
                        polymarket_unwind_attempts=attempts,
                        unmatched_first_contracts=0.0,
                    )
                )
            else:
                self._ledger.remove(position_key(position.market))
            await self._telegram.send_html(
                "✅ <b>[AUTO-UNWIND COMPLETED]</b>\n"
                f"Пара: {position.market.symbol}\n"
                f"Attempts: {attempts}\n"
                f"{self._first_leg_label}-only exposure was closed automatically."
            )
            return
        self._ledger.add(replace(position, polymarket_unwind_attempts=attempts))

    async def _close_position_legs(
        self,
        position: OpenPosition,
        *,
        polymarket_exit_price: float,
        predict_fun_exit_price: float,
    ) -> None:
        poly_filled = position.polymarket_closed
        predict_filled = position.predict_fun_closed
        poly_exit_order_id = "already-closed"
        predict_exit_order_id = "already-closed"

        if not position.polymarket_closed:
            poly_exit_order_id = await self._first_leg.sell(
                token_id=position.market.polymarket_token_id,
                side=position.market.polymarket_side,
                contracts=position.polymarket_contracts,
                min_price=polymarket_exit_price,
                condition_id=position.market.condition_id,
                tick_size=position.market.tick_size,
                neg_risk=position.market.neg_risk,
            )
            poly_report = await self._first_leg.wait_filled(
                poly_exit_order_id,
                self._first_leg_fill_timeout_ms,
            )
            poly_filled = poly_report.is_filled

        if not position.predict_fun_closed:
            predict_exit_order_id = await self._second_leg.sell(
                token_id=position.market.predict_fun_token_id,
                side=position.market.predict_fun_side,
                contracts=position.predict_fun_contracts,
                min_price=predict_fun_exit_price,
                neg_risk=position.market.neg_risk,
            )
            predict_report = await self._second_leg.wait_filled(
                predict_exit_order_id,
                self._second_leg_fill_timeout_ms,
            )
            predict_filled = predict_report.is_filled

        if not poly_filled and poly_exit_order_id != "already-closed":
            await self._first_leg.cancel_order(poly_exit_order_id)
        if not predict_filled and predict_exit_order_id != "already-closed":
            await self._second_leg.cancel_order(predict_exit_order_id)

        updated = replace(
            position,
            status="open" if poly_filled and predict_filled else "partial_exit_pending",
            polymarket_closed=poly_filled,
            predict_fun_closed=predict_filled,
            polymarket_exit_price=polymarket_exit_price if poly_filled else position.polymarket_exit_price,
            predict_fun_exit_price=predict_fun_exit_price if predict_filled else position.predict_fun_exit_price,
        )
        if not poly_filled or not predict_filled:
            self._ledger.add(updated)
            await self._telegram.send_html(
                "🚨 <b>AUTO-CLOSE PARTIAL/FAILED</b>\n"
                f"{self._first_leg_label} exit filled: {poly_filled} ({poly_exit_order_id}).\n"
                f"{self._second_leg_label} exit filled: {predict_filled} ({predict_exit_order_id}).\n"
                "Only the remaining open leg will be retried automatically."
            )
            return

        self._ledger.remove(position_key(position.market))
        profit_pct, profit_usd = calculate_binary_position_profit(
            entry_total_cost=position.polymarket_entry_price + position.predict_fun_entry_price,
            exit_total_value=(updated.polymarket_exit_price or 0.0) + (updated.predict_fun_exit_price or 0.0),
            payout_contracts=position.polymarket_contracts,
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
        await self._telegram.send_html(format_exit_message(close_signal, is_test=False))

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

    async def _has_capacity_for_signal(self, signal: ArbitrageSignal) -> bool:
        required_first = signal.plan.polymarket_capital_usd
        required_second = signal.plan.predict_fun_capital_usd
        first_available = await self._available_balance(self._first_leg, self._first_leg_label)
        second_available = await self._available_balance(self._second_leg, self._second_leg_label)
        ok = first_available >= required_first and second_available >= required_second
        if not ok:
            LOGGER.info(
                "signal_skipped_insufficient_balance",
                extra={
                    "_symbol": signal.market.symbol,
                    "_first_leg": self._first_leg_label,
                    "_first_available": first_available,
                    "_first_required": required_first,
                    "_second_leg": self._second_leg_label,
                    "_second_available": second_available,
                    "_second_required": required_second,
                },
            )
        return ok

    async def _available_balance(self, client: BinaryMarketClient, venue_label: str) -> float:
        raw_balance = await client.get_cash_balance()
        reserved = 0.0
        for position in self._ledger.all():
            if position.market.venue_a_label == venue_label and not position.polymarket_closed:
                reserved += position.polymarket_contracts * position.polymarket_entry_price
            if position.market.venue_b_label == venue_label and not position.predict_fun_closed:
                reserved += position.predict_fun_contracts * position.predict_fun_entry_price
        return raw_balance - reserved

    async def _preflight_price_guard(self, signal: ArbitrageSignal) -> bool:
        try:
            first_book, second_book = await asyncio.gather(
                self._first_leg.watch_order_book(signal.market.polymarket_token_id),
                self._second_leg.watch_order_book(signal.market.predict_fun_token_id),
            )
        except Exception:
            LOGGER.exception("preflight_orderbook_check_failed", extra={"_symbol": signal.market.symbol})
            return False

        first_limit = signal.polymarket_price * (1.0 + self._route_slippage_cap())
        second_limit = signal.predict_fun_price * (1.0 + self._route_slippage_cap())
        if first_book.best_ask.price > first_limit or second_book.best_ask.price > second_limit:
            LOGGER.warning(
                "preflight_price_guard_rejected",
                extra={
                    "_symbol": signal.market.symbol,
                    "_first_price": first_book.best_ask.price,
                    "_first_limit": first_limit,
                    "_second_price": second_book.best_ask.price,
                    "_second_limit": second_limit,
                },
            )
            return False
        return True

    def _route_slippage_cap(self) -> float:
        if self._second_leg_label == "Myriad":
            return self._config.myriad_markets.max_slippage_pct
        return self._config.predict_fun.max_slippage_pct

    def _save_unwind_pending(
        self,
        signal: ArbitrageSignal,
        first_order_id: str,
        first_amount_filled: float,
        second_amount_filled: float,
    ) -> None:
        unmatched = max(0.0, first_amount_filled - second_amount_filled)
        self._ledger.add(
            OpenPosition(
                market=signal.market,
                polymarket_contracts=first_amount_filled,
                polymarket_entry_price=signal.polymarket_price,
                predict_fun_contracts=second_amount_filled,
                predict_fun_entry_price=signal.predict_fun_price if second_amount_filled > 0 else 0.0,
                opened_at=datetime.now(timezone.utc),
                polymarket_order_id=first_order_id,
                predict_fun_order_id="",
                status="unwind_pending",
                polymarket_unwind_attempts=1,
                unmatched_first_contracts=unmatched,
            )
        )

    def _open_position_from_amounts(
        self,
        signal: ArbitrageSignal,
        first_order_id: str,
        second_order_id: str,
        matched_amount: float,
    ) -> OpenPosition:
        return OpenPosition(
            market=signal.market,
            polymarket_contracts=matched_amount,
            polymarket_entry_price=signal.polymarket_price,
            predict_fun_contracts=matched_amount,
            predict_fun_entry_price=signal.predict_fun_price,
            opened_at=datetime.now(timezone.utc),
            polymarket_order_id=first_order_id,
            predict_fun_order_id=second_order_id,
        )

    async def _try_unwind_first_leg(self, signal: ArbitrageSignal, contracts: float | None = None) -> bool:
        try:
            book = await self._first_leg.watch_order_book(signal.market.polymarket_token_id)
            if not book.bids:
                return False
            target_unwind_price = max(0.01, book.best_bid.price - 0.01)
            unwind_order_id = await self._first_leg.sell(
                token_id=signal.market.polymarket_token_id,
                side=signal.market.polymarket_side,
                contracts=contracts if contracts is not None else signal.plan.polymarket_contracts,
                min_price=target_unwind_price,
                condition_id=signal.market.condition_id,
                tick_size=signal.market.tick_size,
                neg_risk=signal.market.neg_risk,
            )
            unwind_report = await self._first_leg.wait_filled(
                unwind_order_id,
                self._first_leg_fill_timeout_ms,
            )
            if unwind_report.is_filled:
                return True
            await self._first_leg.cancel_order(unwind_order_id)
            return False
        except Exception:
            LOGGER.exception("instant_unwind_failed", extra={"_symbol": signal.market.symbol})
            return False

    async def _wait_second_leg_with_spread_guard(
        self,
        signal: ArbitrageSignal,
        order_id: str,
        timeout_ms: int,
    ) -> ExecutionReport:
        deadline = time.monotonic() + timeout_ms / 1000
        requested = signal.plan.predict_fun_contracts
        latest = ExecutionReport.from_amounts(order_id, requested, 0.0, "pending")
        while time.monotonic() < deadline:
            if await self._spread_guard_breached(signal):
                await self._second_leg.cancel_order(order_id)
                final_report = await self._second_leg.wait_filled(order_id, SPREAD_GUARD_INTERVAL_MS)
                return _newer_report(latest, final_report, "spread_guard")
            poll_timeout_ms = min(SPREAD_GUARD_INTERVAL_MS, max(1, int((deadline - time.monotonic()) * 1000)))
            report = await self._second_leg.wait_filled(order_id, poll_timeout_ms)
            latest = _newer_report(latest, report)
            if report.is_filled or report.status in {
                ExecutionStatus.PARTIAL,
                ExecutionStatus.CANCELLED,
                ExecutionStatus.EXPIRED,
            }:
                return latest
        return latest

    async def _spread_guard_breached(self, signal: ArbitrageSignal) -> bool:
        try:
            first_book, second_book = await asyncio.gather(
                self._first_leg.watch_order_book(signal.market.polymarket_token_id),
                self._second_leg.watch_order_book(signal.market.predict_fun_token_id),
            )
            current_spread = 1.0 - (first_book.best_ask.price + second_book.best_ask.price)
            if current_spread < SPREAD_GUARD_FLOOR:
                LOGGER.warning(
                    "spread_guard_triggered",
                    extra={
                        "_symbol": signal.market.symbol,
                        "_current_spread": current_spread,
                        "_floor": SPREAD_GUARD_FLOOR,
                    },
                )
                return True
        except Exception:
            LOGGER.exception("spread_guard_check_failed", extra={"_symbol": signal.market.symbol})
        return False


def _signal_key(signal: ArbitrageSignal) -> str:
    return (
        signal.market.rules_fingerprint
        or f"{signal.market.polymarket_token_id}:{signal.market.predict_fun_token_id}"
        or f"{signal.market.symbol}:{signal.market.target_label}"
    )


def _newer_report(
    current: ExecutionReport,
    candidate: ExecutionReport,
    status_override: str | ExecutionStatus | None = None,
) -> ExecutionReport:
    amount_filled = max(current.amount_filled, candidate.amount_filled)
    return ExecutionReport.from_amounts(
        candidate.order_id,
        max(current.requested_amount, candidate.requested_amount),
        amount_filled,
        status_override or candidate.status,
        candidate.avg_price or current.avg_price,
    )


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
