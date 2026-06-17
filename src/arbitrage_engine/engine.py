from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from .config import AppConfig
from .connectors.base import CefiFuturesClient, PolymarketClient
from .execution import ExecutionRouter
from .models import ArbitrageSignal, ExitSignal
from .quant import build_position_plan, calculate_polymarket_profit, calculate_spread_metrics

LOGGER = logging.getLogger(__name__)


class ArbitrageEngine:
    def __init__(
        self,
        config: AppConfig,
        polymarket: PolymarketClient,
        cefi: CefiFuturesClient,
        execution: ExecutionRouter,
    ) -> None:
        self._config = config
        self._polymarket = polymarket
        self._cefi = cefi
        self._execution = execution

    async def run_forever(self) -> None:
        while True:
            try:
                await self.run_once()
            except Exception:
                LOGGER.exception("engine_cycle_failed")
                await asyncio.sleep(1.0)
            await asyncio.sleep(self._config.poll_interval_ms / 1000)

    async def run_once(self) -> None:
        await self._check_auto_close_positions()
        for market in self._config.markets:
            polymarket_book, cefi_book = await asyncio.gather(
                self._polymarket.watch_order_book(market.polymarket_token_id),
                self._cefi.watch_order_book(market.cefi_symbol),
            )
            metrics = calculate_spread_metrics(
                polymarket_book=polymarket_book,
                cefi_book=cefi_book,
                max_order_size_usd=self._config.max_order_size_usd,
                cefi_taker_fee=self._config.cefi_taker_fee,
                leverage=self._config.cefi_leverage,
            )
            if metrics.net_spread < self._config.min_net_spread:
                continue

            plan = build_position_plan(
                polymarket_book=polymarket_book,
                cefi_book=cefi_book,
                max_order_size_usd=self._config.max_order_size_usd,
                leverage=self._config.cefi_leverage,
            )
            signal = ArbitrageSignal(
                market=market,
                plan=plan,
                metrics=metrics,
                polymarket_price=polymarket_book.best_ask.price,
                cefi_price=cefi_book.best_ask.price,
            )
            await self._execution.handle_signal(signal)

    async def _check_auto_close_positions(self) -> None:
        if not self._config.auto_close.enabled:
            return

        now = datetime.now(timezone.utc)
        for position in self._execution.ledger.all():
            expires_at = position.market.expires_at
            if expires_at is None:
                continue
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)

            seconds_to_expiry = (expires_at - now).total_seconds()
            if seconds_to_expiry > self._config.auto_close.close_before_expiry_seconds:
                continue

            polymarket_book = await self._polymarket.watch_order_book(position.market.polymarket_token_id)
            exit_price = polymarket_book.best_bid.price
            profit_pct, profit_usd = calculate_polymarket_profit(
                entry_price=position.polymarket_entry_price,
                exit_price=exit_price,
                contracts=position.polymarket_contracts,
            )
            if profit_pct < self._config.auto_close.take_profit_pct:
                continue

            await self._execution.handle_exit_signal(
                ExitSignal(
                    position=position,
                    polymarket_exit_price=exit_price,
                    profit_pct=profit_pct,
                    profit_usd=profit_usd,
                )
            )
