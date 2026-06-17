from __future__ import annotations

import logging
from datetime import datetime, timezone

from .config import AppConfig
from .connectors.base import CefiFuturesClient, PolymarketClient
from .models import ArbitrageSignal, ExitSignal, OpenPosition
from .positions import PositionLedger
from .telegram import TelegramNotifier

LOGGER = logging.getLogger(__name__)


class ExecutionRouter:
    def __init__(
        self,
        config: AppConfig,
        polymarket: PolymarketClient,
        cefi: CefiFuturesClient,
        telegram: TelegramNotifier,
        ledger: PositionLedger | None = None,
    ) -> None:
        self._config = config
        self._polymarket = polymarket
        self._cefi = cefi
        self._telegram = telegram
        self._ledger = ledger or PositionLedger()

    @property
    def ledger(self) -> PositionLedger:
        return self._ledger

    async def ensure_balances(self) -> bool:
        poly_balance = await self._polymarket.get_usdc_balance()
        cefi_balance = await self._cefi.get_usdt_balance()
        ok = poly_balance >= self._config.max_order_size_usd and cefi_balance >= self._config.max_order_size_usd
        if not ok:
            await self._telegram.send_html(
                "⚠️ <b>ARBITRAGE ENGINE STOPPED</b>\n"
                f"Недостатній баланс: Polymarket ${poly_balance:.2f}, Binance ${cefi_balance:.2f}."
            )
        return ok

    async def handle_signal(self, signal: ArbitrageSignal) -> None:
        if signal.metrics.net_spread < self._config.min_net_spread:
            LOGGER.info("signal_rejected_spread", extra={"_net_spread": signal.metrics.net_spread})
            return

        if self._config.is_test:
            LOGGER.info("dry_run_signal", extra={"_symbol": signal.market.symbol, "_net_spread": signal.metrics.net_spread})
            await self._telegram.send_signal(signal, is_test=True, min_net_spread=self._config.min_net_spread)
            return

        await self._execute_production(signal)

    async def _execute_production(self, signal: ArbitrageSignal) -> None:
        order_id = await self._polymarket.create_signed_order(
            token_id=signal.market.polymarket_token_id,
            side=signal.market.polymarket_side,
            contracts=signal.plan.polymarket_contracts,
            max_price=signal.polymarket_price,
        )
        filled = await self._polymarket.wait_filled(order_id, self._config.polymarket_fill_timeout_ms)
        if not filled:
            await self._polymarket.cancel_order(order_id)
            LOGGER.warning("polymarket_timeout_cancelled", extra={"_order_id": order_id})
            return

        hedge_order_id = await self._cefi.create_market_order(
            signal.market.cefi_symbol,
            signal.market.cefi_hedge_side,
            signal.plan.cefi_quantity,
        )
        self._ledger.add(
            OpenPosition(
                market=signal.market,
                polymarket_contracts=signal.plan.polymarket_contracts,
                polymarket_entry_price=signal.polymarket_price,
                cefi_quantity=signal.plan.cefi_quantity,
                cefi_entry_side=signal.market.cefi_hedge_side,
                opened_at=datetime.now(timezone.utc),
                polymarket_order_id=order_id,
                cefi_order_id=hedge_order_id,
            )
        )
        LOGGER.info(
            "production_signal_executed",
            extra={"_poly_order_id": order_id, "_hedge_order_id": hedge_order_id},
        )
        await self._telegram.send_signal(signal, is_test=False, min_net_spread=self._config.min_net_spread)

    async def handle_exit_signal(self, signal: ExitSignal) -> None:
        if self._config.is_test:
            LOGGER.info(
                "dry_run_exit_signal",
                extra={
                    "_symbol": signal.position.market.symbol,
                    "_profit_pct": signal.profit_pct,
                    "_profit_usd": signal.profit_usd,
                },
            )
            await self._telegram.send_html(format_exit_message(signal, is_test=True))
            self._ledger.remove(signal.position.market.polymarket_token_id)
            return

        poly_exit_order_id = await self._polymarket.close_position(
            token_id=signal.position.market.polymarket_token_id,
            side=signal.position.market.polymarket_side,
            contracts=signal.position.polymarket_contracts,
            min_price=signal.polymarket_exit_price,
        )
        hedge_exit_order_id = await self._cefi.close_market_order(
            symbol=signal.position.market.cefi_symbol,
            entry_side=signal.position.cefi_entry_side,
            quantity=signal.position.cefi_quantity,
        )
        self._ledger.remove(signal.position.market.polymarket_token_id)
        LOGGER.info(
            "position_auto_closed",
            extra={
                "_poly_exit_order_id": poly_exit_order_id,
                "_hedge_exit_order_id": hedge_exit_order_id,
                "_profit_pct": signal.profit_pct,
                "_profit_usd": signal.profit_usd,
            },
        )
        await self._telegram.send_html(format_exit_message(signal, is_test=False))


def format_exit_message(signal: ExitSignal, is_test: bool) -> str:
    mode = "TEST MODE (Ордери заблоковані)" if is_test else "PRODUCTION"
    return (
        "✅ <b>[AUTO-CLOSE TAKE PROFIT]</b>\n"
        f"Пара: {signal.position.market.symbol} (Target: {signal.position.market.target_label})\n"
        f"Режим: {mode}\n\n"
        "📤 <b>ЗАКРИТТЯ ПОЗИЦІЇ:</b>\n"
        f"• Polymarket exit bid: ${signal.polymarket_exit_price:.4f}\n"
        f"• Контракти: {signal.position.polymarket_contracts:.4f}\n"
        f"• Прибуток Polymarket: {signal.profit_pct:.2%} (${signal.profit_usd:+.2f})\n"
        f"• CeFi hedge close: {signal.position.cefi_quantity:.8f} {signal.position.market.symbol.split('-')[0]}"
    )
