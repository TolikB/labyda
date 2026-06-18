from __future__ import annotations

import html
import logging

from .config import TelegramConfig
from .http import client_session
from .models import ArbitrageSignal, ExitSignal, OpenPosition

LOGGER = logging.getLogger(__name__)


class TelegramNotifier:
    def __init__(self, config: TelegramConfig) -> None:
        self._config = config

    async def send_html(self, message: str) -> None:
        if not self._config.bot_token or not self._config.chat_id:
            LOGGER.warning("telegram_not_configured", extra={"_event": "telegram_not_configured"})
            return

        url = f"https://api.telegram.org/bot{self._config.bot_token}/sendMessage"
        payload = {
            "chat_id": self._config.chat_id,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        try:
            import aiohttp
        except ImportError as exc:
            raise RuntimeError("aiohttp is required for Telegram notifications") from exc

        async with client_session() as session:
            async with session.post(url, json=payload, timeout=10) as response:
                response.raise_for_status()

    async def send_signal(self, signal: ArbitrageSignal, is_test: bool, min_net_spread: float) -> None:
        LOGGER.warning(
            "arbitrage_signal_raw_books",
            extra={
                "_pair": signal.market.symbol,
                "_raw_books": signal.raw_books,
            },
        )
        await self.send_html(format_signal_message(signal, is_test, min_net_spread))

    async def send_position_opened(self, signal: ArbitrageSignal, position: OpenPosition) -> None:
        await self.send_html(format_position_opened_message(signal, position))


def format_signal_message(signal: ArbitrageSignal, is_test: bool, min_net_spread: float) -> str:
    mode = "TEST MODE (Ордери заблоковані)" if is_test else "PRODUCTION"
    side = html.escape(signal.market.polymarket_side.value)
    predict_side = html.escape(signal.market.predict_fun_side.value)
    venue_a = html.escape(signal.market.venue_a_label)
    venue_b = html.escape(signal.market.venue_b_label)
    return (
        "🚨 <b>[ARBITRAGE SIGNAL DETECTED]</b>\n"
        f"Пара: {html.escape(signal.market.symbol)} (Target: {html.escape(signal.market.target_label)})\n"
        f"Режим: {mode}\n\n"
        f"📊 <b>РОЗРАХУНОК ПОЗИЦІЙ</b> (Cost: ${signal.plan.total_cost_usd:.2f}):\n"
        f"• {venue_a}: Купівля {side}\n"
        f" - Поточна ціна: ${signal.polymarket_price:.4f}\n"
        f" - Об'єм купівлі: {signal.plan.polymarket_contracts:.4f} контрактів\n"
        f" - Задіяний капітал: ${signal.plan.polymarket_capital_usd:.2f} USDC\n"
        f"• {venue_b}: Купівля {predict_side}\n"
        f" - Поточна ціна: ${signal.predict_fun_price:.4f}\n"
        f" - Об'єм купівлі: {signal.plan.predict_fun_contracts:.4f} контрактів\n"
        f" - Задіяний капітал: ${signal.plan.predict_fun_capital_usd:.2f}\n\n"
        "📈 <b>МЕТРИКА ПРИБУТКОВОСТІ:</b>\n"
        f"• Gross Спред: {signal.metrics.gross_spread:.2%}\n"
        f"• Combined Cost: ${signal.metrics.combined_cost_per_payout:.4f} за $1 payout\n"
        f"• Очікуваний чистий прибуток (Net Profit): ${signal.metrics.expected_net_profit_usd:+.2f}\n"
        f"• Поточний Net Spread: {signal.metrics.net_spread:.2%} "
        f"(Порог >{min_net_spread:.1%} пройдено)"
    )


def format_position_opened_message(signal: ArbitrageSignal, position: OpenPosition) -> str:
    venue_a = html.escape(signal.market.venue_a_label)
    venue_b = html.escape(signal.market.venue_b_label)
    return (
        "✅ <b>[POSITION OPENED]</b>\n"
        f"Пара: {html.escape(signal.market.symbol)} "
        f"(Target: {html.escape(signal.market.target_label)})\n\n"
        "📥 <b>ВІДКРИТТЯ ПОЗИЦІЇ:</b>\n"
        f"• {venue_a} order: {html.escape(position.polymarket_order_id)}\n"
        f"• {venue_b} order: {html.escape(position.predict_fun_order_id)}\n"
        f"• {venue_a} entry: ${position.polymarket_entry_price:.4f}\n"
        f"• {venue_b} entry: ${position.predict_fun_entry_price:.4f}\n"
        f"• Контракти payout: {position.polymarket_contracts:.4f}\n"
        f"• Загальна вартість: ${signal.plan.total_cost_usd:.2f}\n"
        f"• Приблизний прибуток при payout $1: "
        f"{signal.metrics.net_spread:.2%} (${signal.metrics.expected_net_profit_usd:+.2f})"
    )


def format_exit_message(signal: ExitSignal, is_test: bool) -> str:
    mode = "TEST MODE (Ордери заблоковані)" if is_test else "PRODUCTION"
    venue_a = html.escape(signal.position.market.venue_a_label)
    venue_b = html.escape(signal.position.market.venue_b_label)
    exit_spread_line = f"\n• Поточний spread після виходу: {signal.exit_spread:.2%}" if signal.exit_spread is not None else ""
    return (
        "✅ <b>[POSITION CLOSED]</b>\n"
        f"Пара: {html.escape(signal.position.market.symbol)} "
        f"(Target: {html.escape(signal.position.market.target_label)})\n"
        f"Режим: {mode}\n\n"
        "📤 <b>ЗАКРИТТЯ ПОЗИЦІЇ:</b>\n"
        f"• {venue_a} exit bid: ${signal.polymarket_exit_price:.4f}\n"
        f"• {venue_b} exit bid: ${signal.predict_fun_exit_price:.4f}\n"
        f"• Контракти payout: {signal.position.polymarket_contracts:.4f}\n"
        f"• Прибуток: {signal.profit_pct:.2%} (${signal.profit_usd:+.2f})"
        f"{exit_spread_line}"
    )
