from __future__ import annotations

import html
import logging

from .config import TelegramConfig
from .models import ArbitrageSignal

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
            import aiohttp  # type: ignore[import-not-found]
        except ImportError as exc:
            raise RuntimeError("aiohttp is required for Telegram notifications") from exc

        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=10) as response:
                response.raise_for_status()

    async def send_signal(self, signal: ArbitrageSignal, is_test: bool, min_net_spread: float) -> None:
        await self.send_html(format_signal_message(signal, is_test, min_net_spread))


def format_signal_message(signal: ArbitrageSignal, is_test: bool, min_net_spread: float) -> str:
    mode = "TEST MODE (Ордери заблоковані)" if is_test else "PRODUCTION"
    side = html.escape(signal.market.polymarket_side.value)
    hedge_side = html.escape(signal.market.cefi_hedge_side.value)
    return (
        "🚨 <b>[ARBITRAGE SIGNAL DETECTED]</b>\n"
        f"Пара: {html.escape(signal.market.symbol)} (Target: {html.escape(signal.market.target_label)})\n"
        f"Режим: {mode}\n\n"
        "📊 <b>РОЗРАХУНОК ПОЗИЦІЙ</b> (База: $100):\n"
        f"• Платформа А (Polymarket): Купівля {side}\n"
        f" - Поточна ціна: ${signal.polymarket_price:.4f}\n"
        f" - Об'єм купівлі: {signal.plan.polymarket_contracts:.4f} контрактів\n"
        f" - Задіяний капітал: ${signal.plan.polymarket_capital_usd:.2f} USDC\n"
        f"• Платформа Б (Binance Futures): Відкриття {hedge_side}\n"
        f" - Поточна ціна споту: ${signal.cefi_price:,.2f}\n"
        f" - Розмір хеджу (Дельта): {signal.plan.cefi_quantity:.8f} "
        f"{html.escape(signal.market.symbol.split('-')[0])}\n"
        f" - Задіяний маржинальний капітал: ${signal.plan.cefi_margin_usd:.2f} USDT\n\n"
        "📈 <b>МЕТРИКА ПРИБУТКОВОСТІ:</b>\n"
        f"• Gross Спред: {signal.metrics.gross_spread:.2%}\n"
        f"• Очікуваний чистий прибуток (Net Profit): ${signal.metrics.expected_net_profit_usd:+.2f}\n"
        f"• Поточний Net Spread: {signal.metrics.net_spread:.2%} "
        f"(Порог >{min_net_spread:.1%} пройдено)"
    )
