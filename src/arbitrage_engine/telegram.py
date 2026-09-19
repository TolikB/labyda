from __future__ import annotations

import asyncio
import html
import logging
import time
from decimal import Decimal
from typing import Any

from .config import TelegramConfig
from .http import client_session
from .models import ArbitrageSignal, ExitSignal, MarketSpec, OpenPosition

LOGGER = logging.getLogger(__name__)


class TelegramNotifier:
    def __init__(self, config: TelegramConfig) -> None:
        self._config = config
        self._rest_session: Any | None = None
        self._send_lock = asyncio.Lock()
        self._last_sent_at = 0.0

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

            _ = aiohttp
        except ImportError as exc:
            raise RuntimeError("aiohttp is required for Telegram notifications") from exc

        try:
            async with self._send_lock:
                delay = self._config.min_interval_seconds - (time.monotonic() - self._last_sent_at)
                if delay > 0:
                    await asyncio.sleep(delay)
                session = self._get_rest_session()
                for attempt in range(2):
                    async with session.post(url, json=payload, timeout=10) as response:
                        if response.status == 429 and attempt == 0:
                            try:
                                body = await response.json()
                                retry_after = float(body.get("parameters", {}).get("retry_after", 1.0))
                            except (AttributeError, TypeError, ValueError):
                                retry_after = 1.0
                            await asyncio.sleep(max(0.1, retry_after))
                            continue
                        response.raise_for_status()
                        break
                self._last_sent_at = time.monotonic()
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception("telegram_send_failed")

    def _get_rest_session(self) -> Any:
        if self._rest_session is None or self._rest_session.closed:
            self._rest_session = client_session()
        return self._rest_session

    async def close(self) -> None:
        if self._rest_session is not None and not self._rest_session.closed:
            await self._rest_session.close()
        self._rest_session = None

    async def send_signal(self, signal: ArbitrageSignal, is_test: bool, min_net_spread: float) -> None:
        if self._config.log_raw_signal_books:
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


WINDOW_COMPLETE_PAUSE_REASON = "funded_canary_window_complete"
WRAPPER_EXIT_PAUSE_REASON = "production_closeout_exit_fail_closed"
WRAPPER_SHADOW_SETUP_PAUSE_REASON = "production_closeout_shadow_setup"

# The operator asked for two kinds of message and no others: something a
# human has to act on, and the P&L of a position that closed. A pause the
# wrapper placed on schedule, or one the wrapper recovers from on its own
# (a venue that stayed broken for a while, the daily loss limit until
# midnight), is neither.
_SILENT_PAUSE_REASON_PREFIXES: tuple[str, ...] = (
    WINDOW_COMPLETE_PAUSE_REASON,
    WRAPPER_EXIT_PAUSE_REASON,
    WRAPPER_SHADOW_SETUP_PAUSE_REASON,
    "continuous reconciliation transient failure",
    "daily realized loss ",
)
_SILENT_PAUSE_REASON_SUFFIXES: tuple[str, ...] = (" consecutive execution API errors",)
# Pauses whose site already sends a message with the details; the generic
# alert would only repeat it.
_COVERED_PAUSE_REASON_PREFIXES: tuple[str, ...] = (
    "Polymarket trading geoblocked",
    "filled execution report missing avg_price",
    "unwind exhausted",
    "settlement manual review required",
)
_COVERED_PAUSE_REASON_MARKERS: tuple[str, ...] = (
    "unresolved entry intent(s) found after restart",
    "unresolved redemption intent(s) found after restart",
)


def risk_pause_alert(reason: str | None, daily_loss_usd: Decimal, instance_id: str) -> str | None:
    """The message for a risk pause, or None when nobody needs to read one.

    Returns None for the wrapper's own pauses, for pauses the wrapper recovers
    from by itself, and for pauses whose site already sent the details.
    Everything else -- drift, an order with an unknown outcome, residual
    exposure, a failed reconciliation, an operator drain -- stays paused until
    a human looks, and says so.
    """
    text = reason or ""
    if any(text.startswith(prefix) for prefix in _SILENT_PAUSE_REASON_PREFIXES):
        return None
    if any(text.endswith(suffix) for suffix in _SILENT_PAUSE_REASON_SUFFIXES):
        return None
    if any(text.startswith(prefix) for prefix in _COVERED_PAUSE_REASON_PREFIXES):
        return None
    if any(marker in text for marker in _COVERED_PAUSE_REASON_MARKERS):
        return None
    return (
        "\U0001F6A8 <b>RISK PAUSED \u2014 trading halted</b>\n"
        f"Reason: {html.escape(reason or 'unspecified')}\n"
        f"Daily realized loss: ${daily_loss_usd:.2f}\n"
        f"Instance: {html.escape(instance_id)}\n"
        "Stays halted until an operator runs <code>risk resume</code>."
    )


def format_settlement_message(
    position: OpenPosition,
    *,
    payout_contracts: Decimal,
    entry_cost_usd: Decimal,
) -> str:
    """P&L of a hedged pair the venues have resolved and paid out.

    A fully hedged pair pays $1 per contract on exactly one leg, whichever
    way the market resolved, so the payout is the matched contract count.
    Entry cost is what the fills cost at their prices; the venues' fees are
    not recorded per fill, so the figure is before fees and says so.
    """
    market = position.market
    pnl = payout_contracts - entry_cost_usd
    pct = (pnl / entry_cost_usd) if entry_cost_usd > 0 else Decimal(0)
    venue_a = html.escape(market.venue_a_label)
    venue_b = html.escape(market.venue_b_label)
    unmatched = position.polymarket_contracts - position.predict_fun_contracts
    residual = ""
    if unmatched != 0:
        heavier = venue_a if unmatched > 0 else venue_b
        residual = (
            f"\n\u26a0\ufe0f Незбалансовано: {abs(unmatched):.4f} контр. на {heavier} "
            "(виплата залежить від результату, не врахована)"
        )
    return (
        "\U0001F4B0 <b>[POSITION SETTLED]</b>\n"
        f"Пара: {html.escape(market.symbol)} (Target: {html.escape(market.target_label)})\n"
        f"\u2022 {venue_a}: {position.polymarket_contracts:.4f} контр. @ ${position.polymarket_entry_price:.4f}\n"
        f"\u2022 {venue_b}: {position.predict_fun_contracts:.4f} контр. @ ${position.predict_fun_entry_price:.4f}\n"
        f"Виплата: ${payout_contracts:.2f} \u2022 Вхід: ${entry_cost_usd:.2f}\n"
        f"<b>PnL: ${pnl:+.2f} ({pct:+.2%})</b> до комісій венью"
        f"{residual}"
        f"{_format_market_links(market)}"
    )


def format_signal_message(signal: ArbitrageSignal, is_test: bool, min_net_spread: float) -> str:
    mode = "TEST MODE (Ордери заблоковані)" if is_test else "PRODUCTION"
    side = html.escape(signal.market.polymarket_side.value)
    predict_side = html.escape(signal.market.predict_fun_side.value)
    venue_a = html.escape(signal.market.venue_a_label)
    venue_b = html.escape(signal.market.venue_b_label)
    market_links = _format_market_links(signal.market)
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
        f"{market_links}"
    )


def _format_market_links(market: MarketSpec) -> str:
    urls = {
        "Polymarket": market.polymarket_url,
        "Predict.fun": market.predict_fun_url
        or (f"https://predict.fun/market/{market.predict_fun_market_id}" if market.predict_fun_market_id else None),
        "SX Bet": market.predict_fun_url
        or (f"https://sx.bet/market/{market.predict_fun_market_id}" if market.predict_fun_market_id else None),
        "Myriad": market.myriad_url
        or (f"https://myriad.markets/markets/{market.myriad_market_id}" if market.myriad_market_id else None),
    }
    links: list[str] = []
    for venue in (market.venue_a_label, market.venue_b_label):
        url = urls.get(venue)
        if not url or not url.startswith(("https://", "http://")):
            continue
        links.append(f'<a href="{html.escape(url, quote=True)}">{html.escape(venue)}</a>')
    return "\n\n🔗 <b>Маркети:</b> " + " • ".join(links) if links else ""


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
    exit_spread_line = (
        f"\n• Поточний spread після виходу: {signal.exit_spread:.2%}" if signal.exit_spread is not None else ""
    )
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
