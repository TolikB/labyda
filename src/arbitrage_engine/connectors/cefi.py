from __future__ import annotations

import logging
from typing import Any

from arbitrage_engine.config import BinanceConfig
from arbitrage_engine.connectors.base import CefiFuturesClient
from arbitrage_engine.models import HedgeSide, OrderBook, OrderBookLevel, opposite_hedge_side

LOGGER = logging.getLogger(__name__)


class CcxtProBinanceFuturesClient(CefiFuturesClient):
    def __init__(self, config: BinanceConfig) -> None:
        self._config = config
        self._exchange: Any | None = None

    async def _get_exchange(self) -> Any:
        if self._exchange is None:
            try:
                import ccxt.pro as ccxtpro  # type: ignore[import-not-found]
            except ImportError as exc:
                raise RuntimeError("ccxt.pro is required for production CeFi connectivity") from exc
            self._exchange = ccxtpro.binanceusdm(
                {
                    "apiKey": self._config.api_key,
                    "secret": self._config.api_secret,
                    "enableRateLimit": True,
                    "options": {"defaultType": "future"},
                }
            )
        return self._exchange

    async def watch_order_book(self, symbol: str) -> OrderBook:
        exchange = await self._get_exchange()
        raw = await exchange.watch_order_book(symbol, limit=10)
        bids = [OrderBookLevel(float(price), float(size)) for price, size in raw["bids"][:10]]
        asks = [OrderBookLevel(float(price), float(size)) for price, size in raw["asks"][:10]]
        return OrderBook(bids=bids, asks=asks)

    async def create_market_order(self, symbol: str, side: HedgeSide, quantity: float) -> str:
        exchange = await self._get_exchange()
        ccxt_side = "buy" if side is HedgeSide.LONG else "sell"
        order = await exchange.create_order(symbol, "market", ccxt_side, quantity)
        return str(order["id"])

    async def close_market_order(self, symbol: str, entry_side: HedgeSide, quantity: float) -> str:
        return await self.create_market_order(symbol, opposite_hedge_side(entry_side), quantity)

    async def get_usdt_balance(self) -> float:
        exchange = await self._get_exchange()
        balance = await exchange.fetch_balance()
        return float(balance.get("USDT", {}).get("free", 0.0))
