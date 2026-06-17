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
        self._has_ws = False
        self._leveraged_symbols: set[tuple[str, float]] = set()

    async def _get_exchange(self) -> Any:
        if self._exchange is None:
            try:
                import ccxt.pro as ccxtpro  # type: ignore[import-untyped]
                self._exchange = ccxtpro.binanceusdm(
                    {
                        "apiKey": self._config.api_key,
                        "secret": self._config.api_secret,
                        "enableRateLimit": True,
                        "options": {"defaultType": "future"},
                    }
                )
                self._has_ws = True
                return self._exchange
            except ImportError:
                pass

            try:
                import ccxt.async_support as ccxt_async  # type: ignore[import-untyped]
            except ImportError as exc:
                raise RuntimeError("ccxt is required for production CeFi connectivity") from exc
            self._exchange = ccxt_async.binanceusdm(
                {
                    "apiKey": self._config.api_key,
                    "secret": self._config.api_secret,
                    "enableRateLimit": True,
                    "options": {"defaultType": "future"},
                }
            )
            self._has_ws = False
        return self._exchange

    async def watch_order_book(self, symbol: str) -> OrderBook:
        exchange = await self._get_exchange()
        if self._has_ws:
            raw = await exchange.watch_order_book(symbol, limit=10)
        else:
            raw = await exchange.fetch_order_book(symbol, limit=10)
        bids = [OrderBookLevel(float(price), float(size)) for price, size in raw["bids"][:10]]
        asks = [OrderBookLevel(float(price), float(size)) for price, size in raw["asks"][:10]]
        return OrderBook(bids=bids, asks=asks)

    async def create_market_order(self, symbol: str, side: HedgeSide, quantity: float) -> str:
        exchange = await self._get_exchange()
        if not self._config.api_key or not self._config.api_secret:
            raise RuntimeError("BINANCE_API_KEY and BINANCE_API_SECRET are required for production orders")
        ccxt_side = "buy" if side is HedgeSide.LONG else "sell"
        order = await exchange.create_order(symbol, "market", ccxt_side, quantity)
        return str(order["id"])

    async def set_leverage(self, symbol: str, leverage: float) -> None:
        key = (symbol, leverage)
        if key in self._leveraged_symbols:
            return
        exchange = await self._get_exchange()
        setter = getattr(exchange, "set_leverage", None)
        if setter is None:
            LOGGER.warning("cefi_set_leverage_unavailable", extra={"_symbol": symbol, "_leverage": leverage})
            return
        await setter(int(leverage), symbol)
        self._leveraged_symbols.add(key)

    async def close_market_order(self, symbol: str, entry_side: HedgeSide, quantity: float) -> str:
        return await self.create_market_order(symbol, opposite_hedge_side(entry_side), quantity)

    async def get_usdt_balance(self) -> float:
        exchange = await self._get_exchange()
        balance = await exchange.fetch_balance()
        return float(balance.get("USDT", {}).get("free", 0.0))

    async def close(self) -> None:
        if self._exchange is not None:
            close = getattr(self._exchange, "close", None)
            if close is not None:
                await close()
