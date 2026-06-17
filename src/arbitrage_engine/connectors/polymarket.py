from __future__ import annotations

import asyncio
import logging
from typing import Any

from arbitrage_engine.config import PolymarketConfig
from arbitrage_engine.connectors.base import PolymarketClient
from arbitrage_engine.models import OrderBook, OrderBookLevel, PolymarketSide

LOGGER = logging.getLogger(__name__)


class PolymarketClobClient(PolymarketClient):
    def __init__(self, config: PolymarketConfig) -> None:
        self._config = config

    async def watch_order_book(self, token_id: str) -> OrderBook:
        try:
            import aiohttp  # type: ignore[import-not-found]
        except ImportError as exc:
            raise RuntimeError("aiohttp is required for Polymarket connectivity") from exc

        url = f"{self._config.api_base_url}/book"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params={"token_id": token_id}, timeout=10) as response:
                response.raise_for_status()
                raw: dict[str, Any] = await response.json()
        bids = [OrderBookLevel(float(item["price"]), float(item["size"])) for item in raw.get("bids", [])[:10]]
        asks = [OrderBookLevel(float(item["price"]), float(item["size"])) for item in raw.get("asks", [])[:10]]
        return OrderBook(bids=bids, asks=asks)

    async def create_signed_order(self, token_id: str, side: PolymarketSide, contracts: float, max_price: float) -> str:
        if not self._config.private_key:
            raise RuntimeError("POLYMARKET_PRIVATE_KEY is required for production orders")
        raise NotImplementedError(
            "Production Polymarket EIP-712 signing must be wired to the approved CLOB schema before live trading"
        )

    async def close_position(self, token_id: str, side: PolymarketSide, contracts: float, min_price: float) -> str:
        if not self._config.private_key:
            raise RuntimeError("POLYMARKET_PRIVATE_KEY is required for production exits")
        raise NotImplementedError(
            "Production Polymarket close order signing must be wired to the approved CLOB schema before live trading"
        )

    async def wait_filled(self, order_id: str, timeout_ms: int) -> bool:
        deadline = asyncio.get_running_loop().time() + timeout_ms / 1000
        while asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.025)
            raise NotImplementedError("Polymarket order status polling/websocket is not implemented")
        return False

    async def cancel_order(self, order_id: str) -> None:
        LOGGER.warning("cancel_order_not_implemented", extra={"_order_id": order_id})

    async def get_usdc_balance(self) -> float:
        raise NotImplementedError("Polymarket balance check must be connected to wallet/CLOB auth")
