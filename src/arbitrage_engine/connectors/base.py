from __future__ import annotations

from abc import ABC, abstractmethod

from arbitrage_engine.models import HedgeSide, OrderBook, PolymarketSide


class PolymarketClient(ABC):
    @abstractmethod
    async def watch_order_book(self, token_id: str) -> OrderBook:
        raise NotImplementedError

    @abstractmethod
    async def create_signed_order(self, token_id: str, side: PolymarketSide, contracts: float, max_price: float) -> str:
        raise NotImplementedError

    @abstractmethod
    async def close_position(self, token_id: str, side: PolymarketSide, contracts: float, min_price: float) -> str:
        raise NotImplementedError

    @abstractmethod
    async def wait_filled(self, order_id: str, timeout_ms: int) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def cancel_order(self, order_id: str) -> None:
        raise NotImplementedError

    @abstractmethod
    async def get_usdc_balance(self) -> float:
        raise NotImplementedError


class CefiFuturesClient(ABC):
    @abstractmethod
    async def watch_order_book(self, symbol: str) -> OrderBook:
        raise NotImplementedError

    @abstractmethod
    async def create_market_order(self, symbol: str, side: HedgeSide, quantity: float) -> str:
        raise NotImplementedError

    @abstractmethod
    async def close_market_order(self, symbol: str, entry_side: HedgeSide, quantity: float) -> str:
        raise NotImplementedError

    @abstractmethod
    async def get_usdt_balance(self) -> float:
        raise NotImplementedError
