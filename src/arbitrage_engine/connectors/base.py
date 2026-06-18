from __future__ import annotations

from abc import ABC, abstractmethod

from arbitrage_engine.models import BinarySide, ExecutionReport, OrderBook


class BinaryMarketClient(ABC):
    @abstractmethod
    async def watch_order_book(self, token_id: str) -> OrderBook:
        raise NotImplementedError

    @abstractmethod
    async def buy(
        self,
        token_id: str,
        side: BinarySide,
        contracts: float,
        max_price: float,
        *,
        condition_id: str | None = None,
        tick_size: str | None = None,
        neg_risk: bool | None = None,
    ) -> str:
        raise NotImplementedError

    @abstractmethod
    async def sell(
        self,
        token_id: str,
        side: BinarySide,
        contracts: float,
        min_price: float,
        *,
        condition_id: str | None = None,
        tick_size: str | None = None,
        neg_risk: bool | None = None,
    ) -> str:
        raise NotImplementedError

    @abstractmethod
    async def wait_filled(self, order_id: str, timeout_ms: int) -> ExecutionReport:
        raise NotImplementedError

    @abstractmethod
    async def cancel_order(self, order_id: str) -> None:
        raise NotImplementedError

    @abstractmethod
    async def get_cash_balance(self) -> float:
        raise NotImplementedError


class PolymarketClient(BinaryMarketClient, ABC):
    pass


class PredictFunClient(BinaryMarketClient, ABC):
    pass
