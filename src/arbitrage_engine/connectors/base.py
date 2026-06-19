from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
import time
from typing import Any

from arbitrage_engine.models import BinarySide, ExecutionReport, OrderBook


class OrderBookStaleException(RuntimeError):
    """Raised when a venue cannot provide a sufficiently recent order book."""


class OrderBookUnavailableException(RuntimeError):
    """Raised when a venue has no usable two-sided book for a market."""


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

    def forget_order(self, order_id: str) -> None:
        """Release connector-local bookkeeping after final reconciliation."""
        del order_id

    def market_data_age_seconds(self) -> float | None:
        """Return age of the stalest active streaming subscription, if any."""
        return None

    async def reconnect_market_data(self) -> None:
        """Reconnect streaming market data when the venue supports it."""
        return None


class PolymarketClient(BinaryMarketClient, ABC):
    pass


class PredictFunClient(BinaryMarketClient, ABC):
    pass


def event_timestamp(payload: Any) -> float:
    """Extract a venue update time, falling back to local receipt time."""
    if isinstance(payload, dict):
        for key in (
            "updateTimestampMs",
            "updatedTimestampMs",
            "timestampMs",
            "updated_at",
            "updatedAt",
            "timestamp",
            "ts",
        ):
            value = payload.get(key)
            parsed = _parse_event_timestamp(value)
            if parsed is not None:
                return min(parsed, time.time())
        for key in ("data", "orderbook", "orderBook", "book", "source", "pub"):
            nested = payload.get(key)
            if isinstance(nested, dict):
                parsed = event_timestamp(nested)
                if parsed < time.time() - 0.001:
                    return parsed
    return time.time()


def _parse_event_timestamp(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        if isinstance(value, str) and not value.replace(".", "", 1).isdigit():
            return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        parsed = float(value)
        if parsed > 10_000_000_000:
            parsed /= 1_000.0
        return parsed if parsed > 0 else None
    except (TypeError, ValueError):
        return None
