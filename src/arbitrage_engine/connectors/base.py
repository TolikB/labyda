from __future__ import annotations

import time
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from arbitrage_engine.models import (
    BinarySide,
    ExecutionReport,
    FillRecord,
    MarketConstraints,
    OrderBook,
    OrderIntent,
    SettlementStatus,
    VenueOrder,
)


class OrderBookStaleException(RuntimeError):
    """Raised when a venue cannot provide a sufficiently recent order book."""


class OrderBookUnavailableException(RuntimeError):
    """Raised when a venue has no usable two-sided book for a market."""


class ReconciliationUnsupported(RuntimeError):
    """Raised when a venue cannot provide the account-level reconciliation contract."""


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

    async def submit_order(self, intent: OrderIntent) -> str:
        if intent.action.upper() == "BUY":
            return await self.buy(
                intent.token_id,
                intent.binary_side,
                float(intent.quantity),
                float(intent.limit_price),
            )
        if intent.action.upper() == "SELL":
            return await self.sell(
                intent.token_id,
                intent.binary_side,
                float(intent.quantity),
                float(intent.limit_price),
            )
        raise ValueError(f"Unsupported order action: {intent.action}")

    async def get_order(self, order_id: str) -> ExecutionReport:
        return await self.wait_filled(order_id, 1)

    async def list_open_orders(self) -> list[VenueOrder]:
        raise ReconciliationUnsupported(f"{type(self).__name__} does not implement list_open_orders")

    async def list_fills(self, since: datetime | None = None) -> list[FillRecord]:
        del since
        raise ReconciliationUnsupported(f"{type(self).__name__} does not implement list_fills")

    async def get_balances(self) -> dict[str, Decimal]:
        return {"cash": Decimal(str(await self.get_cash_balance()))}

    async def get_positions(self) -> dict[str, Decimal]:
        raise ReconciliationUnsupported(f"{type(self).__name__} does not implement get_positions")

    async def get_market_constraints(self, token_id: str, condition_id: str | None = None) -> MarketConstraints | None:
        del token_id, condition_id
        return None

    def supports_full_reconciliation(self) -> bool:
        return False

    async def get_settlement_status(self, market_id: str) -> SettlementStatus:
        del market_id
        return SettlementStatus.MANUAL_REVIEW

    async def redeem_position(self, market_id: str) -> str:
        del market_id
        raise ReconciliationUnsupported(f"{type(self).__name__} does not implement automatic redemption")

    def reconciliation_clock(self) -> datetime:
        return datetime.now(UTC)

    def forget_order(self, order_id: str) -> None:
        """Release connector-local bookkeeping after final reconciliation."""
        del order_id

    def market_data_age_seconds(self) -> float | None:
        """Return age of the stalest active streaming subscription, if any."""
        return None

    async def reconnect_market_data(self) -> None:
        """Reconnect streaming market data when the venue supports it."""
        return None

    def set_market_data_snapshot_interval(self, seconds: float) -> None:
        del seconds

    def market_data_ready(self) -> bool:
        return True

    def telemetry_snapshot(self) -> dict[str, float]:
        return {}

    async def close(self) -> None:
        """Release connector resources."""
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


def event_sequence(payload: Any) -> int | None:
    if not isinstance(payload, dict):
        return None
    for key in ("sequence", "sequence_number", "sequenceNumber", "seq", "version"):
        value = payload.get(key)
        if value not in (None, ""):
            try:
                return int(str(value))
            except (TypeError, ValueError):
                return None
    for key in ("data", "orderbook", "orderBook", "book", "source", "pub"):
        nested = payload.get(key)
        if isinstance(nested, dict) and (sequence := event_sequence(nested)) is not None:
            return sequence
    return None


def event_checksum(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    for key in ("checksum", "bookHash", "book_hash", "hash"):
        value = payload.get(key)
        if value not in (None, ""):
            return str(value)
    return None


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
