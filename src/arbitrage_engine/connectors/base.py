from __future__ import annotations

import random
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
    OrderPreview,
    RedemptionReport,
    SettlementRequest,
    SettlementStatus,
    VenueFeeQuote,
    VenueOrder,
)


class OrderBookStaleException(RuntimeError):
    """Raised when a venue cannot provide a sufficiently recent order book."""


class OrderBookUnavailableException(RuntimeError):
    """Raised when a venue has no usable two-sided book for a market."""


class ReconciliationUnsupported(RuntimeError):
    """Raised when a venue cannot provide the account-level reconciliation contract."""


class WebSocketReconnectBackoff:
    """Bounded full-jitter backoff with a non-zero reconnect delay."""

    def __init__(self, initial_seconds: float = 1.0, maximum_seconds: float = 30.0) -> None:
        self._initial_seconds = initial_seconds
        self._maximum_seconds = maximum_seconds
        self._attempt = 0
        self.current_delay_seconds = 0.0

    def next_delay(self) -> float:
        ceiling = min(self._initial_seconds * (2**self._attempt), self._maximum_seconds)
        self._attempt += 1
        self.current_delay_seconds = max(0.1, random.uniform(0.0, ceiling))
        return self.current_delay_seconds

    def reset(self) -> None:
        self._attempt = 0
        self.current_delay_seconds = 0.0


class BinaryMarketClient(ABC):
    venue_name = "Unknown"

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

    async def get_fee_quote(
        self,
        token_id: str,
        average_price: Decimal,
        constraints: MarketConstraints | None = None,
    ) -> VenueFeeQuote | None:
        del average_price
        resolved = constraints or await self.get_market_constraints(token_id)
        if resolved is None:
            return None
        model = "zero_fee" if resolved.fee_rate_bps == 0 else "notional_bps"
        return VenueFeeQuote(self.venue_name, resolved.fee_rate_bps, model)

    async def preview_buy(
        self,
        token_id: str,
        side: BinarySide,
        contracts: Decimal,
        max_price: Decimal,
        *,
        condition_id: str | None = None,
        tick_size: str | None = None,
        neg_risk: bool | None = None,
    ) -> OrderPreview:
        blockers: list[str] = []
        if contracts <= 0:
            blockers.append("contracts_not_positive")
        if max_price <= 0 or max_price > 1:
            blockers.append("limit_price_out_of_range")
        constraints = await self.get_market_constraints(token_id, condition_id)
        if constraints is None:
            blockers.append("constraints_unavailable")
        book = await self.watch_order_book(token_id)
        if book.status.value != "VALID":
            blockers.append(f"orderbook_status:{book.status.value.lower()}")
        if not book.asks:
            blockers.append("asks_unavailable")

        requested = max(Decimal(0), contracts)
        remaining = requested
        filled = Decimal(0)
        spent = Decimal(0)
        available_depth = Decimal(0)
        best_ask: Decimal | None = None
        for level in book.asks:
            price = Decimal(str(level.price))
            size = Decimal(str(level.size))
            if price <= 0 or size <= 0 or price > max_price:
                continue
            if best_ask is None:
                best_ask = price
            available_depth += price * size
            take = min(remaining, size)
            if take > 0:
                filled += take
                spent += take * price
                remaining -= take
        if remaining > Decimal("1e-18"):
            blockers.append("insufficient_executable_depth")
        average_price = spent / filled if filled > 0 else Decimal(0)
        if constraints is not None and spent < constraints.minimum_notional:
            blockers.append("minimum_notional_not_met")
        fee_quote = await self.get_fee_quote(token_id, average_price, constraints)
        if fee_quote is None:
            blockers.append("fee_metadata_unavailable")
            expected_fee = Decimal(0)
        else:
            expected_fee = fee_quote.fee_for_fill(filled, average_price)
        price_impact = (
            max(Decimal(0), (average_price - best_ask) / best_ask)
            if best_ask is not None and best_ask > 0
            else Decimal(0)
        )
        payload_fingerprint: str | None = None
        if not blockers:
            payload_fingerprint = await self._preview_buy_signature(
                token_id,
                side,
                contracts,
                max_price,
                condition_id=condition_id,
                tick_size=tick_size,
                neg_risk=neg_risk,
            )
            if not payload_fingerprint:
                blockers.append("signature_preview_unavailable")
        return OrderPreview(
            venue=self.venue_name,
            token_id=token_id,
            side=side,
            requested_contracts=requested,
            limit_price=max_price,
            average_price=average_price,
            notional_usd=spent,
            available_depth_usd=available_depth,
            price_impact_pct=price_impact,
            expected_fee_usd=expected_fee,
            fee_quote=fee_quote,
            constraints=constraints,
            signing_validated=bool(payload_fingerprint),
            payload_fingerprint=payload_fingerprint,
            blockers=tuple(dict.fromkeys(blockers)),
        )

    async def _preview_buy_signature(
        self,
        token_id: str,
        side: BinarySide,
        contracts: Decimal,
        max_price: Decimal,
        *,
        condition_id: str | None,
        tick_size: str | None,
        neg_risk: bool | None,
    ) -> str | None:
        del token_id, side, contracts, max_price, condition_id, tick_size, neg_risk
        return None

    def supports_full_reconciliation(self) -> bool:
        return False

    def supports_automatic_redemption(self) -> bool:
        return False

    def prepare_settlement_request(self, request: SettlementRequest) -> SettlementRequest:
        return request

    async def get_settlement_status(self, request: SettlementRequest) -> SettlementStatus:
        del request
        return SettlementStatus.MANUAL_REVIEW

    async def redeem_position(self, request: SettlementRequest, redemption_id: str) -> RedemptionReport:
        del request, redemption_id
        raise ReconciliationUnsupported(f"{type(self).__name__} does not implement automatic redemption")

    async def reconcile_redemption(
        self,
        request: SettlementRequest,
        report: RedemptionReport,
    ) -> RedemptionReport:
        del request
        return report

    def reconciliation_clock(self) -> datetime:
        return datetime.now(UTC)

    def forget_order(self, order_id: str) -> None:
        """Release connector-local bookkeeping after final reconciliation."""
        del order_id

    def market_data_age_seconds(self) -> float | None:
        """Return age of the latest real event on the venue stream, if any."""
        return None

    def sync_market_data_targets(self, token_ids: set[str]) -> None:
        del token_ids

    def has_active_market_data_targets(self) -> bool:
        return True

    def active_market_data_target_count(self) -> int:
        return int(self.has_active_market_data_targets())

    async def reconnect_market_data(self) -> None:
        """Reconnect streaming market data when the venue supports it."""
        return None

    def set_market_data_snapshot_interval(self, seconds: float) -> None:
        del seconds

    def set_market_data_execution_freshness(self, seconds: float) -> None:
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
