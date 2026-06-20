from __future__ import annotations

import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any


class BinarySide(str, Enum):
    YES = "YES"
    NO = "NO"


class ExecutionStatus(str, Enum):
    OPEN = "OPEN"
    PARTIAL = "PARTIAL"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"


class ExecutionMode(str, Enum):
    PAPER = "paper"
    SHADOW = "shadow"
    CANARY = "canary"
    LIVE = "live"

    @property
    def submits_orders(self) -> bool:
        return self in {ExecutionMode.CANARY, ExecutionMode.LIVE}


class MappingStatus(str, Enum):
    CANDIDATE = "CANDIDATE"
    VERIFIED = "VERIFIED"
    REJECTED = "REJECTED"
    STALE = "STALE"


class OrderIntentStatus(str, Enum):
    PREPARED = "PREPARED"
    SUBMITTING = "SUBMITTING"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    PARTIAL = "PARTIAL"
    FILLED = "FILLED"
    CANCEL_PENDING = "CANCEL_PENDING"
    CANCELLED = "CANCELLED"
    UNKNOWN = "UNKNOWN"
    MANUAL_REVIEW = "MANUAL_REVIEW"


class MarketDataStatus(str, Enum):
    VALID = "VALID"
    INVALID = "INVALID"
    STALE = "STALE"


class SettlementStatus(str, Enum):
    OPEN = "OPEN"
    RESOLVED = "RESOLVED"
    VOID = "VOID"
    REDEEM_PENDING = "REDEEM_PENDING"
    SETTLED = "SETTLED"
    MANUAL_REVIEW = "MANUAL_REVIEW"


PolymarketSide = BinarySide


def opposite_binary_side(side: BinarySide) -> BinarySide:
    return BinarySide.NO if side is BinarySide.YES else BinarySide.YES


def _execution_status(
    value: str | ExecutionStatus,
    amount_filled: float,
    amount_requested: float,
) -> ExecutionStatus:
    if isinstance(value, ExecutionStatus):
        return value
    normalized = value.strip().lower().replace("-", "_")
    if normalized in {"filled", "matched", "executed", "complete", "completed"}:
        return ExecutionStatus.FILLED
    if normalized in {"partial", "partially_filled"} or 0 < amount_filled < amount_requested:
        return ExecutionStatus.PARTIAL
    if normalized in {"cancelled", "canceled", "rejected", "failed"}:
        return ExecutionStatus.CANCELLED
    if normalized == "expired":
        return ExecutionStatus.EXPIRED
    return ExecutionStatus.OPEN


@dataclass(frozen=True)
class OrderBookLevel:
    price: float
    size: float


@dataclass(frozen=True)
class OrderBook:
    bids: Sequence[OrderBookLevel]
    asks: Sequence[OrderBookLevel]
    raw_payload: Any | None = None
    timestamp: float = field(default_factory=time.time)
    sequence: int | None = None
    checksum: str | None = None
    status: MarketDataStatus = MarketDataStatus.VALID

    @property
    def best_bid(self) -> OrderBookLevel:
        if not self.bids:
            raise ValueError("order book has no bids")
        return self.bids[0]

    @property
    def best_ask(self) -> OrderBookLevel:
        if not self.asks:
            raise ValueError("order book has no asks")
        return self.asks[0]


@dataclass(frozen=True)
class ExecutionReport:
    order_id: str
    status: ExecutionStatus
    amount_requested: float
    amount_filled: float
    remaining_amount: float
    avg_price: float
    client_order_id: str | None = None
    venue_order_id: str | None = None
    submitted_at: datetime | None = None
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    cumulative_filled: Decimal | None = None

    @property
    def requested_amount(self) -> float:
        return self.amount_requested

    @property
    def is_filled(self) -> bool:
        return self.remaining_amount <= 1e-9 and self.amount_filled > 0

    @property
    def has_fill(self) -> bool:
        return self.amount_filled > 1e-9

    @classmethod
    def from_amounts(
        cls,
        order_id: str,
        amount_requested: float,
        amount_filled: float,
        status: str | ExecutionStatus,
        avg_price: float = 0.0,
    ) -> ExecutionReport:
        filled = min(max(0.0, amount_filled), max(0.0, amount_requested))
        normalized_status = _execution_status(status, filled, amount_requested)
        return cls(
            order_id=order_id,
            status=normalized_status,
            amount_requested=amount_requested,
            amount_filled=filled,
            remaining_amount=max(0.0, amount_requested - filled),
            avg_price=max(0.0, avg_price),
            venue_order_id=order_id,
            cumulative_filled=Decimal(str(filled)),
        )


@dataclass(frozen=True)
class MarketConstraints:
    fee_rate_bps: int
    tick_size: Decimal
    lot_size: Decimal
    minimum_notional: Decimal
    fetched_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(frozen=True)
class CanonicalMarket:
    canonical_id: str
    title: str
    category: str
    resolution_source: str
    cutoff_at: datetime
    timezone_name: str
    outcome_semantics: str
    rules_fingerprint: str


@dataclass(frozen=True)
class VenueInstrument:
    venue: str
    market_id: str
    yes_token_id: str
    no_token_id: str
    closes_at: datetime
    resolution_source: str
    rules_fingerprint: str
    constraints: MarketConstraints | None = None


@dataclass(frozen=True)
class MarketMapping:
    mapping_id: str
    canonical_market_id: str
    left_venue: str
    left_market_id: str
    right_venue: str
    right_market_id: str
    status: MappingStatus
    rules_fingerprint: str
    verified_at: datetime | None = None
    verified_by: str | None = None


@dataclass(frozen=True)
class OrderIntent:
    client_order_id: str
    route: str
    market_key: str
    venue: str
    token_id: str
    binary_side: BinarySide
    action: str
    quantity: Decimal
    limit_price: Decimal
    status: OrderIntentStatus = OrderIntentStatus.PREPARED
    venue_order_id: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(frozen=True)
class VenueOrder:
    client_order_id: str
    venue_order_id: str
    venue: str
    status: OrderIntentStatus
    quantity: Decimal
    cumulative_filled: Decimal
    average_price: Decimal
    updated_at: datetime


@dataclass(frozen=True)
class FillRecord:
    fill_id: str
    client_order_id: str
    venue_order_id: str
    venue: str
    quantity: Decimal
    price: Decimal
    fee: Decimal
    occurred_at: datetime


@dataclass(frozen=True)
class ReconciliationResult:
    venue: str
    started_at: datetime
    completed_at: datetime
    orders_checked: int
    fills_recorded: int
    drift_count: int
    success: bool
    error: str | None = None


@dataclass(frozen=True)
class AmmPool:
    yes_reserve: float
    no_reserve: float
    fee_pct: float = 0.0


@dataclass(frozen=True)
class MarketSpec:
    symbol: str
    target_label: str
    polymarket_token_id: str
    polymarket_side: BinarySide
    predict_fun_token_id: str
    predict_fun_side: BinarySide
    venue_a_label: str = "Polymarket"
    venue_b_label: str = "Predict.fun"
    expires_at: datetime | None = None
    condition_id: str | None = None
    polymarket_market_id: str | None = None
    polymarket_url: str | None = None
    tick_size: str | None = None
    neg_risk: bool | None = None
    predict_fun_neg_risk: bool | None = None
    predict_fun_fee_rate_bps: int | None = None
    predict_fun_market_id: str | None = None
    predict_fun_url: str | None = None
    predict_fun_amm_pool: AmmPool | None = None
    myriad_market_id: str | None = None
    myriad_url: str | None = None
    myriad_side: BinarySide = BinarySide.NO
    rules_fingerprint: str | None = None
    polymarket_volume_usd: float | None = None
    predict_fun_volume_usd: float | None = None
    myriad_volume_usd: float | None = None
    category: str | None = None
    mapping_status: MappingStatus = MappingStatus.CANDIDATE
    resolution_source: str | None = None
    outcome_semantics: str | None = None
    cutoff_at: datetime | None = None
    timezone_name: str = "UTC"
    verified_routes: frozenset[str] = frozenset()


@dataclass(frozen=True)
class PositionPlan:
    polymarket_contracts: float
    polymarket_capital_usd: float
    predict_fun_contracts: float
    predict_fun_capital_usd: float
    payout_contracts: float
    total_cost_usd: float
    polymarket_fee_usd: float = 0.0
    predict_fun_fee_usd: float = 0.0


@dataclass(frozen=True)
class SpreadMetrics:
    gross_spread: float
    net_spread: float
    expected_net_profit_usd: float
    polymarket_slippage: float
    predict_fun_slippage: float
    combined_cost_per_payout: float


@dataclass(frozen=True)
class ArbitrageSignal:
    market: MarketSpec
    plan: PositionPlan
    metrics: SpreadMetrics
    polymarket_price: float
    predict_fun_price: float
    raw_books: dict[str, Any] | None = None


@dataclass(frozen=True)
class OpenPosition:
    market: MarketSpec
    polymarket_contracts: float
    polymarket_entry_price: float
    predict_fun_contracts: float
    predict_fun_entry_price: float
    opened_at: datetime
    polymarket_order_id: str
    predict_fun_order_id: str
    status: str = "open"
    polymarket_unwind_attempts: int = 0
    polymarket_closed: bool = False
    predict_fun_closed: bool = False
    polymarket_exit_price: float | None = None
    predict_fun_exit_price: float | None = None
    unmatched_first_contracts: float = 0.0
    unmatched_second_contracts: float = 0.0
    polymarket_closed_contracts: float = 0.0
    predict_fun_closed_contracts: float = 0.0
    polymarket_exit_proceeds_usd: float = 0.0
    predict_fun_exit_proceeds_usd: float = 0.0


@dataclass(frozen=True)
class ExitSignal:
    position: OpenPosition
    polymarket_exit_price: float
    predict_fun_exit_price: float
    profit_pct: float
    profit_usd: float
    exit_spread: float | None = None


def position_key(market: MarketSpec) -> str:
    fingerprint = market.rules_fingerprint or f"{market.symbol}:{market.target_label}"
    return (
        f"{fingerprint}:"
        f"{market.venue_a_label}:{market.polymarket_token_id}:{market.polymarket_side.value}:"
        f"{market.venue_b_label}:{market.predict_fun_token_id}:{market.predict_fun_side.value}"
    )
