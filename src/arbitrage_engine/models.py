from __future__ import annotations

import time
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
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


class RedemptionIntentStatus(str, Enum):
    PENDING = "PENDING"
    SUBMITTED = "SUBMITTED"
    CONFIRMED = "CONFIRMED"
    UNKNOWN = "UNKNOWN"
    FAILED = "FAILED"
    MANUAL_REVIEW = "MANUAL_REVIEW"


PolymarketSide = BinarySide


def opposite_binary_side(side: BinarySide) -> BinarySide:
    return BinarySide.NO if side is BinarySide.YES else BinarySide.YES


def myriad_execution_side_for_route(market: MarketSpec, route: str) -> BinarySide | None:
    if not market.myriad_market_id:
        return None
    if route == "polymarket_myriad":
        return market.myriad_side
    if route in {"predict_myriad", "sx_myriad"}:
        return opposite_binary_side(market.predict_fun_side)
    raise ValueError(f"Unsupported route for Myriad side derivation: {route}")


def myriad_execution_token_for_route(market: MarketSpec, route: str) -> str | None:
    side = myriad_execution_side_for_route(market, route)
    if side is None or not market.myriad_market_id:
        return None
    return f"{market.myriad_market_id}:{side.value}"


def execution_route_for_market(market: MarketSpec) -> str:
    if market.venue_a_label == "Predict.fun" and market.venue_b_label == "SX Bet":
        return "predict_sx"
    if market.venue_a_label == "Predict.fun" and market.venue_b_label == "Myriad":
        return "predict_myriad"
    if market.venue_a_label == "SX Bet" and market.venue_b_label == "Myriad":
        return "sx_myriad"
    if market.venue_b_label == "Myriad":
        return "polymarket_myriad"
    if market.venue_b_label == "Predict.fun":
        return "polymarket_predict"
    if market.venue_b_label == "SX Bet":
        return "polymarket_sx"
    raise ValueError(
        f"Unsupported market route labels: venue_a={market.venue_a_label!r}, venue_b={market.venue_b_label!r}"
    )


def first_leg_token_for_route(market: MarketSpec, route: str) -> str | None:
    if route not in {
        "polymarket_myriad",
        "polymarket_predict",
        "predict_myriad",
        "predict_sx",
        "polymarket_sx",
        "sx_myriad",
    }:
        raise ValueError(f"Unsupported route for first-leg token derivation: {route}")
    return market.polymarket_token_id


def second_leg_token_for_route(market: MarketSpec, route: str) -> str | None:
    if route not in {
        "polymarket_myriad",
        "polymarket_predict",
        "predict_myriad",
        "predict_sx",
        "polymarket_sx",
        "sx_myriad",
    }:
        raise ValueError(f"Unsupported route for second-leg token derivation: {route}")
    return market.predict_fun_token_id


def first_leg_side_for_route(market: MarketSpec, route: str) -> BinarySide | None:
    if route not in {
        "polymarket_myriad",
        "polymarket_predict",
        "predict_myriad",
        "predict_sx",
        "polymarket_sx",
        "sx_myriad",
    }:
        raise ValueError(f"Unsupported route for first-leg side derivation: {route}")
    return market.polymarket_side


def second_leg_side_for_route(market: MarketSpec, route: str) -> BinarySide | None:
    if route not in {
        "polymarket_myriad",
        "polymarket_predict",
        "predict_myriad",
        "predict_sx",
        "polymarket_sx",
        "sx_myriad",
    }:
        raise ValueError(f"Unsupported route for second-leg side derivation: {route}")
    return market.predict_fun_side


def market_supports_execution_route(market: MarketSpec, route: str) -> bool:
    if route == "polymarket_myriad":
        return bool(market.polymarket_token_id and market.myriad_market_id)
    if route == "polymarket_predict":
        return bool(
            market.venue_a_label == "Polymarket"
            and market.venue_b_label == "Predict.fun"
            and market.polymarket_token_id
            and market.predict_fun_token_id
        )
    if route == "predict_myriad":
        return bool(market.venue_b_label == "Predict.fun" and market.predict_fun_token_id and market.myriad_market_id)
    if route == "predict_sx":
        return bool(
            market.venue_a_label == "Predict.fun"
            and market.venue_b_label == "SX Bet"
            and market.polymarket_token_id
            and market.predict_fun_token_id
        )
    if route == "polymarket_sx":
        return bool(
            market.venue_a_label == "Polymarket"
            and market.venue_b_label == "SX Bet"
            and market.polymarket_token_id
            and market.predict_fun_token_id
        )
    if route == "sx_myriad":
        return bool(market.venue_b_label == "SX Bet" and market.predict_fun_token_id and market.myriad_market_id)
    return False


def route_execution_sides_are_complementary(market: MarketSpec, route: str) -> bool:
    if route == "polymarket_myriad":
        first_side = market.polymarket_side
        second_side = myriad_execution_side_for_route(market, route)
    elif route in {"predict_myriad", "sx_myriad"}:
        # Myriad discovery records the outcome paired with Polymarket. A
        # cross-route hedge is valid only when that orientation is the same as
        # the Predict/SX outcome; execution then buys the opposite Myriad side.
        return bool(market.myriad_market_id) and market.myriad_side == market.predict_fun_side
    elif route in {"polymarket_predict", "predict_sx", "polymarket_sx"}:
        first_side = market.polymarket_side
        second_side = market.predict_fun_side
    else:
        return False
    return first_side is not None and second_side is not None and first_side != second_side


def _execution_status(
    value: str | ExecutionStatus,
    amount_filled: Decimal,
    amount_requested: Decimal,
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
    amount_requested: Decimal
    amount_filled: Decimal
    remaining_amount: Decimal
    avg_price: Decimal
    client_order_id: str | None = None
    venue_order_id: str | None = None
    submitted_at: datetime | None = None
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    cumulative_filled: Decimal | None = None

    def __post_init__(self) -> None:
        for name in ("amount_requested", "amount_filled", "remaining_amount", "avg_price"):
            object.__setattr__(self, name, _decimal(getattr(self, name)))
        if self.cumulative_filled is not None:
            object.__setattr__(self, "cumulative_filled", _decimal(self.cumulative_filled))

    @property
    def requested_amount(self) -> Decimal:
        return self.amount_requested

    @property
    def is_filled(self) -> bool:
        return self.remaining_amount <= Decimal("1e-18") and self.amount_filled > 0

    @property
    def has_fill(self) -> bool:
        return self.amount_filled > Decimal("1e-18")

    @classmethod
    def from_amounts(
        cls,
        order_id: str,
        amount_requested: Decimal | float,
        amount_filled: Decimal | float,
        status: str | ExecutionStatus,
        avg_price: Decimal | float = Decimal(0),
    ) -> ExecutionReport:
        requested = max(Decimal(0), _decimal(amount_requested))
        filled = min(max(Decimal(0), _decimal(amount_filled)), requested)
        normalized_status = _execution_status(status, filled, requested)
        return cls(
            order_id=order_id,
            status=normalized_status,
            amount_requested=requested,
            amount_filled=filled,
            remaining_amount=max(Decimal(0), requested - filled),
            avg_price=max(Decimal(0), _decimal(avg_price)),
            venue_order_id=order_id,
            cumulative_filled=filled,
        )


@dataclass(frozen=True)
class MarketConstraints:
    fee_rate_bps: int
    tick_size: Decimal
    lot_size: Decimal
    minimum_notional: Decimal
    fee_exponent: Decimal = Decimal("1")
    fetched_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(frozen=True)
class VenueFeeQuote:
    """Typed fee semantics for a single executable fill, never a global flat assumption."""

    venue: str
    fee_rate_bps: int = 0
    model: str = "notional_bps"
    source: str = "unverified"
    verified: bool = False
    fee_exponent: Decimal = Decimal("1")
    fee_rate_fraction: Decimal | None = None

    def fee_for_fill(self, contracts: Decimal, average_price: Decimal) -> Decimal:
        if self.fee_rate_bps < 0:
            raise ValueError("fee_rate_bps must be non-negative")
        if average_price < 0 or average_price > 1:
            raise ValueError("average_price must be between 0 and 1")
        rate = (
            Decimal(str(self.fee_rate_fraction))
            if self.fee_rate_fraction is not None
            else Decimal(self.fee_rate_bps) / Decimal(10_000)
        )
        if not rate.is_finite() or rate < 0 or rate >= 1:
            raise ValueError("fee rate must be finite and between 0 and 1")
        if self.model in {"polymarket_dynamic", "polymarket_taker"}:
            exponent = Decimal(str(self.fee_exponent))
            if not exponent.is_finite() or exponent < 0:
                raise ValueError("fee_exponent must be finite and non-negative")
            price_curve = average_price * (Decimal(1) - average_price)
            if exponent == 0:
                curve_factor = Decimal(1)
            elif price_curve == 0:
                curve_factor = Decimal(0)
            else:
                curve_factor = price_curve**exponent
            return contracts * rate * curve_factor
        if self.model == "notional_bps":
            return contracts * average_price * rate
        if self.model == "myriad_curve":
            effective_rate = rate * min(average_price, Decimal(1) - average_price) / Decimal("0.5")
            return contracts * average_price * effective_rate
        if self.model == "sx_payout_profit":
            # SX v3 charges a taker payout fee only on profit when the bet wins.
            return contracts * (Decimal(1) - average_price) * rate
        if self.model == "zero_fee":
            return Decimal(0)
        raise ValueError(f"unsupported fee model: {self.model}")


@dataclass(frozen=True)
class OrderPreview:
    """Executable order proof produced locally without submitting to the venue."""

    venue: str
    token_id: str
    side: BinarySide
    requested_contracts: Decimal
    limit_price: Decimal
    average_price: Decimal
    notional_usd: Decimal
    available_depth_usd: Decimal
    price_impact_pct: Decimal
    expected_fee_usd: Decimal
    fee_quote: VenueFeeQuote | None
    constraints: MarketConstraints | None
    signing_validated: bool
    payload_fingerprint: str | None = None
    blockers: tuple[str, ...] = ()
    guaranteed_contracts: Decimal | None = None
    maximum_notional_usd: Decimal | None = None
    maximum_fee_usd: Decimal | None = None

    @property
    def executable(self) -> bool:
        return not self.blockers and self.signing_validated and self.fee_quote is not None and self.fee_quote.verified


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
    match_strategy: str | None = None
    verified_at: datetime | None = None
    verified_by: str | None = None
    updated_at: datetime | None = None


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
    transient_failure: bool = False


@dataclass(frozen=True)
class SettlementRequest:
    position_key: str
    venue: str
    market_id: str
    condition_id: str
    collateral_token: str
    expected_contracts: Decimal
    index_sets: tuple[int, ...] = (1, 2)


@dataclass(frozen=True)
class RedemptionReport:
    status: RedemptionIntentStatus
    tx_hash: str | None = None
    error: str | None = None


@dataclass(frozen=True)
class RedemptionIntent:
    redemption_id: str
    position_key: str
    venue: str
    market_id: str
    condition_id: str
    collateral_token: str
    expected_contracts: Decimal
    status: RedemptionIntentStatus = RedemptionIntentStatus.PENDING
    tx_hash: str | None = None
    last_error: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))


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
    predict_fun_price_precision: int | None = None
    predict_fun_market_id: str | None = None
    predict_fun_url: str | None = None
    predict_fun_amm_pool: AmmPool | None = None
    myriad_market_id: str | None = None
    myriad_condition_id: str | None = None
    myriad_collateral_token: str | None = None
    myriad_url: str | None = None
    myriad_side: BinarySide = BinarySide.NO
    rules_fingerprint: str | None = None
    mapping_strategy: str | None = None
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

    @property
    def first_venue_label(self) -> str:
        return self.venue_a_label

    @property
    def second_venue_label(self) -> str:
        return self.venue_b_label

    @property
    def first_leg_token_id(self) -> str:
        return self.polymarket_token_id

    @property
    def second_leg_token_id(self) -> str:
        return self.predict_fun_token_id

    @property
    def first_leg_side(self) -> BinarySide:
        return self.polymarket_side

    @property
    def second_leg_side(self) -> BinarySide:
        return self.predict_fun_side

    @property
    def second_leg_market_id(self) -> str | None:
        return self.predict_fun_market_id

    @property
    def second_leg_url(self) -> str | None:
        return self.predict_fun_url

    @property
    def second_leg_neg_risk(self) -> bool | None:
        return self.predict_fun_neg_risk

    @property
    def second_leg_fee_rate_bps(self) -> int | None:
        return self.predict_fun_fee_rate_bps

    @property
    def second_leg_price_precision(self) -> int | None:
        return self.predict_fun_price_precision

    @property
    def second_leg_amm_pool(self) -> AmmPool | None:
        return self.predict_fun_amm_pool

    @property
    def second_leg_volume_usd(self) -> float | None:
        return self.predict_fun_volume_usd


@dataclass(frozen=True)
class PositionPlan:
    polymarket_contracts: Decimal
    polymarket_capital_usd: Decimal
    predict_fun_contracts: Decimal
    predict_fun_capital_usd: Decimal
    payout_contracts: Decimal
    total_cost_usd: Decimal
    polymarket_fee_usd: Decimal = Decimal(0)
    predict_fun_fee_usd: Decimal = Decimal(0)

    def __post_init__(self) -> None:
        for name in (
            "polymarket_contracts",
            "polymarket_capital_usd",
            "predict_fun_contracts",
            "predict_fun_capital_usd",
            "payout_contracts",
            "total_cost_usd",
            "polymarket_fee_usd",
            "predict_fun_fee_usd",
        ):
            object.__setattr__(self, name, _decimal(getattr(self, name)))

    @property
    def first_leg_contracts(self) -> Decimal:
        return self.polymarket_contracts

    @property
    def first_leg_capital_usd(self) -> Decimal:
        return self.polymarket_capital_usd

    @property
    def second_leg_contracts(self) -> Decimal:
        return self.predict_fun_contracts

    @property
    def second_leg_capital_usd(self) -> Decimal:
        return self.predict_fun_capital_usd

    @property
    def first_leg_fee_usd(self) -> Decimal:
        return self.polymarket_fee_usd

    @property
    def second_leg_fee_usd(self) -> Decimal:
        return self.predict_fun_fee_usd


@dataclass(frozen=True)
class SpreadMetrics:
    gross_spread: float
    net_spread: float
    expected_net_profit_usd: float
    polymarket_slippage: float
    predict_fun_slippage: float
    combined_cost_per_payout: float
    fixed_chain_cost_usd: float = 0.0

    @property
    def first_leg_slippage(self) -> float:
        return self.polymarket_slippage

    @property
    def second_leg_slippage(self) -> float:
        return self.predict_fun_slippage


@dataclass(frozen=True)
class ArbitrageSignal:
    market: MarketSpec
    plan: PositionPlan
    metrics: SpreadMetrics
    polymarket_price: float
    predict_fun_price: float
    raw_books: dict[str, Any] | None = None

    @property
    def first_leg_price(self) -> float:
        return self.polymarket_price

    @property
    def second_leg_price(self) -> float:
        return self.predict_fun_price


@dataclass(frozen=True)
class ResidualExitSnapshot:
    venue_order_id: str
    requested_contracts: Decimal
    closed_contracts: Decimal
    exit_proceeds_usd: Decimal
    residual_contracts: Decimal

    def __post_init__(self) -> None:
        normalized_order_id = self.venue_order_id.strip().lower()
        if not normalized_order_id:
            raise ValueError("residual exit snapshot requires a venue order id")
        object.__setattr__(self, "venue_order_id", normalized_order_id)
        for name in (
            "requested_contracts",
            "closed_contracts",
            "exit_proceeds_usd",
            "residual_contracts",
        ):
            object.__setattr__(self, name, _decimal(getattr(self, name)))
        values = (
            self.requested_contracts,
            self.closed_contracts,
            self.exit_proceeds_usd,
            self.residual_contracts,
        )
        if (
            any(not value.is_finite() or value < 0 for value in values)
            or self.requested_contracts <= 0
            or self.residual_contracts <= 0
            or (self.closed_contracts > 0 and self.exit_proceeds_usd <= 0)
            or self.closed_contracts + self.residual_contracts > self.requested_contracts + Decimal("1e-18")
        ):
            raise ValueError("residual exit snapshot contains invalid cumulative accounting")

    @property
    def average_exit_price(self) -> Decimal:
        if self.closed_contracts <= Decimal("1e-18"):
            return Decimal(0)
        return self.exit_proceeds_usd / self.closed_contracts


@dataclass(frozen=True)
class OpenPosition:
    market: MarketSpec
    polymarket_contracts: Decimal
    polymarket_entry_price: Decimal
    predict_fun_contracts: Decimal
    predict_fun_entry_price: Decimal
    opened_at: datetime
    polymarket_order_id: str
    predict_fun_order_id: str
    status: str = "open"
    polymarket_unwind_attempts: int = 0
    polymarket_closed: bool = False
    predict_fun_closed: bool = False
    polymarket_exit_price: Decimal | None = None
    predict_fun_exit_price: Decimal | None = None
    unmatched_first_contracts: Decimal = Decimal(0)
    unmatched_second_contracts: Decimal = Decimal(0)
    polymarket_closed_contracts: Decimal = Decimal(0)
    predict_fun_closed_contracts: Decimal = Decimal(0)
    polymarket_exit_proceeds_usd: Decimal = Decimal(0)
    predict_fun_exit_proceeds_usd: Decimal = Decimal(0)
    polymarket_residual_exposure_contracts: Decimal = Decimal(0)
    predict_fun_residual_exposure_contracts: Decimal = Decimal(0)
    polymarket_residual_exit_order_ids: tuple[str, ...] = ()
    predict_fun_residual_exit_order_ids: tuple[str, ...] = ()
    polymarket_residual_exit_snapshots: tuple[ResidualExitSnapshot, ...] = ()
    predict_fun_residual_exit_snapshots: tuple[ResidualExitSnapshot, ...] = ()

    def __post_init__(self) -> None:
        for name in (
            "polymarket_contracts",
            "polymarket_entry_price",
            "predict_fun_contracts",
            "predict_fun_entry_price",
            "unmatched_first_contracts",
            "unmatched_second_contracts",
            "polymarket_closed_contracts",
            "predict_fun_closed_contracts",
            "polymarket_exit_proceeds_usd",
            "predict_fun_exit_proceeds_usd",
            "polymarket_residual_exposure_contracts",
            "predict_fun_residual_exposure_contracts",
        ):
            object.__setattr__(self, name, _decimal(getattr(self, name)))
        for name in ("polymarket_exit_price", "predict_fun_exit_price"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _decimal(value))
        for name in ("polymarket_residual_exit_order_ids", "predict_fun_residual_exit_order_ids"):
            normalized = tuple(dict.fromkeys(str(value).lower() for value in getattr(self, name) if str(value)))
            object.__setattr__(self, name, normalized)
        for snapshots_name, order_ids_name in (
            ("polymarket_residual_exit_snapshots", "polymarket_residual_exit_order_ids"),
            ("predict_fun_residual_exit_snapshots", "predict_fun_residual_exit_order_ids"),
        ):
            snapshots = tuple(getattr(self, snapshots_name))
            if any(not isinstance(snapshot, ResidualExitSnapshot) for snapshot in snapshots):
                raise TypeError(f"{snapshots_name} must contain ResidualExitSnapshot values")
            snapshot_ids = tuple(snapshot.venue_order_id for snapshot in snapshots)
            if len(set(snapshot_ids)) != len(snapshot_ids):
                raise ValueError(f"{snapshots_name} contains duplicate venue order ids")
            object.__setattr__(self, snapshots_name, snapshots)
            order_ids = tuple(dict.fromkeys((*getattr(self, order_ids_name), *snapshot_ids)))
            object.__setattr__(self, order_ids_name, order_ids)

    @property
    def first_leg_contracts(self) -> Decimal:
        return self.polymarket_contracts

    @property
    def second_leg_contracts(self) -> Decimal:
        return self.predict_fun_contracts

    @property
    def first_leg_entry_price(self) -> Decimal:
        return self.polymarket_entry_price

    @property
    def second_leg_entry_price(self) -> Decimal:
        return self.predict_fun_entry_price

    @property
    def first_leg_order_id(self) -> str:
        return self.polymarket_order_id

    @property
    def second_leg_order_id(self) -> str:
        return self.predict_fun_order_id

    @property
    def first_leg_closed(self) -> bool:
        return self.polymarket_closed

    @property
    def second_leg_closed(self) -> bool:
        return self.predict_fun_closed

    @property
    def first_leg_exit_price(self) -> Decimal | None:
        return self.polymarket_exit_price

    @property
    def second_leg_exit_price(self) -> Decimal | None:
        return self.predict_fun_exit_price

    @property
    def first_leg_closed_contracts(self) -> Decimal:
        return self.polymarket_closed_contracts

    @property
    def second_leg_closed_contracts(self) -> Decimal:
        return self.predict_fun_closed_contracts

    @property
    def first_leg_exit_proceeds_usd(self) -> Decimal:
        return self.polymarket_exit_proceeds_usd

    @property
    def second_leg_exit_proceeds_usd(self) -> Decimal:
        return self.predict_fun_exit_proceeds_usd


def apply_residual_exit_snapshot(
    position: OpenPosition,
    *,
    venue: str,
    snapshot: ResidualExitSnapshot,
) -> OpenPosition:
    tolerance = Decimal("1e-18")

    def apply_to_leg(
        *,
        total_contracts: Decimal,
        closed_contracts: Decimal,
        exit_proceeds_usd: Decimal,
        residual_contracts: Decimal,
        order_ids: tuple[str, ...],
        snapshots: tuple[ResidualExitSnapshot, ...],
    ) -> tuple[Decimal, Decimal, Decimal, bool, Decimal | None, tuple[str, ...], tuple[ResidualExitSnapshot, ...]]:
        previous = next(
            (item for item in snapshots if item.venue_order_id == snapshot.venue_order_id),
            None,
        )
        if previous is None and snapshot.venue_order_id in order_ids:
            raise RuntimeError("residual exit order is missing its cumulative accounting snapshot")
        if previous is not None:
            if abs(previous.requested_contracts - snapshot.requested_contracts) > tolerance:
                raise RuntimeError("residual exit snapshot changed requested contracts")
            old_values = (
                previous.closed_contracts,
                previous.exit_proceeds_usd,
                previous.residual_contracts,
            )
            new_values = (
                snapshot.closed_contracts,
                snapshot.exit_proceeds_usd,
                snapshot.residual_contracts,
            )
            if all(new <= old + tolerance for old, new in zip(old_values, new_values, strict=True)):
                return (
                    closed_contracts,
                    exit_proceeds_usd,
                    residual_contracts,
                    closed_contracts >= total_contracts - tolerance,
                    exit_proceeds_usd / closed_contracts if closed_contracts > tolerance else None,
                    order_ids,
                    snapshots,
                )
            if any(new < old - tolerance for old, new in zip(old_values, new_values, strict=True)):
                raise RuntimeError("residual exit snapshot contains mixed cumulative regression")
            delta_closed = snapshot.closed_contracts - previous.closed_contracts
            delta_proceeds = snapshot.exit_proceeds_usd - previous.exit_proceeds_usd
            delta_residual = snapshot.residual_contracts - previous.residual_contracts
            if delta_closed > tolerance and delta_proceeds <= tolerance:
                raise RuntimeError("residual exit snapshot added closed contracts without proceeds")
            updated_snapshots = tuple(
                snapshot if item.venue_order_id == snapshot.venue_order_id else item for item in snapshots
            )
        else:
            delta_closed = snapshot.closed_contracts
            delta_proceeds = snapshot.exit_proceeds_usd
            delta_residual = snapshot.residual_contracts
            updated_snapshots = (*snapshots, snapshot)

        if closed_contracts + delta_closed > total_contracts + tolerance:
            raise RuntimeError("residual exit snapshot exceeds the remaining position contracts")
        updated_closed = min(total_contracts, closed_contracts + delta_closed)
        updated_proceeds = exit_proceeds_usd + delta_proceeds
        updated_residual = residual_contracts + delta_residual
        is_closed = updated_closed >= total_contracts - tolerance
        exit_price = updated_proceeds / updated_closed if updated_closed > tolerance else None
        updated_order_ids = tuple(dict.fromkeys((*order_ids, snapshot.venue_order_id)))
        return (
            updated_closed,
            updated_proceeds,
            updated_residual,
            is_closed,
            exit_price,
            updated_order_ids,
            updated_snapshots,
        )

    if position.market.first_venue_label == venue:
        closed, proceeds, residual, is_closed, exit_price, order_ids, snapshots = apply_to_leg(
            total_contracts=position.polymarket_contracts,
            closed_contracts=position.polymarket_closed_contracts,
            exit_proceeds_usd=position.polymarket_exit_proceeds_usd,
            residual_contracts=position.polymarket_residual_exposure_contracts,
            order_ids=position.polymarket_residual_exit_order_ids,
            snapshots=position.polymarket_residual_exit_snapshots,
        )
        return replace(
            position,
            status="manual_review",
            polymarket_closed=is_closed,
            polymarket_closed_contracts=closed,
            polymarket_exit_proceeds_usd=proceeds,
            polymarket_exit_price=exit_price,
            polymarket_residual_exposure_contracts=residual,
            polymarket_residual_exit_order_ids=order_ids,
            polymarket_residual_exit_snapshots=snapshots,
        )
    if position.market.second_venue_label == venue:
        closed, proceeds, residual, is_closed, exit_price, order_ids, snapshots = apply_to_leg(
            total_contracts=position.predict_fun_contracts,
            closed_contracts=position.predict_fun_closed_contracts,
            exit_proceeds_usd=position.predict_fun_exit_proceeds_usd,
            residual_contracts=position.predict_fun_residual_exposure_contracts,
            order_ids=position.predict_fun_residual_exit_order_ids,
            snapshots=position.predict_fun_residual_exit_snapshots,
        )
        return replace(
            position,
            status="manual_review",
            predict_fun_closed=is_closed,
            predict_fun_closed_contracts=closed,
            predict_fun_exit_proceeds_usd=proceeds,
            predict_fun_exit_price=exit_price,
            predict_fun_residual_exposure_contracts=residual,
            predict_fun_residual_exit_order_ids=order_ids,
            predict_fun_residual_exit_snapshots=snapshots,
        )
    raise RuntimeError(f"venue {venue} is not part of position {position.market.symbol}")


@dataclass(frozen=True)
class ExitSignal:
    position: OpenPosition
    polymarket_exit_price: Decimal
    predict_fun_exit_price: Decimal
    profit_pct: float
    profit_usd: Decimal
    exit_spread: float | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "polymarket_exit_price", _decimal(self.polymarket_exit_price))
        object.__setattr__(self, "predict_fun_exit_price", _decimal(self.predict_fun_exit_price))
        object.__setattr__(self, "profit_usd", _decimal(self.profit_usd))

    @property
    def first_leg_exit_price(self) -> Decimal:
        return self.polymarket_exit_price

    @property
    def second_leg_exit_price(self) -> Decimal:
        return self.predict_fun_exit_price


def position_key(market: MarketSpec) -> str:
    fingerprint = market.rules_fingerprint or f"{market.symbol}:{market.target_label}"
    return (
        f"{fingerprint}:"
        f"{market.first_venue_label}:{market.first_leg_token_id}:{market.first_leg_side.value}:"
        f"{market.second_venue_label}:{market.second_leg_token_id}:{market.second_leg_side.value}"
    )


def _decimal(value: Decimal | float | int | str) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))
