from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Sequence


class BinarySide(str, Enum):
    YES = "YES"
    NO = "NO"


PolymarketSide = BinarySide


def opposite_binary_side(side: BinarySide) -> BinarySide:
    return BinarySide.NO if side is BinarySide.YES else BinarySide.YES


@dataclass(frozen=True)
class OrderBookLevel:
    price: float
    size: float


@dataclass(frozen=True)
class OrderBook:
    bids: Sequence[OrderBookLevel]
    asks: Sequence[OrderBookLevel]

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
    tick_size: str | None = None
    neg_risk: bool | None = None
    predict_fun_market_id: str | None = None
    predict_fun_amm_pool: AmmPool | None = None
    myriad_market_id: str | None = None
    myriad_side: BinarySide = BinarySide.NO
    rules_fingerprint: str | None = None


@dataclass(frozen=True)
class PositionPlan:
    polymarket_contracts: float
    polymarket_capital_usd: float
    predict_fun_contracts: float
    predict_fun_capital_usd: float
    payout_contracts: float
    total_cost_usd: float


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
