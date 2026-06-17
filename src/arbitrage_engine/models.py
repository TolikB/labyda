from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Sequence


class PolymarketSide(str, Enum):
    YES = "YES"
    NO = "NO"


class HedgeSide(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"


def opposite_hedge_side(side: HedgeSide) -> HedgeSide:
    return HedgeSide.SHORT if side is HedgeSide.LONG else HedgeSide.LONG


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
class MarketSpec:
    symbol: str
    target_label: str
    polymarket_token_id: str
    polymarket_side: PolymarketSide
    cefi_symbol: str
    cefi_hedge_side: HedgeSide
    expires_at: datetime | None = None


@dataclass(frozen=True)
class PositionPlan:
    polymarket_contracts: float
    polymarket_capital_usd: float
    cefi_quantity: float
    cefi_notional_usd: float
    cefi_margin_usd: float


@dataclass(frozen=True)
class SpreadMetrics:
    gross_spread: float
    net_spread: float
    expected_net_profit_usd: float
    polymarket_slippage: float
    cefi_slippage: float


@dataclass(frozen=True)
class ArbitrageSignal:
    market: MarketSpec
    plan: PositionPlan
    metrics: SpreadMetrics
    polymarket_price: float
    cefi_price: float


@dataclass(frozen=True)
class OpenPosition:
    market: MarketSpec
    polymarket_contracts: float
    polymarket_entry_price: float
    cefi_quantity: float
    cefi_entry_side: HedgeSide
    opened_at: datetime
    polymarket_order_id: str
    cefi_order_id: str


@dataclass(frozen=True)
class ExitSignal:
    position: OpenPosition
    polymarket_exit_price: float
    profit_pct: float
    profit_usd: float
