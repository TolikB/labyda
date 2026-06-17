from __future__ import annotations

from collections.abc import Iterable

from .models import OrderBook, OrderBookLevel, PositionPlan, SpreadMetrics


def weighted_average_fill(levels: Iterable[OrderBookLevel], target_notional_usd: float) -> tuple[float, float, float]:
    """Return average price, base size, and spent notional for a target notional."""
    if target_notional_usd <= 0:
        raise ValueError("target_notional_usd must be positive")

    remaining = target_notional_usd
    spent = 0.0
    size = 0.0

    for level in levels:
        level_notional = level.price * level.size
        take_notional = min(remaining, level_notional)
        take_size = take_notional / level.price
        spent += take_notional
        size += take_size
        remaining -= take_notional
        if remaining <= 1e-9:
            break

    if spent <= 0 or size <= 0:
        raise ValueError("insufficient book liquidity")

    return spent / size, size, spent


def slippage_from_best(avg_price: float, best_price: float) -> float:
    if best_price <= 0:
        raise ValueError("best_price must be positive")
    return max(0.0, (avg_price - best_price) / best_price)


def build_position_plan(
    polymarket_book: OrderBook,
    cefi_book: OrderBook,
    max_order_size_usd: float,
    leverage: float,
) -> PositionPlan:
    poly_avg_price, poly_contracts, poly_spent = weighted_average_fill(
        polymarket_book.asks, max_order_size_usd
    )
    del poly_avg_price

    cefi_price = cefi_book.best_bid.price if cefi_book.bids else cefi_book.best_ask.price
    cefi_quantity = max_order_size_usd / cefi_price

    return PositionPlan(
        polymarket_contracts=poly_contracts,
        polymarket_capital_usd=min(poly_spent, max_order_size_usd),
        cefi_quantity=cefi_quantity,
        cefi_notional_usd=max_order_size_usd,
        cefi_margin_usd=max_order_size_usd / leverage,
    )


def calculate_spread_metrics(
    polymarket_book: OrderBook,
    cefi_book: OrderBook,
    max_order_size_usd: float,
    cefi_taker_fee: float,
    leverage: float,
) -> SpreadMetrics:
    poly_avg_price, _, _ = weighted_average_fill(polymarket_book.asks, max_order_size_usd)
    p_poly = polymarket_book.best_ask.price
    poly_slippage = slippage_from_best(poly_avg_price, p_poly)

    cefi_avg_price, _, _ = weighted_average_fill(cefi_book.asks, max_order_size_usd)
    cefi_slippage = slippage_from_best(cefi_avg_price, cefi_book.best_ask.price)

    gross_spread = (1.0 - p_poly) / p_poly
    net_spread = ((1.0 - p_poly - poly_slippage) / p_poly) - (
        (cefi_taker_fee + cefi_slippage) / leverage
    )
    expected_net_profit = max_order_size_usd * net_spread

    return SpreadMetrics(
        gross_spread=gross_spread,
        net_spread=net_spread,
        expected_net_profit_usd=expected_net_profit,
        polymarket_slippage=poly_slippage,
        cefi_slippage=cefi_slippage,
    )


def calculate_polymarket_profit(entry_price: float, exit_price: float, contracts: float) -> tuple[float, float]:
    if entry_price <= 0:
        raise ValueError("entry_price must be positive")
    if contracts <= 0:
        raise ValueError("contracts must be positive")

    profit_pct = (exit_price - entry_price) / entry_price
    profit_usd = (exit_price - entry_price) * contracts
    return profit_pct, profit_usd
