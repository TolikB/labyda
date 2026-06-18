from __future__ import annotations

from collections.abc import Iterable
from collections.abc import Callable
from dataclasses import dataclass

from .models import AmmPool, BinarySide, OrderBook, OrderBookLevel, PositionPlan, SpreadMetrics

MAX_PRICE_IMPACT = 0.015


@dataclass(frozen=True)
class FillQuote:
    avg_price: float
    contracts: float
    spent_usd: float
    slippage_pct: float


def weighted_average_fill(levels: Iterable[OrderBookLevel], target_notional_usd: float) -> tuple[float, float, float]:
    if target_notional_usd <= 0:
        raise ValueError("target_notional_usd must be positive")

    remaining = target_notional_usd
    spent = 0.0
    size = 0.0

    for level in levels:
        if level.price <= 0 or level.size <= 0:
            continue
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
    if spent + 1e-9 < target_notional_usd:
        raise ValueError("insufficient book liquidity for target notional")

    return spent / size, size, spent


def orderbook_buy_quote(book: OrderBook, target_notional_usd: float) -> FillQuote:
    best_price = book.best_ask.price
    avg_price, contracts, spent = weighted_average_fill(book.asks, target_notional_usd)
    slippage = max(0.0, (avg_price - best_price) / best_price)
    return FillQuote(avg_price=avg_price, contracts=contracts, spent_usd=spent, slippage_pct=slippage)


def amm_buy_quote(pool: AmmPool, side: BinarySide, target_notional_usd: float) -> FillQuote:
    if target_notional_usd <= 0:
        raise ValueError("target_notional_usd must be positive")
    x_reserve = pool.no_reserve if side is BinarySide.YES else pool.yes_reserve
    y_reserve = pool.yes_reserve if side is BinarySide.YES else pool.no_reserve
    if x_reserve <= 0 or y_reserve <= 0:
        raise ValueError("AMM reserves must be positive")

    effective_in = target_notional_usd * (1.0 - pool.fee_pct)
    contracts_out = (y_reserve * effective_in) / (x_reserve + effective_in)
    if contracts_out <= 0:
        raise ValueError("AMM quote returned zero contracts")

    spot_price = x_reserve / (x_reserve + y_reserve)
    avg_price = target_notional_usd / contracts_out
    slippage = max(0.0, (avg_price - spot_price) / spot_price)
    return FillQuote(avg_price=avg_price, contracts=contracts_out, spent_usd=target_notional_usd, slippage_pct=slippage)


def quote_with_liquidity_guard(
    quote_fn: Callable[[float], FillQuote],
    max_order_size_usd: float,
    max_slippage_pct: float,
) -> FillQuote:
    quote = quote_fn(max_order_size_usd)
    if quote.slippage_pct <= max_slippage_pct:
        return quote
    raise ValueError("price impact exceeds slippage cap")


def build_position_plan(
    polymarket_book: OrderBook | None,
    predict_fun_book: OrderBook | None,
    max_order_size_usd: float,
    max_slippage_pct: float,
    polymarket_amm_pool: AmmPool | None = None,
    polymarket_side: BinarySide = BinarySide.YES,
    predict_fun_amm_pool: AmmPool | None = None,
    predict_fun_side: BinarySide = BinarySide.NO,
) -> PositionPlan:
    max_slippage_pct = min(max_slippage_pct, MAX_PRICE_IMPACT)
    if polymarket_amm_pool is not None:
        poly_quote_fn = lambda notional: amm_buy_quote(polymarket_amm_pool, polymarket_side, notional)
    elif polymarket_book is not None:
        poly_quote_fn = lambda notional: orderbook_buy_quote(polymarket_book, notional)
    else:
        raise ValueError("polymarket_book or polymarket_amm_pool is required")

    poly_quote = quote_with_liquidity_guard(
        poly_quote_fn,
        max_order_size_usd,
        max_slippage_pct,
    )
    if predict_fun_amm_pool is not None:
        predict_quote_fn = lambda notional: amm_buy_quote(predict_fun_amm_pool, predict_fun_side, notional)
    elif predict_fun_book is not None:
        predict_quote_fn = lambda notional: orderbook_buy_quote(predict_fun_book, notional)
    else:
        raise ValueError("predict_fun_book or predict_fun_amm_pool is required")

    predict_quote = quote_with_liquidity_guard(
        predict_quote_fn,
        max_order_size_usd,
        max_slippage_pct,
    )

    payout_contracts = min(poly_quote.contracts, predict_quote.contracts)
    if payout_contracts <= 0:
        raise ValueError("zero binary payout contracts")

    poly_capital = payout_contracts * poly_quote.avg_price
    predict_capital = payout_contracts * predict_quote.avg_price
    return PositionPlan(
        polymarket_contracts=payout_contracts,
        polymarket_capital_usd=poly_capital,
        predict_fun_contracts=payout_contracts,
        predict_fun_capital_usd=predict_capital,
        payout_contracts=payout_contracts,
        total_cost_usd=poly_capital + predict_capital,
    )


def calculate_spread_metrics(
    polymarket_book: OrderBook | None,
    predict_fun_book: OrderBook | None,
    max_order_size_usd: float,
    min_net_spread: float,
    max_slippage_pct: float,
    polymarket_amm_pool: AmmPool | None = None,
    polymarket_side: BinarySide = BinarySide.YES,
    predict_fun_amm_pool: AmmPool | None = None,
    predict_fun_side: BinarySide = BinarySide.NO,
) -> SpreadMetrics:
    plan = build_position_plan(
        polymarket_book=polymarket_book,
        predict_fun_book=predict_fun_book,
        max_order_size_usd=max_order_size_usd,
        max_slippage_pct=max_slippage_pct,
        polymarket_amm_pool=polymarket_amm_pool,
        polymarket_side=polymarket_side,
        predict_fun_amm_pool=predict_fun_amm_pool,
        predict_fun_side=predict_fun_side,
    )
    poly_avg = plan.polymarket_capital_usd / plan.payout_contracts
    predict_avg = plan.predict_fun_capital_usd / plan.payout_contracts
    combined_cost = poly_avg + predict_avg
    net_spread = 1.0 - combined_cost

    expected_net_profit = plan.payout_contracts * net_spread
    first_best = _best_leg_price(polymarket_book, polymarket_amm_pool, polymarket_side)
    second_best = _best_leg_price(predict_fun_book, predict_fun_amm_pool, predict_fun_side)
    gross_spread = 1.0 - (first_best + second_best)
    return SpreadMetrics(
        gross_spread=gross_spread,
        net_spread=net_spread,
        expected_net_profit_usd=expected_net_profit,
        polymarket_slippage=max(0.0, (poly_avg - first_best) / first_best),
        predict_fun_slippage=_predict_slippage(predict_fun_book, predict_fun_amm_pool, predict_fun_side, predict_avg),
        combined_cost_per_payout=combined_cost,
    )


def is_binary_signal_allowed(metrics: SpreadMetrics, min_net_spread: float) -> bool:
    epsilon = 1e-9
    return (
        metrics.combined_cost_per_payout < 1.0 - min_net_spread - epsilon
        and metrics.net_spread > min_net_spread + epsilon
    )


def calculate_binary_position_profit(
    entry_total_cost: float,
    exit_total_value: float,
    payout_contracts: float,
) -> tuple[float, float]:
    if entry_total_cost <= 0 or payout_contracts <= 0:
        raise ValueError("entry_total_cost and payout_contracts must be positive")
    profit_usd = (exit_total_value - entry_total_cost) * payout_contracts
    profit_pct = (exit_total_value - entry_total_cost) / entry_total_cost
    return profit_pct, profit_usd


def _best_predict_price(book: OrderBook | None, pool: AmmPool | None, side: BinarySide) -> float:
    return _best_leg_price(book, pool, side)


def _best_leg_price(book: OrderBook | None, pool: AmmPool | None, side: BinarySide) -> float:
    if book is not None:
        return book.best_ask.price
    if pool is None:
        raise ValueError("order book or AMM pool is required")
    x_reserve = pool.no_reserve if side is BinarySide.YES else pool.yes_reserve
    y_reserve = pool.yes_reserve if side is BinarySide.YES else pool.no_reserve
    return x_reserve / (x_reserve + y_reserve)


def _predict_slippage(book: OrderBook | None, pool: AmmPool | None, side: BinarySide, avg_price: float) -> float:
    best = _best_predict_price(book, pool, side)
    return max(0.0, (avg_price - best) / best)
