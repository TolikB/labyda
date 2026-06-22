from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from decimal import Decimal

from .models import AmmPool, BinarySide, OrderBook, OrderBookLevel, PositionPlan, SpreadMetrics


def _d(value: float | int | Decimal) -> Decimal:
    return Decimal(str(value))


@dataclass(frozen=True)
class FillQuote:
    avg_price: float
    contracts: float
    spent_usd: float
    slippage_pct: float


def weighted_average_fill(levels: Iterable[OrderBookLevel], target_notional_usd: float) -> tuple[float, float, float]:
    if target_notional_usd <= 0:
        raise ValueError("target_notional_usd must be positive")

    target = _d(target_notional_usd)
    remaining = target
    spent = Decimal(0)
    size = Decimal(0)

    for level in levels:
        if level.price <= 0 or level.size <= 0:
            continue
        price = _d(level.price)
        level_size = _d(level.size)
        level_notional = price * level_size
        take_notional = min(remaining, level_notional)
        take_size = take_notional / price
        spent += take_notional
        size += take_size
        remaining -= take_notional
        if remaining <= Decimal("1e-12"):
            break

    if spent <= 0 or size <= 0:
        raise ValueError("insufficient book liquidity")
    if spent + Decimal("1e-12") < target:
        raise ValueError("insufficient book liquidity for target notional")

    return float(spent / size), float(size), float(spent)


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

    x = _d(x_reserve)
    y = _d(y_reserve)
    target = _d(target_notional_usd)
    effective_in = target * (Decimal(1) - _d(pool.fee_pct))
    contracts_out = (y * effective_in) / (x + effective_in)
    if contracts_out <= 0:
        raise ValueError("AMM quote returned zero contracts")

    spot_price = x / (x + y)
    avg_price = target / contracts_out
    slippage = max(Decimal(0), (avg_price - spot_price) / spot_price)
    return FillQuote(
        avg_price=float(avg_price),
        contracts=float(contracts_out),
        spent_usd=target_notional_usd,
        slippage_pct=float(slippage),
    )


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
    *,
    max_price_impact: float,
    polymarket_amm_pool: AmmPool | None = None,
    polymarket_side: BinarySide = BinarySide.YES,
    predict_fun_amm_pool: AmmPool | None = None,
    predict_fun_side: BinarySide = BinarySide.NO,
    polymarket_fee_pct: float = 0.0,
    predict_fun_fee_pct: float = 0.0,
) -> PositionPlan:
    if not 0 <= polymarket_fee_pct < 1 or not 0 <= predict_fun_fee_pct < 1:
        raise ValueError("trading fee percentages must be between 0 and 1")
    if not 0 < max_price_impact <= 1:
        raise ValueError("max_price_impact must be between 0 and 1")
    max_slippage_pct = min(max_slippage_pct, max_price_impact)
    if polymarket_amm_pool is not None:

        def poly_quote_fn(notional: float) -> FillQuote:
            return amm_buy_quote(polymarket_amm_pool, polymarket_side, notional)
    elif polymarket_book is not None:

        def poly_quote_fn(notional: float) -> FillQuote:
            return orderbook_buy_quote(polymarket_book, notional)
    else:
        raise ValueError("polymarket_book or polymarket_amm_pool is required")

    poly_quote = quote_with_liquidity_guard(
        poly_quote_fn,
        max_order_size_usd,
        max_slippage_pct,
    )
    if predict_fun_amm_pool is not None:

        def predict_quote_fn(notional: float) -> FillQuote:
            return amm_buy_quote(predict_fun_amm_pool, predict_fun_side, notional)
    elif predict_fun_book is not None:

        def predict_quote_fn(notional: float) -> FillQuote:
            return orderbook_buy_quote(predict_fun_book, notional)
    else:
        raise ValueError("predict_fun_book or predict_fun_amm_pool is required")

    predict_quote = quote_with_liquidity_guard(
        predict_quote_fn,
        max_order_size_usd,
        max_slippage_pct,
    )

    payout_contracts = _d(min(poly_quote.contracts, predict_quote.contracts))
    if payout_contracts <= 0:
        raise ValueError("zero binary payout contracts")

    poly_capital = payout_contracts * _d(poly_quote.avg_price)
    predict_capital = payout_contracts * _d(predict_quote.avg_price)
    poly_fee = poly_capital * _d(polymarket_fee_pct)
    predict_fee = predict_capital * _d(predict_fun_fee_pct)
    return PositionPlan(
        polymarket_contracts=payout_contracts,
        polymarket_capital_usd=poly_capital,
        predict_fun_contracts=payout_contracts,
        predict_fun_capital_usd=predict_capital,
        payout_contracts=payout_contracts,
        total_cost_usd=poly_capital + predict_capital + poly_fee + predict_fee,
        polymarket_fee_usd=poly_fee,
        predict_fun_fee_usd=predict_fee,
    )


def calculate_spread_metrics(
    polymarket_book: OrderBook | None,
    predict_fun_book: OrderBook | None,
    max_order_size_usd: float,
    min_net_spread: float,
    max_slippage_pct: float,
    *,
    max_price_impact: float,
    polymarket_amm_pool: AmmPool | None = None,
    polymarket_side: BinarySide = BinarySide.YES,
    predict_fun_amm_pool: AmmPool | None = None,
    predict_fun_side: BinarySide = BinarySide.NO,
    polymarket_fee_pct: float = 0.0,
    predict_fun_fee_pct: float = 0.0,
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
        polymarket_fee_pct=polymarket_fee_pct,
        predict_fun_fee_pct=predict_fun_fee_pct,
        max_price_impact=max_price_impact,
    )
    poly_avg = (plan.polymarket_capital_usd + plan.polymarket_fee_usd) / plan.payout_contracts
    predict_avg = (plan.predict_fun_capital_usd + plan.predict_fun_fee_usd) / plan.payout_contracts
    combined_cost = float(poly_avg + predict_avg)
    net_spread = float(Decimal(1) - _d(combined_cost))

    expected_net_profit = float(plan.payout_contracts * _d(net_spread))
    first_best = _best_leg_price(polymarket_book, polymarket_amm_pool, polymarket_side)
    second_best = _best_leg_price(predict_fun_book, predict_fun_amm_pool, predict_fun_side)
    gross_spread = 1.0 - (first_best + second_best)
    return SpreadMetrics(
        gross_spread=gross_spread,
        net_spread=net_spread,
        expected_net_profit_usd=expected_net_profit,
        polymarket_slippage=float(max(Decimal(0), (poly_avg - _d(first_best)) / _d(first_best))),
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
    entry_total_cost: float | Decimal,
    exit_total_value: float | Decimal,
    payout_contracts: float | Decimal,
) -> tuple[float, Decimal]:
    if entry_total_cost <= 0 or payout_contracts <= 0:
        raise ValueError("entry_total_cost and payout_contracts must be positive")
    entry = _d(entry_total_cost)
    difference = _d(exit_total_value) - entry
    profit_usd = difference * _d(payout_contracts)
    profit_pct = difference / entry
    return float(profit_pct), profit_usd


def calculate_realized_position_profit(entry_cost_usd: float, exit_proceeds_usd: float) -> tuple[float, float]:
    profit_pct, profit_usd = calculate_realized_position_profit_decimal(entry_cost_usd, exit_proceeds_usd)
    return float(profit_pct), float(profit_usd)


def calculate_realized_position_profit_decimal(
    entry_cost_usd: float | Decimal,
    exit_proceeds_usd: float | Decimal,
) -> tuple[Decimal, Decimal]:
    if entry_cost_usd <= 0:
        raise ValueError("entry_cost_usd must be positive")
    entry = _d(entry_cost_usd)
    profit = _d(exit_proceeds_usd) - entry
    return profit / entry, profit


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


def _predict_slippage(
    book: OrderBook | None,
    pool: AmmPool | None,
    side: BinarySide,
    avg_price: float | Decimal,
) -> float:
    best = _best_predict_price(book, pool, side)
    return float(max(Decimal(0), (_d(avg_price) - _d(best)) / _d(best)))
