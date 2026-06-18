import unittest

from arbitrage_engine.models import AmmPool, BinarySide, OrderBook, OrderBookLevel
from arbitrage_engine.quant import (
    amm_buy_quote,
    build_position_plan,
    calculate_spread_metrics,
    is_binary_signal_allowed,
    weighted_average_fill,
)


class QuantTests(unittest.TestCase):
    def test_weighted_average_fill_walks_book(self) -> None:
        levels = [OrderBookLevel(0.40, 100), OrderBookLevel(0.50, 120)]

        avg_price, size, spent = weighted_average_fill(levels, 100)

        self.assertAlmostEqual(spent, 100)
        self.assertAlmostEqual(size, 220)
        self.assertAlmostEqual(avg_price, 100 / 220)

    def test_binary_signal_requires_combined_cost_below_ten_percent_threshold(self) -> None:
        poly = OrderBook(bids=[OrderBookLevel(0.41, 1000)], asks=[OrderBookLevel(0.42, 1000)])
        predict = OrderBook(bids=[OrderBookLevel(0.46, 1000)], asks=[OrderBookLevel(0.47, 1000)])

        metrics = calculate_spread_metrics(poly, predict, 100, 0.10, 0.015)

        self.assertAlmostEqual(metrics.combined_cost_per_payout, 0.89)
        self.assertTrue(is_binary_signal_allowed(metrics, 0.10))

    def test_binary_signal_rejects_cost_at_090_or_above(self) -> None:
        poly = OrderBook(bids=[OrderBookLevel(0.42, 1000)], asks=[OrderBookLevel(0.43, 1000)])
        predict = OrderBook(bids=[OrderBookLevel(0.46, 1000)], asks=[OrderBookLevel(0.47, 1000)])

        metrics = calculate_spread_metrics(poly, predict, 100, 0.10, 0.015)

        self.assertAlmostEqual(metrics.combined_cost_per_payout, 0.90)
        self.assertFalse(is_binary_signal_allowed(metrics, 0.10))

    def test_amm_quote_accounts_for_price_impact(self) -> None:
        pool = AmmPool(yes_reserve=1000, no_reserve=1000)

        small = amm_buy_quote(pool, BinarySide.YES, 10)
        large = amm_buy_quote(pool, BinarySide.YES, 100)

        self.assertGreater(large.slippage_pct, small.slippage_pct)

    def test_position_plan_blocks_thin_book_instead_of_shrinking_size(self) -> None:
        poly = OrderBook(
            bids=[OrderBookLevel(0.40, 1000)],
            asks=[OrderBookLevel(0.40, 10), OrderBookLevel(0.80, 1000)],
        )
        predict = OrderBook(bids=[OrderBookLevel(0.45, 1000)], asks=[OrderBookLevel(0.45, 1000)])

        with self.assertRaisesRegex(ValueError, "price impact"):
            build_position_plan(poly, predict, max_order_size_usd=100, max_slippage_pct=0.015)

    def test_signal_blocks_when_best_price_spread_disappears_after_book_walk(self) -> None:
        poly = OrderBook(
            bids=[OrderBookLevel(0.39, 1000)],
            asks=[OrderBookLevel(0.40, 25), OrderBookLevel(0.75, 1000)],
        )
        predict = OrderBook(bids=[OrderBookLevel(0.49, 1000)], asks=[OrderBookLevel(0.50, 1000)])

        with self.assertRaisesRegex(ValueError, "price impact"):
            calculate_spread_metrics(poly, predict, 100, 0.10, 0.015)


if __name__ == "__main__":
    unittest.main()
