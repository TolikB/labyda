import unittest

from arbitrage_engine.models import OrderBook, OrderBookLevel
from arbitrage_engine.quant import build_position_plan, calculate_spread_metrics, weighted_average_fill


class QuantTests(unittest.TestCase):
    def test_weighted_average_fill_walks_book(self) -> None:
        levels = [OrderBookLevel(0.40, 100), OrderBookLevel(0.50, 120)]

        avg_price, size, spent = weighted_average_fill(levels, 100)

        self.assertAlmostEqual(spent, 100)
        self.assertAlmostEqual(size, 220)
        self.assertAlmostEqual(avg_price, 100 / 220)

    def test_spread_passes_threshold_for_cheap_polymarket_ask(self) -> None:
        poly = OrderBook(bids=[OrderBookLevel(0.41, 100)], asks=[OrderBookLevel(0.42, 500)])
        cefi = OrderBook(bids=[OrderBookLevel(75100, 1)], asks=[OrderBookLevel(75120, 1)])

        metrics = calculate_spread_metrics(poly, cefi, 100, cefi_taker_fee=0.0005, leverage=10)

        self.assertGreater(metrics.net_spread, 0.05)
        self.assertAlmostEqual(metrics.gross_spread, (1 - 0.42) / 0.42)

    def test_position_plan_caps_each_leg_at_max_order_size(self) -> None:
        poly = OrderBook(bids=[OrderBookLevel(0.41, 100)], asks=[OrderBookLevel(0.42, 1000)])
        cefi = OrderBook(bids=[OrderBookLevel(75000, 1)], asks=[OrderBookLevel(75100, 1)])

        plan = build_position_plan(poly, cefi, max_order_size_usd=100, leverage=10)

        self.assertLessEqual(plan.polymarket_capital_usd, 100)
        self.assertEqual(plan.cefi_notional_usd, 100)
        self.assertEqual(plan.cefi_margin_usd, 10)


if __name__ == "__main__":
    unittest.main()

