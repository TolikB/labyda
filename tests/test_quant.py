import unittest

from arbitrage_engine.models import HedgeSide, OrderBook, OrderBookLevel
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

        metrics = calculate_spread_metrics(
            poly,
            cefi,
            100,
            cefi_taker_fee=0.0005,
            leverage=10,
            cefi_hedge_side=HedgeSide.SHORT,
        )

        self.assertGreater(metrics.net_spread, 0.05)
        self.assertAlmostEqual(metrics.gross_spread, (1 - 0.42) / 0.42)

    def test_position_plan_caps_each_leg_at_max_order_size(self) -> None:
        poly = OrderBook(bids=[OrderBookLevel(0.41, 100)], asks=[OrderBookLevel(0.42, 1000)])
        cefi = OrderBook(bids=[OrderBookLevel(75000, 1)], asks=[OrderBookLevel(75100, 1)])

        plan = build_position_plan(
            poly,
            cefi,
            max_order_size_usd=100,
            leverage=10,
            cefi_hedge_side=HedgeSide.SHORT,
        )

        self.assertLessEqual(plan.polymarket_capital_usd, 100)
        self.assertEqual(plan.cefi_notional_usd, 100)
        self.assertEqual(plan.cefi_margin_usd, 10)

    def test_cefi_frictions_are_not_divided_by_leverage(self) -> None:
        poly = OrderBook(bids=[OrderBookLevel(0.49, 1000)], asks=[OrderBookLevel(0.50, 1000)])
        cefi = OrderBook(bids=[OrderBookLevel(100, 1000)], asks=[OrderBookLevel(101, 1000)])

        metrics = calculate_spread_metrics(
            poly,
            cefi,
            100,
            cefi_taker_fee=0.02,
            leverage=10,
            cefi_hedge_side=HedgeSide.SHORT,
        )

        self.assertAlmostEqual(metrics.net_spread, 0.98)

    def test_short_hedge_slippage_uses_bids(self) -> None:
        poly = OrderBook(bids=[OrderBookLevel(0.49, 1000)], asks=[OrderBookLevel(0.50, 1000)])
        cefi = OrderBook(
            bids=[OrderBookLevel(100, 0.5), OrderBookLevel(90, 1.0)],
            asks=[OrderBookLevel(1000, 1000)],
        )

        metrics = calculate_spread_metrics(
            poly,
            cefi,
            100,
            cefi_taker_fee=0.0,
            leverage=10,
            cefi_hedge_side=HedgeSide.SHORT,
        )

        self.assertGreater(metrics.cefi_slippage, 0.0)
        self.assertAlmostEqual(metrics.cefi_slippage, (100 - (100 / (0.5 + (50 / 90)))) / 100)


if __name__ == "__main__":
    unittest.main()
