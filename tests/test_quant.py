import unittest
from decimal import Decimal

from arbitrage_engine.models import AmmPool, BinarySide, OrderBook, OrderBookLevel, VenueFeeQuote
from arbitrage_engine.quant import (
    amm_buy_quote,
    build_position_plan,
    calculate_spread_metrics,
    is_binary_signal_allowed,
    top_of_book_ask_depth_usd,
    weighted_average_fill,
)
from arbitrage_engine.utils.math import quantize_up


class QuantTests(unittest.TestCase):
    def test_sell_price_rounding_never_weakens_minimum_limit(self) -> None:
        self.assertEqual(str(quantize_up(0.405, 0.01)), "0.41")

    def test_weighted_average_fill_walks_book(self) -> None:
        levels = [OrderBookLevel(0.40, 100), OrderBookLevel(0.50, 120)]

        avg_price, size, spent = weighted_average_fill(levels, 100)

        self.assertAlmostEqual(spent, 100)
        self.assertAlmostEqual(size, 220)
        self.assertAlmostEqual(avg_price, 100 / 220)

    def test_binary_signal_requires_combined_cost_below_ten_percent_threshold(self) -> None:
        poly = OrderBook(bids=[OrderBookLevel(0.41, 1000)], asks=[OrderBookLevel(0.42, 1000)])
        predict = OrderBook(bids=[OrderBookLevel(0.46, 1000)], asks=[OrderBookLevel(0.47, 1000)])

        metrics = calculate_spread_metrics(poly, predict, 100, 0.10, 0.015, max_price_impact=0.015)

        self.assertAlmostEqual(metrics.combined_cost_per_payout, 0.89)
        self.assertTrue(is_binary_signal_allowed(metrics, 0.10))

    def test_binary_signal_rejects_cost_at_090_or_above(self) -> None:
        poly = OrderBook(bids=[OrderBookLevel(0.42, 1000)], asks=[OrderBookLevel(0.43, 1000)])
        predict = OrderBook(bids=[OrderBookLevel(0.46, 1000)], asks=[OrderBookLevel(0.47, 1000)])

        metrics = calculate_spread_metrics(poly, predict, 100, 0.10, 0.015, max_price_impact=0.015)

        self.assertAlmostEqual(metrics.combined_cost_per_payout, 0.90)
        self.assertFalse(is_binary_signal_allowed(metrics, 0.10))

    def test_trading_fees_are_included_in_signal_profitability(self) -> None:
        poly = OrderBook(bids=[OrderBookLevel(0.41, 1000)], asks=[OrderBookLevel(0.42, 1000)])
        predict = OrderBook(bids=[OrderBookLevel(0.46, 1000)], asks=[OrderBookLevel(0.47, 1000)])

        metrics = calculate_spread_metrics(
            poly,
            predict,
            100,
            0.10,
            0.015,
            max_price_impact=0.015,
            polymarket_fee_pct=0.02,
            predict_fun_fee_pct=0.02,
        )

        self.assertGreater(metrics.combined_cost_per_payout, 0.90)
        self.assertFalse(is_binary_signal_allowed(metrics, 0.10))

    def test_polymarket_dynamic_taker_fee_uses_price_curve(self) -> None:
        fee = VenueFeeQuote("Polymarket", fee_rate_bps=200, model="polymarket_taker")

        self.assertEqual(fee.fee_for_fill(Decimal("10"), Decimal("0.5")), Decimal("0.05"))
        self.assertLess(
            fee.fee_for_fill(Decimal("10"), Decimal("0.9")),
            fee.fee_for_fill(Decimal("10"), Decimal("0.5")),
        )

    def test_polymarket_v2_fee_curve_uses_market_exponent(self) -> None:
        fee = VenueFeeQuote(
            "Polymarket",
            fee_rate_bps=500,
            model="polymarket_dynamic",
            source="polymarket_clob_market_info_v2",
            verified=True,
            fee_exponent=Decimal("2"),
        )

        self.assertEqual(
            fee.fee_for_fill(Decimal("10"), Decimal("0.5")),
            Decimal("0.031250"),
        )

    def test_required_executable_depth_blocks_shallow_book(self) -> None:
        poly = OrderBook(bids=[OrderBookLevel(0.4, 100)], asks=[OrderBookLevel(0.4, 25)])
        predict = OrderBook(bids=[OrderBookLevel(0.5, 100)], asks=[OrderBookLevel(0.5, 100)])

        with self.assertRaisesRegex(ValueError, "executable depth"):
            build_position_plan(
                poly,
                predict,
                10,
                0.015,
                max_price_impact=0.015,
                required_executable_depth_usd=12.5,
            )

    def test_amm_quote_accounts_for_price_impact(self) -> None:
        pool = AmmPool(yes_reserve=1000, no_reserve=1000)

        small = amm_buy_quote(pool, BinarySide.YES, 10)
        large = amm_buy_quote(pool, BinarySide.YES, 100)

        self.assertGreater(large.slippage_pct, small.slippage_pct)

    def test_synthetic_amm_level_is_not_counted_as_zero_impact_top_depth(self) -> None:
        book = OrderBook(
            bids=[OrderBookLevel(0.49, 1000)],
            asks=[OrderBookLevel(0.50, 1000)],
            raw_payload={
                "amm_pool": {
                    "yes_reserve": 1000,
                    "no_reserve": 1000,
                    "fee_pct": 0,
                }
            },
        )

        self.assertEqual(top_of_book_ask_depth_usd(book), Decimal(0))

    def test_inverted_synthetic_amm_level_is_not_counted_as_zero_impact_top_depth(self) -> None:
        book = OrderBook(
            bids=[OrderBookLevel(0.49, 1000)],
            asks=[OrderBookLevel(0.50, 1000)],
            raw_payload={
                "source": {
                    "amm_pool": {
                        "yes_reserve": 1000,
                        "no_reserve": 1000,
                        "fee_pct": 0,
                    }
                },
                "inverted_from": "YES",
            },
        )

        self.assertEqual(top_of_book_ask_depth_usd(book), Decimal(0))

    def test_position_plan_blocks_thin_book_instead_of_shrinking_size(self) -> None:
        poly = OrderBook(
            bids=[OrderBookLevel(0.40, 1000)],
            asks=[OrderBookLevel(0.40, 10), OrderBookLevel(0.80, 1000)],
        )
        predict = OrderBook(bids=[OrderBookLevel(0.45, 1000)], asks=[OrderBookLevel(0.45, 1000)])

        with self.assertRaisesRegex(ValueError, "price impact"):
            build_position_plan(
                poly,
                predict,
                max_order_size_usd=100,
                max_slippage_pct=0.015,
                max_price_impact=0.015,
            )

    def test_configurable_production_price_impact_cap_is_honored(self) -> None:
        poly = OrderBook(
            bids=[OrderBookLevel(0.39, 1000)],
            asks=[OrderBookLevel(0.40, 241.25), OrderBookLevel(0.80, 1000)],
        )
        predict = OrderBook(bids=[OrderBookLevel(0.45, 1000)], asks=[OrderBookLevel(0.45, 1000)])

        with self.assertRaisesRegex(ValueError, "price impact"):
            build_position_plan(poly, predict, 100, 0.02, max_price_impact=0.015)

        plan = build_position_plan(poly, predict, 100, 0.02, max_price_impact=0.02)
        self.assertGreater(plan.polymarket_contracts, 0)

    def test_signal_blocks_when_best_price_spread_disappears_after_book_walk(self) -> None:
        poly = OrderBook(
            bids=[OrderBookLevel(0.39, 1000)],
            asks=[OrderBookLevel(0.40, 25), OrderBookLevel(0.75, 1000)],
        )
        predict = OrderBook(bids=[OrderBookLevel(0.49, 1000)], asks=[OrderBookLevel(0.50, 1000)])

        with self.assertRaisesRegex(ValueError, "price impact"):
            calculate_spread_metrics(poly, predict, 100, 0.10, 0.015, max_price_impact=0.015)

    def test_position_plan_requires_explicit_production_price_impact(self) -> None:
        poly = OrderBook(bids=[OrderBookLevel(0.39, 1000)], asks=[OrderBookLevel(0.40, 1000)])
        predict = OrderBook(bids=[OrderBookLevel(0.45, 1000)], asks=[OrderBookLevel(0.45, 1000)])

        with self.assertRaisesRegex(TypeError, "max_price_impact"):
            build_position_plan(poly, predict, 100, 0.015)  # type: ignore[call-arg]


if __name__ == "__main__":
    unittest.main()
