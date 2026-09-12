import unittest
from decimal import Decimal

from arbitrage_engine.models import AmmPool, BinarySide, OrderBook, OrderBookLevel, VenueFeeQuote
from arbitrage_engine.quant import (
    amm_buy_quote,
    build_position_plan,
    calculate_spread_metrics,
    depth_limited_leg_notional_usd,
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


class DepthLimitedSizingTests(unittest.TestCase):
    """A market showing $20 at the best ask is tradable at $16, not untradable.

    Entries used to be all-or-nothing at the configured leg size, which on the
    thinner routes discarded between a fifth and a half of every evaluation. The
    property worth keeping is zero price impact, and a smaller fill has exactly
    that -- so the size follows the depth rather than the opportunity being
    dropped.
    """

    @staticmethod
    def book(price: float, size: float) -> OrderBook:
        return OrderBook(bids=[OrderBookLevel(price - 0.01, size)], asks=[OrderBookLevel(price, size)])

    def size_for(
        self,
        first: OrderBook | None,
        second: OrderBook | None,
        **kwargs: float,
    ) -> Decimal | None:
        options = {"target_notional_usd": 25.0, "depth_buffer": 1.25, "minimum_notional_usd": 5.0}
        options.update(kwargs)
        return depth_limited_leg_notional_usd(
            top_of_book_ask_depth_usd(first) if first is not None else None,
            top_of_book_ask_depth_usd(second) if second is not None else None,
            **options,
        )

    def test_deep_books_take_the_full_configured_leg(self) -> None:
        deep = self.book(0.50, 1000)

        self.assertEqual(self.size_for(deep, deep), Decimal("25"))

    def test_the_thinner_book_sets_the_size(self) -> None:
        # $20 resting at the best ask, over a 1.25 buffer, is $16 of impact-free
        # room -- and $16 is what a pair of these books supports, not zero.
        deep = self.book(0.50, 1000)
        thin = self.book(0.50, 40)  # 40 * 0.50 = $20

        self.assertEqual(self.size_for(deep, thin), Decimal("16"))
        self.assertEqual(self.size_for(thin, deep), Decimal("16"))

    def test_the_buffer_is_kept_against_the_chosen_size_not_the_target(self) -> None:
        thin = self.book(0.50, 40)
        sized = self.size_for(thin, thin)

        assert sized is not None
        self.assertLessEqual(sized * Decimal("1.25"), top_of_book_ask_depth_usd(thin))

    def test_the_size_survives_the_float_round_trip_the_guard_puts_it_through(self) -> None:
        # The exact numbers from window-001 of run 20260912T191034Z: a Myriad
        # book with $7.6190476190476188 at the best ask. Sized to depth / 1.25
        # exactly, the size came back through float as 6.095238095238095, and
        # 6.095238095238095 * 1.25 is 7.619047619047619 -- above the depth by a
        # few ulps, so the guard rejected the only eligible market for an hour.
        depth = Decimal("7.6190476190476188")
        sized = depth_limited_leg_notional_usd(
            Decimal("17415.836"),
            depth,
            target_notional_usd=25.0,
            depth_buffer=1.25,
            minimum_notional_usd=5.0,
        )

        assert sized is not None
        self.assertEqual(sized, Decimal("6.09"))
        self.assertLessEqual(Decimal(str(float(sized) * 1.25)), depth)

    def test_sizes_are_whole_cents(self) -> None:
        thin = self.book(0.30, 37)  # $11.10 at the best ask; / 1.25 = 8.88

        self.assertEqual(self.size_for(thin, thin), Decimal("8.88"))
        odd = self.book(0.33, 31)  # $10.23 / 1.25 = 8.184
        self.assertEqual(self.size_for(odd, odd), Decimal("8.18"))

    def test_a_size_below_the_floor_is_refused_rather_than_shrunk_further(self) -> None:
        # Past some point the fill cannot carry its own gas. The net-spread
        # threshold would reject it too, but refusing here keeps the reason legible.
        barely_there = self.book(0.50, 4)  # $2 at the best ask

        self.assertIsNone(self.size_for(barely_there, barely_there))

    def test_depth_beyond_the_best_ask_does_not_count(self) -> None:
        # Filling into the second level moves the marginal price, which is the
        # one thing this is meant to prevent.
        laddered = OrderBook(
            bids=[OrderBookLevel(0.49, 1000)],
            asks=[OrderBookLevel(0.50, 40), OrderBookLevel(0.60, 1000)],
        )

        self.assertEqual(self.size_for(laddered, laddered), Decimal("16"))

    def test_an_absent_book_leaves_the_target_to_the_caller(self) -> None:
        deep = self.book(0.50, 1000)

        self.assertEqual(self.size_for(None, deep), Decimal("25"))
        self.assertEqual(self.size_for(None, None), Decimal("25"))

    def test_an_empty_book_yields_nothing(self) -> None:
        empty = OrderBook(bids=[], asks=[])

        self.assertIsNone(self.size_for(empty, self.book(0.50, 1000)))

    def test_a_non_positive_target_is_refused(self) -> None:
        deep = self.book(0.50, 1000)

        self.assertIsNone(self.size_for(deep, deep, target_notional_usd=0.0))

    def test_a_non_positive_buffer_is_a_programming_error(self) -> None:
        deep = self.book(0.50, 1000)

        with self.assertRaises(ValueError):
            self.size_for(deep, deep, depth_buffer=0.0)


if __name__ == "__main__":
    unittest.main()
