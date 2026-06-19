import unittest

from arbitrage_engine.models import (
    ArbitrageSignal,
    BinarySide,
    MarketSpec,
    PositionPlan,
    SpreadMetrics,
)
from arbitrage_engine.telegram import format_signal_message


class TelegramFormattingTests(unittest.TestCase):
    def test_signal_contains_clickable_links_for_active_route_only(self) -> None:
        market = MarketSpec(
            symbol="Market",
            target_label="Market",
            polymarket_token_id="poly",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="predict",
            predict_fun_side=BinarySide.NO,
            polymarket_url="https://polymarket.com/event/market",
            predict_fun_market_id="123",
            predict_fun_url="https://predict.fun/market/123",
            myriad_market_id="456",
            myriad_url="https://myriad.markets/markets/456",
        )
        signal = ArbitrageSignal(
            market=market,
            plan=PositionPlan(10, 4, 10, 5, 10, 9),
            metrics=SpreadMetrics(0.1, 0.1, 1, 0, 0, 0.9),
            polymarket_price=0.4,
            predict_fun_price=0.5,
        )

        message = format_signal_message(signal, True, 0.08)

        self.assertIn('<a href="https://polymarket.com/event/market">Polymarket</a>', message)
        self.assertIn('<a href="https://predict.fun/market/123">Predict.fun</a>', message)
        self.assertNotIn("myriad.markets", message)

    def test_untrusted_config_url_is_not_rendered(self) -> None:
        market = MarketSpec(
            symbol="Market",
            target_label="Market",
            polymarket_token_id="poly",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="predict",
            predict_fun_side=BinarySide.NO,
            polymarket_url="javascript:alert(1)",
        )
        signal = ArbitrageSignal(
            market=market,
            plan=PositionPlan(10, 4, 10, 5, 10, 9),
            metrics=SpreadMetrics(0.1, 0.1, 1, 0, 0, 0.9),
            polymarket_price=0.4,
            predict_fun_price=0.5,
        )

        self.assertNotIn("javascript:", format_signal_message(signal, True, 0.08))


if __name__ == "__main__":
    unittest.main()
