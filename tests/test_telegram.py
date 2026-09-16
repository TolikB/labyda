import unittest
from decimal import Decimal

from arbitrage_engine.models import (
    ArbitrageSignal,
    BinarySide,
    MarketSpec,
    PositionPlan,
    SpreadMetrics,
)
from arbitrage_engine.telegram import format_risk_pause_message, format_signal_message


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
            plan=PositionPlan(*(Decimal(value) for value in ("10", "4", "10", "5", "10", "9"))),
            metrics=SpreadMetrics(0.1, 0.1, 1, 0, 0, 0.9),
            polymarket_price=0.4,
            predict_fun_price=0.5,
        )

        message = format_signal_message(signal, True, 0.08)

        self.assertIn('<a href="https://polymarket.com/event/market">Polymarket</a>', message)
        self.assertIn('<a href="https://predict.fun/market/123">Predict.fun</a>', message)
        self.assertNotIn("myriad.markets", message)

    def test_wrapper_driven_pauses_are_not_worded_as_incidents(self) -> None:
        # Five "🚨 trading halted" overnight, all of them the wrapper closing a
        # window on schedule, and the operator asked what was wrong.
        closed = format_risk_pause_message("funded_canary_window_complete", Decimal("0"), "quote_arb")
        self.assertIn("Funded window closed", closed)
        self.assertNotIn("trading halted", closed)
        self.assertNotIn("risk resume", closed)
        self.assertIn("Instance: quote_arb", closed)

        exited = format_risk_pause_message("production_closeout_exit_fail_closed", Decimal("0"), "quote_arb")
        self.assertIn("paused by the wrapper on exit", exited)
        self.assertNotIn("trading halted", exited)

        setup = format_risk_pause_message("production_closeout_shadow_setup", Decimal("0"), "quote_arb")
        self.assertIn("shadow preflight", setup)

        # The runtime's own pauses keep the siren and the resume instruction.
        drift = format_risk_pause_message("continuous reconciliation detected drift", Decimal("3.5"), "quote_arb")
        self.assertIn("RISK PAUSED", drift)
        self.assertIn("Reason: continuous reconciliation detected drift", drift)
        self.assertIn("Daily realized loss: $3.50", drift)
        self.assertIn("<code>risk resume</code>", drift)
        self.assertIn("Reason: unspecified", format_risk_pause_message(None, Decimal("0"), "quote_arb"))
        # Reasons carry venue error text; it must not become markup.
        self.assertIn("&lt;b&gt;", format_risk_pause_message("venue said <b>", Decimal("0"), "quote_arb"))

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
            plan=PositionPlan(*(Decimal(value) for value in ("10", "4", "10", "5", "10", "9"))),
            metrics=SpreadMetrics(0.1, 0.1, 1, 0, 0, 0.9),
            polymarket_price=0.4,
            predict_fun_price=0.5,
        )

        self.assertNotIn("javascript:", format_signal_message(signal, True, 0.08))


if __name__ == "__main__":
    unittest.main()
