import unittest
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

from arbitrage_engine.models import (
    ArbitrageSignal,
    BinarySide,
    MarketSpec,
    OpenPosition,
    PositionPlan,
    SpreadMetrics,
)
from arbitrage_engine.telegram import format_settlement_message, format_signal_message, risk_pause_alert


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

    def test_only_pauses_that_need_a_human_reach_telegram(self) -> None:
        from arbitrage_engine.reconciliation import RECONCILIATION_TRANSIENT_PAUSE_REASON

        loss = Decimal("0")
        # Routine and self-recovering: nothing.
        for reason in (
            "funded_canary_window_complete",
            "production_closeout_exit_fail_closed",
            "production_closeout_shadow_setup",
            RECONCILIATION_TRANSIENT_PAUSE_REASON,
            "daily realized loss $12.00 reached limit $10.00",
            "3 consecutive execution API errors",
        ):
            with self.subTest(reason=reason):
                self.assertIsNone(risk_pause_alert(reason, loss, "quote_arb"))
        # Already announced with details by the site that paused: nothing more.
        for reason in (
            "Polymarket trading geoblocked from this host",
            "filled execution report missing avg_price: Myriad",
            "unwind exhausted after 30 attempts: BTC-USD",
            "settlement manual review required for BTC-USD: void",
            "2 unresolved entry intent(s) found after restart",
        ):
            with self.subTest(reason=reason):
                self.assertIsNone(risk_pause_alert(reason, loss, "quote_arb"))
        # Needs a human, and nobody else said so.
        for reason in (
            "continuous reconciliation detected drift",
            "continuous reconciliation failed: boom",
            "unknown order outcome: Polymarket client_order_id=abc",
            "residual opposite exposure: Myriad client_order_id=abc",
            "production drain: release",
            None,
        ):
            with self.subTest(reason=reason):
                message = risk_pause_alert(reason, Decimal("3.5"), "quote_arb")
                assert message is not None
                self.assertIn("Потрібне втручання", message)
                self.assertIn("Торгівля зупинена:", message)
                self.assertIn("Збиток за день: $3.50", message)
                self.assertIn("risk resume", message)

    def test_settlement_message_reports_the_pair_pnl_before_fees(self) -> None:
        position = OpenPosition(
            market=MarketSpec(
                symbol="Broncos vs. Chiefs: Who wins?",
                target_label="Broncos",
                polymarket_token_id="poly",
                polymarket_side=BinarySide.NO,
                predict_fun_token_id="3041:YES",
                predict_fun_side=BinarySide.YES,
                venue_b_label="Myriad",
                myriad_market_id="3041",
                myriad_side=BinarySide.YES,
                myriad_url="https://myriad.markets/markets/3041",
            ),
            polymarket_contracts=Decimal("16.652174"),
            polymarket_entry_price=Decimal("0.4724"),
            predict_fun_contracts=Decimal("16.652174"),
            predict_fun_entry_price=Decimal("0.4669"),
            opened_at=datetime(2026, 9, 14, 21, 28, tzinfo=UTC),
            polymarket_order_id="poly-1",
            predict_fun_order_id="myriad-1",
        )
        payout = Decimal("16.652174")
        entry_cost = Decimal("16.652174") * (Decimal("0.4724") + Decimal("0.4669"))
        message = format_settlement_message(position, payout_contracts=payout, entry_cost_usd=entry_cost)
        self.assertIn("Позицію закрито</b> (ринок вирішено)", message)
        self.assertIn("Виплата $16.65 \u2212 вхід $15.64 = <b>+1.01 $ (+6.5%)</b> до комісій", message)
        self.assertNotIn("без пари", message)
        # Short: three lines, no links, no per-leg detail.
        self.assertLessEqual(message.count(chr(10)), 3)

        lopsided = replace(position, predict_fun_contracts=Decimal("10"))
        message = format_settlement_message(
            lopsided, payout_contracts=Decimal("10"), entry_cost_usd=Decimal("12.535")
        )
        self.assertIn("Ще 6.65 контр. на Polymarket без пари", message)
        self.assertIn("<b>-2.54 $", message)

    def test_intervention_messages_say_what_and_what_to_do(self) -> None:
        message = risk_pause_alert("continuous reconciliation detected drift", Decimal("0"), "quote_arb")
        assert message is not None
        self.assertEqual(
            message,
            "\U0001F6A8 <b>Потрібне втручання</b>"
            + chr(10)
            + "Торгівля зупинена: позиції на венью не збігаються з базою (дрифт)."
            + chr(10)
            + "\u27a1\ufe0f Перевірте позиції та ордери на венью, потім risk resume.",
        )
        unknown = risk_pause_alert(
            "unknown order outcome: Polymarket client_order_id=abc", Decimal("3.5"), "quote_arb"
        )
        assert unknown is not None
        self.assertIn("невідомий результат ордера на Polymarket. Збиток за день: $3.50.", unknown)
        self.assertNotIn("client_order_id", unknown)
        self.assertIn("&lt;b&gt;", risk_pause_alert("venue said <b>", Decimal("0"), "quote_arb") or "")

    def test_closed_position_messages_are_three_lines(self) -> None:
        from arbitrage_engine.models import ExitSignal
        from arbitrage_engine.telegram import format_closed_leg_message, format_exit_message

        leg = format_closed_leg_message("Broncos vs. Chiefs: Who wins?", attempts=3)
        self.assertIn("Позицію закрито</b> (незахеджовану ногу продано)", leg)
        self.assertIn("Спроб: 3", leg)
        self.assertLessEqual(leg.count(chr(10)), 2)

        position = OpenPosition(
            market=MarketSpec(
                symbol="BTC above $78,000?",
                target_label="Yes",
                polymarket_token_id="poly",
                polymarket_side=BinarySide.YES,
                predict_fun_token_id="predict",
                predict_fun_side=BinarySide.NO,
            ),
            polymarket_contracts=Decimal("20"),
            polymarket_entry_price=Decimal("0.45"),
            predict_fun_contracts=Decimal("20"),
            predict_fun_entry_price=Decimal("0.5"),
            opened_at=datetime(2026, 9, 20, tzinfo=UTC),
            polymarket_order_id="p",
            predict_fun_order_id="q",
        )
        exit_signal = ExitSignal(
            position=position,
            polymarket_exit_price=Decimal("0.5"),
            predict_fun_exit_price=Decimal("0.52"),
            profit_pct=0.0421,
            profit_usd=Decimal("0.80"),
        )
        closed = format_exit_message(exit_signal, is_test=False)
        self.assertIn("Позицію закрито</b> (достроковий вихід)", closed)
        self.assertIn("<b>+0.80 $ (+4.2%)</b>", closed)
        self.assertLessEqual(closed.count(chr(10)), 2)
        self.assertTrue(format_exit_message(exit_signal, is_test=True).startswith("\U0001F4B0 <b>[ТЕСТ] "))

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
