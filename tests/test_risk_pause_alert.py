"""Every automatic risk pause must reach a human.

The engine halts trading on the daily loss limit, an UNKNOWN order outcome,
reconciliation drift, consecutive API errors and settlement manual review, and
then waits for an explicit operator resume. Inside a supervised canary window
somebody is watching the console. Running unattended, an alert is the only thing
that closes that loop — so the notifier is wired through the same
`register_pause_callback` hook the execution and reconciliation callbacks use.
"""

from __future__ import annotations

import unittest
from decimal import Decimal

from arbitrage_engine.risk import GlobalRiskController


class RecordingNotifier:
    def __init__(self, fail: bool = False) -> None:
        self.messages: list[str] = []
        self._fail = fail

    async def send_html(self, message: str) -> None:
        if self._fail:
            raise RuntimeError("telegram unreachable")
        self.messages.append(message)


def controller(max_daily_loss_usd: float = 10.0, max_api_errors: int = 3) -> GlobalRiskController:
    return GlobalRiskController(max_daily_loss_usd, max_api_errors)


def attach(risk: GlobalRiskController, notifier: RecordingNotifier) -> None:
    async def alert() -> None:
        await notifier.send_html(f"RISK PAUSED: {risk.pause_reason}")

    risk.register_pause_callback(alert)


class RiskPauseAlertTests(unittest.IsolatedAsyncioTestCase):
    async def test_explicit_pause_is_announced_with_its_reason(self) -> None:
        risk = controller()
        notifier = RecordingNotifier()
        attach(risk, notifier)

        await risk.pause("unknown order outcome: Polymarket client_order_id=abc")

        self.assertEqual(len(notifier.messages), 1)
        self.assertIn("unknown order outcome", notifier.messages[0])

    async def test_daily_loss_limit_is_announced(self) -> None:
        risk = controller(max_daily_loss_usd=10.0)
        notifier = RecordingNotifier()
        attach(risk, notifier)

        await risk.record_realized_result(Decimal("-12"))

        self.assertTrue(risk.is_paused())
        self.assertEqual(len(notifier.messages), 1)
        self.assertIn("daily realized loss", notifier.messages[0])

    async def test_consecutive_api_errors_are_announced_once_at_the_threshold(self) -> None:
        risk = controller(max_api_errors=3)
        notifier = RecordingNotifier()
        attach(risk, notifier)

        for _ in range(4):
            await risk.record_api_error()

        # Paused once; the alert must not repeat for every later error.
        self.assertEqual(len(notifier.messages), 1)
        self.assertIn("consecutive execution API errors", notifier.messages[0])

    async def test_already_paused_does_not_alert_again(self) -> None:
        risk = controller()
        notifier = RecordingNotifier()
        attach(risk, notifier)

        await risk.pause("first")
        await risk.pause("second")

        self.assertEqual(len(notifier.messages), 1)

    async def test_a_failing_notifier_cannot_disturb_the_pause(self) -> None:
        # An outage in the alert path must never leave trading un-paused.
        risk = controller()
        attach(risk, RecordingNotifier(fail=True))

        await risk.pause("reconciliation drift")

        self.assertTrue(risk.is_paused())
        self.assertEqual(risk.pause_reason, "reconciliation drift")

    async def test_resume_is_still_refused_while_the_daily_limit_stands(self) -> None:
        # The guard that makes an unattended daily-loss stop meaningful.
        risk = controller(max_daily_loss_usd=10.0)
        attach(risk, RecordingNotifier())
        await risk.record_realized_result(Decimal("-12"))

        with self.assertRaises(RuntimeError):
            await risk.resume()

        self.assertTrue(risk.is_paused())

    async def test_alert_fires_before_later_callbacks(self) -> None:
        # Registration order is the contract: the alert must not wait on
        # reconciliation, which can take a while.
        risk = controller()
        order: list[str] = []

        async def alert() -> None:
            order.append("alert")

        async def reconcile() -> None:
            order.append("reconcile")

        risk.register_pause_callback(alert)
        risk.register_pause_callback(reconcile)

        await risk.pause("drift")

        self.assertEqual(order, ["alert", "reconcile"])


if __name__ == "__main__":
    unittest.main()
