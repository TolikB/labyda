import tempfile
import unittest
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

from arbitrage_engine.risk import GlobalRiskController


class GlobalRiskControllerTests(unittest.IsolatedAsyncioTestCase):
    async def test_daily_loss_pauses_all_registered_execution_routes_and_persists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "state.json"
            controller = GlobalRiskController(100.0, 3, state_path)
            pauses = 0

            async def on_pause() -> None:
                nonlocal pauses
                pauses += 1

            controller.register_pause_callback(on_pause)
            controller.register_pause_callback(on_pause)

            self.assertFalse(await controller.record_realized_result(-99.0))
            self.assertTrue(await controller.record_realized_result(-1.0, fees_usd=0.5))
            self.assertTrue(controller.paused)
            self.assertEqual(pauses, 2)

            restored = GlobalRiskController(100.0, 3, state_path)
            self.assertTrue(restored.paused)
            self.assertEqual(restored.daily_loss_usd, Decimal("100.5"))

    async def test_daily_loss_cannot_be_reset_by_same_day_manual_resume(self) -> None:
        controller = GlobalRiskController(10.0, 2)
        await controller.record_realized_result(-10.0)

        self.assertTrue(controller.paused)
        with self.assertRaisesRegex(RuntimeError, "daily-loss limit"):
            await controller.resume()
        self.assertTrue(controller.paused)
        self.assertEqual(controller.daily_loss_usd, Decimal("10.0"))

    async def test_manual_resume_preserves_sub_limit_loss_and_new_day_resets_it(self) -> None:
        controller = GlobalRiskController(10.0, 2)
        await controller.record_realized_result(-4.0)
        await controller.pause("operator pause")
        await controller.resume()

        self.assertFalse(controller.paused)
        self.assertEqual(controller.daily_loss_usd, Decimal("4.0"))

        await controller.pause("overnight review")
        controller.loss_day -= timedelta(days=1)
        await controller.resume()
        self.assertEqual(controller.daily_loss_usd, Decimal(0))

    async def test_corrupt_durable_state_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "state.json"
            state_path.write_text("not-json", encoding="utf-8")

            controller = GlobalRiskController(10.0, 2, state_path)

            self.assertTrue(controller.paused)
            self.assertIn("could not be loaded", controller.pause_reason or "")
