import tempfile
import unittest
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
            self.assertAlmostEqual(restored.daily_loss_usd, 100.5)

    async def test_manual_resume_is_required_after_pause(self) -> None:
        controller = GlobalRiskController(10.0, 2)
        await controller.record_realized_result(-10.0)

        self.assertTrue(controller.paused)
        await controller.resume()
        self.assertFalse(controller.paused)
        self.assertEqual(controller.daily_loss_usd, 0.0)

    async def test_corrupt_durable_state_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "state.json"
            state_path.write_text("not-json", encoding="utf-8")

            controller = GlobalRiskController(10.0, 2, state_path)

            self.assertTrue(controller.paused)
            self.assertIn("could not be loaded", controller.pause_reason or "")
