import asyncio
import tempfile
import unittest
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

from arbitrage_engine.risk import GlobalRiskController


class _RiskStore:
    def __init__(self) -> None:
        self.state: dict[str, object] | None = None

    async def load_risk_state(self) -> dict[str, object] | None:
        return self.state

    async def save_risk_state(self, state: dict[str, object]) -> None:
        self.state = state


class GlobalRiskControllerTests(unittest.IsolatedAsyncioTestCase):
    async def test_external_pause_refresh_runs_live_process_callbacks(self) -> None:
        store = _RiskStore()
        controller = GlobalRiskController(10, 3, state_store=store)
        await controller.initialize()
        callbacks = 0

        async def on_pause() -> None:
            nonlocal callbacks
            callbacks += 1

        controller.register_pause_callback(on_pause)
        assert store.state is not None
        store.state = {**store.state, "paused": True, "pause_reason": "operator stop"}

        self.assertTrue(await controller.refresh_from_store())
        self.assertTrue(controller.paused)
        self.assertEqual(controller.pause_reason, "operator stop")
        self.assertEqual(callbacks, 1)

    async def test_external_monitor_observes_pause(self) -> None:
        store = _RiskStore()
        controller = GlobalRiskController(10, 3, state_store=store)
        await controller.initialize()
        controller.start_external_monitor(0.01)
        assert store.state is not None
        store.state = {**store.state, "paused": True, "pause_reason": "operator stop"}

        await asyncio.sleep(0.03)

        self.assertTrue(controller.paused)
        await controller.close()
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
