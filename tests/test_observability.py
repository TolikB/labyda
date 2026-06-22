import asyncio
import time
import unittest
from typing import Any

from arbitrage_engine.observability import ObservabilityServer
from arbitrage_engine.risk import GlobalRiskController


class ObservabilityDiscoveryMetricsTests(unittest.IsolatedAsyncioTestCase):
    async def test_metrics_db_timeout_does_not_block_exporter(self) -> None:
        class SlowRepository:
            async def ping(self) -> bool:
                await asyncio.sleep(10)
                return True

            async def has_stale_mappings(self) -> bool:
                return False

            async def metrics_snapshot(self) -> dict[str, Any]:
                await asyncio.sleep(10)
                return {}

        server = ObservabilityServer(
            "127.0.0.1",
            0,
            GlobalRiskController(10, 3),
            {},
            repository=SlowRepository(),  # type: ignore[arg-type]
        )

        started = time.monotonic()
        response = await server._metrics(None)  # type: ignore[arg-type]

        self.assertLess(time.monotonic() - started, 1.5)
        assert isinstance(response.body, bytes | bytearray)
        self.assertIn(b"arbitrage_ready 0.0", response.body)

    async def test_discovery_pipeline_diagnostics_are_exported(self) -> None:
        risk = GlobalRiskController(10, 3)
        server = ObservabilityServer(
            "127.0.0.1",
            0,
            risk,
            {},
            discovery_status=lambda: {
                "missing_routes": (),
                "stale": False,
                "diagnostics": {
                    "stages": {"tradable": 85},
                    "rejection_reasons": {"no_safe_match": 217},
                },
            },
        )

        response = await server._metrics(None)  # type: ignore[arg-type]
        assert isinstance(response.body, bytes | bytearray)
        body = response.body.decode()

        self.assertIn('arbitrage_discovery_stage_count{stage="tradable"} 85.0', body)
        self.assertIn('arbitrage_discovery_rejections{reason="no_safe_match"} 217.0', body)


if __name__ == "__main__":
    unittest.main()
