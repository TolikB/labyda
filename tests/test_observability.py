import asyncio
import time
import unittest
from typing import Any

from arbitrage_engine.connectors.base import BinaryMarketClient
from arbitrage_engine.models import BinarySide, ExecutionReport, OrderBook
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

    async def test_metrics_export_active_market_data_target_counts(self) -> None:
        class ActiveClient(BinaryMarketClient):
            async def watch_order_book(self, token_id: str) -> OrderBook:
                del token_id
                raise AssertionError("unreachable")

            async def buy(
                self,
                token_id: str,
                side: BinarySide,
                contracts: float,
                max_price: float,
                **kwargs: Any,
            ) -> str:
                del token_id, side, contracts, max_price, kwargs
                raise AssertionError("unreachable")

            async def sell(
                self,
                token_id: str,
                side: BinarySide,
                contracts: float,
                min_price: float,
                **kwargs: Any,
            ) -> str:
                del token_id, side, contracts, min_price, kwargs
                raise AssertionError("unreachable")

            async def wait_filled(self, order_id: str, timeout_ms: int) -> ExecutionReport:
                del order_id, timeout_ms
                raise AssertionError("unreachable")

            async def cancel_order(self, order_id: str) -> None:
                del order_id
                raise AssertionError("unreachable")

            async def get_cash_balance(self) -> float:
                raise AssertionError("unreachable")

            def active_market_data_target_count(self) -> int:
                return 3

            def has_active_market_data_targets(self) -> bool:
                return True

            def market_data_ready(self) -> bool:
                return True

            def market_data_age_seconds(self) -> float | None:
                return 0.5

        server = ObservabilityServer(
            "127.0.0.1",
            0,
            GlobalRiskController(10, 3),
            {"Polymarket": ActiveClient()},
        )

        response = await server._metrics(None)  # type: ignore[arg-type]
        assert isinstance(response.body, bytes | bytearray)
        body = response.body.decode()

        self.assertIn('arbitrage_market_data_active_targets{venue="Polymarket"} 3.0', body)

    async def test_readiness_ignores_venues_without_active_market_data_targets(self) -> None:
        class InactiveClient(BinaryMarketClient):
            async def watch_order_book(self, token_id: str) -> OrderBook:
                del token_id
                raise AssertionError("unreachable")

            async def buy(
                self,
                token_id: str,
                side: BinarySide,
                contracts: float,
                max_price: float,
                **kwargs: Any,
            ) -> str:
                del token_id, side, contracts, max_price, kwargs
                raise AssertionError("unreachable")

            async def sell(
                self,
                token_id: str,
                side: BinarySide,
                contracts: float,
                min_price: float,
                **kwargs: Any,
            ) -> str:
                del token_id, side, contracts, min_price, kwargs
                raise AssertionError("unreachable")

            async def wait_filled(self, order_id: str, timeout_ms: int) -> ExecutionReport:
                del order_id, timeout_ms
                raise AssertionError("unreachable")

            async def cancel_order(self, order_id: str) -> None:
                del order_id
                raise AssertionError("unreachable")

            async def get_cash_balance(self) -> float:
                raise AssertionError("unreachable")

            def has_active_market_data_targets(self) -> bool:
                return False

            def market_data_ready(self) -> bool:
                return False

            def market_data_age_seconds(self) -> float | None:
                return 99.0

        server = ObservabilityServer(
            "127.0.0.1",
            0,
            GlobalRiskController(10, 3),
            {"Predict.fun": InactiveClient()},
        )

        ready, reasons = await server.readiness()

        self.assertTrue(ready)
        self.assertEqual(reasons, [])

    async def test_readiness_tolerates_quiet_but_connected_active_market(self) -> None:
        class QuietActiveClient(BinaryMarketClient):
            async def watch_order_book(self, token_id: str) -> OrderBook:
                del token_id
                raise AssertionError("unreachable")

            async def buy(
                self,
                token_id: str,
                side: BinarySide,
                contracts: float,
                max_price: float,
                **kwargs: Any,
            ) -> str:
                del token_id, side, contracts, max_price, kwargs
                raise AssertionError("unreachable")

            async def sell(
                self,
                token_id: str,
                side: BinarySide,
                contracts: float,
                min_price: float,
                **kwargs: Any,
            ) -> str:
                del token_id, side, contracts, min_price, kwargs
                raise AssertionError("unreachable")

            async def wait_filled(self, order_id: str, timeout_ms: int) -> ExecutionReport:
                del order_id, timeout_ms
                raise AssertionError("unreachable")

            async def cancel_order(self, order_id: str) -> None:
                del order_id
                raise AssertionError("unreachable")

            async def get_cash_balance(self) -> float:
                raise AssertionError("unreachable")

            def has_active_market_data_targets(self) -> bool:
                return True

            def market_data_ready(self) -> bool:
                return True

            def market_data_age_seconds(self) -> float | None:
                return 5.0

        server = ObservabilityServer(
            "127.0.0.1",
            0,
            GlobalRiskController(10, 3),
            {"Myriad": QuietActiveClient()},
            max_market_data_age_seconds=2.0,
            max_stream_silence_seconds=10.0,
        )

        ready, reasons = await server.readiness()

        self.assertTrue(ready)
        self.assertEqual(reasons, [])

    async def test_readiness_fails_after_stream_silence_threshold(self) -> None:
        class SilentActiveClient(BinaryMarketClient):
            async def watch_order_book(self, token_id: str) -> OrderBook:
                del token_id
                raise AssertionError("unreachable")

            async def buy(
                self,
                token_id: str,
                side: BinarySide,
                contracts: float,
                max_price: float,
                **kwargs: Any,
            ) -> str:
                del token_id, side, contracts, max_price, kwargs
                raise AssertionError("unreachable")

            async def sell(
                self,
                token_id: str,
                side: BinarySide,
                contracts: float,
                min_price: float,
                **kwargs: Any,
            ) -> str:
                del token_id, side, contracts, min_price, kwargs
                raise AssertionError("unreachable")

            async def wait_filled(self, order_id: str, timeout_ms: int) -> ExecutionReport:
                del order_id, timeout_ms
                raise AssertionError("unreachable")

            async def cancel_order(self, order_id: str) -> None:
                del order_id
                raise AssertionError("unreachable")

            async def get_cash_balance(self) -> float:
                raise AssertionError("unreachable")

            def has_active_market_data_targets(self) -> bool:
                return True

            def market_data_ready(self) -> bool:
                return True

            def market_data_age_seconds(self) -> float | None:
                return 11.0

        server = ObservabilityServer(
            "127.0.0.1",
            0,
            GlobalRiskController(10, 3),
            {"Myriad": SilentActiveClient()},
            max_market_data_age_seconds=2.0,
            max_stream_silence_seconds=10.0,
        )

        ready, reasons = await server.readiness()

        self.assertFalse(ready)
        self.assertEqual(reasons, ["market_data_stale:Myriad:11.000"])


if __name__ == "__main__":
    unittest.main()
