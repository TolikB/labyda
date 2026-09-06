import asyncio
import time
import unittest
from typing import Any

from arbitrage_engine.connectors.base import BinaryMarketClient
from arbitrage_engine.models import BinarySide, ExecutionReport, OrderBook
from arbitrage_engine.observability import ObservabilityServer
from arbitrage_engine.risk import GlobalRiskController


class _TargetAwareClient(BinaryMarketClient):
    def __init__(self, states: dict[str, tuple[bool, float | None]]) -> None:
        self.states = states

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

    def market_data_target_age_seconds(self, token_id: str) -> float | None:
        return self.states[token_id][1]

    def market_data_target_ready(self, token_id: str, max_age_seconds: float) -> bool:
        del max_age_seconds
        return self.states[token_id][0]


class ObservabilityDiscoveryMetricsTests(unittest.IsolatedAsyncioTestCase):
    async def test_metrics_readiness_db_timeout_does_not_block_exporter(self) -> None:
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
            "test",
            GlobalRiskController(10, 3),
            {},
            repository=SlowRepository(),  # type: ignore[arg-type]
        )

        started = time.monotonic()
        response = await server._metrics(None)  # type: ignore[arg-type]

        self.assertLess(time.monotonic() - started, 3.5)
        assert isinstance(response.body, bytes | bytearray)
        self.assertIn(b"arbitrage_ready 0.0", response.body)

    async def test_metrics_scrape_does_not_run_repository_snapshot_inline(self) -> None:
        class Repository:
            async def ping(self) -> bool:
                return True

            async def metrics_snapshot(self) -> dict[str, Any]:
                raise AssertionError("snapshot must run only in the background monitor")

        server = ObservabilityServer(
            "127.0.0.1",
            0,
            "test",
            GlobalRiskController(10, 3),
            {},
            repository=Repository(),  # type: ignore[arg-type]
        )

        response = await server._metrics(None)  # type: ignore[arg-type]

        assert isinstance(response.body, bytes | bytearray)
        self.assertIn(b"arbitrage_observability_errors_total 0.0", response.body)

    async def test_repository_metrics_snapshot_is_applied_by_background_refresh(self) -> None:
        class Repository:
            def __init__(self) -> None:
                self.called = asyncio.Event()

            async def ping(self) -> bool:
                return True

            async def metrics_snapshot(self) -> dict[str, Any]:
                self.called.set()
                return {
                    "canonical_markets": 12,
                    "mappings": {"verified": 3},
                    "order_intents": {"filled": 2},
                    "reconciliation_drift_total": 0,
                    "exposure_usd": 4.5,
                }

        repository = Repository()
        server = ObservabilityServer(
            "127.0.0.1",
            0,
            "test",
            GlobalRiskController(10, 3),
            {},
            repository=repository,  # type: ignore[arg-type]
        )
        task = asyncio.create_task(server._monitor_repository_metrics())  # noqa: SLF001
        await asyncio.wait_for(repository.called.wait(), timeout=1)
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

        response = await server._metrics(None)  # type: ignore[arg-type]

        assert isinstance(response.body, bytes | bytearray)
        body = response.body.decode()
        self.assertIn("arbitrage_canonical_markets 12.0", body)
        self.assertIn('arbitrage_market_mappings{status="verified"} 3.0', body)
        self.assertIn('arbitrage_order_intents{status="filled"} 2.0', body)
        self.assertIn("arbitrage_reconciliation_drift_total 0.0", body)
        self.assertIn("arbitrage_exposure_usd 4.5", body)

    async def test_discovery_pipeline_diagnostics_are_exported(self) -> None:
        risk = GlobalRiskController(10, 3)
        server = ObservabilityServer(
            "127.0.0.1",
            0,
            "test",
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

    async def test_signal_evaluation_metrics_are_route_attributed(self) -> None:
        server = ObservabilityServer(
            "127.0.0.1",
            0,
            "test",
            GlobalRiskController(10, 3),
            {},
        )
        server.record_signal_evaluation("polymarket_predict", "below_min_net_spread", 0.0125)

        response = await server._metrics(None)  # type: ignore[arg-type]
        assert isinstance(response.body, bytes | bytearray)
        body = response.body.decode()

        self.assertIn(
            'arbitrage_signal_evaluations_total{outcome="below_min_net_spread",route="polymarket_predict"} 1.0',
            body,
        )
        self.assertIn('arbitrage_signal_last_net_spread{route="polymarket_predict"} 0.0125', body)

    async def test_route_economics_metrics_are_exported(self) -> None:
        server = ObservabilityServer(
            "127.0.0.1",
            0,
            "test",
            GlobalRiskController(10, 3),
            {},
        )
        server.record_signal_evaluation("polymarket_sx", "eligible_signal", 0.031)
        server.record_shadow_preflight("polymarket_sx", "sample_rejected")
        server.record_shadow_preflight("polymarket_sx", "evidence_passed")
        server.record_accepted_entry_preflight("polymarket_sx")
        server.record_market_economics(
            "polymarket_sx",
            {
                "first_executable_depth_usd": 25.0,
                "second_executable_depth_usd": 19.5,
                "fee_cost_usd": 0.12,
                "chain_cost_usd": 0.25,
                "expected_profit_usd": 0.75,
                "dynamic_threshold": 0.018,
                "adverse_move_reserve": 0.006,
                "preflight_latency_seconds": 0.14,
            },
        )
        server.record_route_calibration("polymarket_sx", 0.001)
        server.record_route_calibration("polymarket_sx", None)

        response = await server._metrics(None)  # type: ignore[arg-type]
        assert isinstance(response.body, bytes | bytearray)
        body = response.body.decode()

        self.assertIn('arbitrage_signal_best_net_spread{route="polymarket_sx"} 0.031', body)
        self.assertIn("arbitrage_runtime_start_time_seconds", body)
        self.assertIn("arbitrage_entry_submission_in_progress 0.0", body)
        self.assertIn(
            'arbitrage_entry_preflight_accepted_total{route="polymarket_sx"} 1.0',
            body,
        )
        self.assertIn(
            'arbitrage_shadow_preflight_evaluations_total{outcome="sample_rejected",route="polymarket_sx"} 1.0',
            body,
        )
        self.assertIn(
            'arbitrage_shadow_preflight_evaluations_total{outcome="evidence_passed",route="polymarket_sx"} 1.0',
            body,
        )
        self.assertIn(
            'arbitrage_shadow_preflight_last_success_timestamp_seconds{route="polymarket_sx"}',
            body,
        )
        self.assertIn('arbitrage_executable_depth_usd{leg="first",route="polymarket_sx"} 25.0', body)
        self.assertIn('arbitrage_fee_cost_usd{route="polymarket_sx"} 0.12', body)
        self.assertIn('arbitrage_chain_cost_usd{route="polymarket_sx"} 0.25', body)
        self.assertIn('arbitrage_expected_profit_usd{route="polymarket_sx"} 0.75', body)

        self.assertIn('arbitrage_dynamic_threshold{route="polymarket_sx"} 0.018', body)
        self.assertIn('arbitrage_adverse_move_reserve{route="polymarket_sx"} 0.006', body)
        self.assertIn('arbitrage_preflight_latency_seconds{route="polymarket_sx"} 0.14', body)
        self.assertIn('arbitrage_calibration_valid_evaluations_total{route="polymarket_sx"} 2.0', body)
        self.assertIn(
            'arbitrage_calibration_adverse_move_pct_bucket{le="0.001",route="polymarket_sx"} 1.0',
            body,
        )
        self.assertIn('arbitrage_calibration_adverse_move_pct_count{route="polymarket_sx"} 1.0', body)

    def test_runtime_start_marker_is_stable_across_observability_servers(self) -> None:
        first = ObservabilityServer("127.0.0.1", 0, "bootstrap", GlobalRiskController(10, 3), {})
        second = ObservabilityServer("127.0.0.1", 0, "primary", GlobalRiskController(10, 3), {})

        first_value = next(iter(first.runtime_start_time.collect())).samples[0].value
        second_value = next(iter(second.runtime_start_time.collect())).samples[0].value

        self.assertGreater(first_value, 0)
        self.assertEqual(first_value, second_value)

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
            "test",
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
            "test",
            GlobalRiskController(10, 3),
            {"Predict.fun": InactiveClient()},
        )

        ready, reasons = await server.readiness()

        self.assertTrue(ready)
        self.assertEqual(reasons, [])

    async def test_readiness_tolerates_quiet_but_connected_active_market(self) -> None:
        class QuietActiveClient(BinaryMarketClient):
            def __init__(self) -> None:
                self.connected = True

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
                return 99.0

            def telemetry_snapshot(self) -> dict[str, float]:
                return {
                    "connected": float(self.connected),
                    "reconnecting": float(not self.connected),
                }

        client = QuietActiveClient()
        server = ObservabilityServer(
            "127.0.0.1",
            0,
            "test",
            GlobalRiskController(10, 3),
            {"Myriad": client},
            max_market_data_age_seconds=2.0,
            max_stream_silence_seconds=10.0,
        )

        ready, reasons = await server.readiness()

        self.assertTrue(ready)
        self.assertEqual(reasons, [])

        client.connected = False
        ready, reasons = await server.readiness()

        self.assertFalse(ready)
        self.assertEqual(reasons, ["market_data_disconnected:Myriad"])

    async def test_readiness_tolerates_connected_market_with_fresh_age_but_not_all_books_valid(self) -> None:
        class FreshButPartialClient(BinaryMarketClient):
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
                return False

            def market_data_age_seconds(self) -> float | None:
                return 0.3

        server = ObservabilityServer(
            "127.0.0.1",
            0,
            "test",
            GlobalRiskController(10, 3),
            {"Myriad": FreshButPartialClient()},
            max_market_data_age_seconds=2.0,
            max_stream_silence_seconds=10.0,
        )

        ready, reasons = await server.readiness()

        self.assertTrue(ready)
        self.assertEqual(reasons, [])

    async def test_readiness_tolerates_only_an_explicit_bounded_target_transition(self) -> None:
        class TransitioningClient(BinaryMarketClient):
            def __init__(self) -> None:
                self.transitioning = True

            async def watch_order_book(self, token_id: str) -> OrderBook:
                del token_id
                raise AssertionError("unreachable")

            async def buy(self, *args: Any, **kwargs: Any) -> str:
                del args, kwargs
                raise AssertionError("unreachable")

            async def sell(self, *args: Any, **kwargs: Any) -> str:
                del args, kwargs
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
                return False

            def market_data_transitioning(self) -> bool:
                return self.transitioning

            def telemetry_snapshot(self) -> dict[str, float]:
                return {"connected": 1.0, "reconnecting": 0.0}

        client = TransitioningClient()
        server = ObservabilityServer(
            "127.0.0.1",
            0,
            "test",
            GlobalRiskController(10, 3),
            {"Polymarket": client},
        )

        ready, reasons = await server.readiness()
        self.assertTrue(ready)
        self.assertEqual(reasons, [])

        client.transitioning = False
        ready, reasons = await server.readiness()
        self.assertFalse(ready)
        self.assertEqual(reasons, ["market_data_invalid:Polymarket"])

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
            "test",
            GlobalRiskController(10, 3),
            {"Myriad": SilentActiveClient()},
            max_market_data_age_seconds=2.0,
            max_stream_silence_seconds=10.0,
        )

        ready, reasons = await server.readiness()

        self.assertFalse(ready)
        self.assertEqual(reasons, ["market_data_stale:Myriad:11.000"])

    async def test_funded_readiness_fails_for_stale_funded_target(self) -> None:
        client = _TargetAwareClient(
            {
                "funded": (False, 12.0),
                "discovery-only": (True, 0.1),
            }
        )
        server = ObservabilityServer(
            "127.0.0.1",
            0,
            "test",
            GlobalRiskController(10, 3),
            {"Polymarket": client},
            max_market_data_age_seconds=2.0,
            funded_market_data_targets=lambda: {
                "polymarket_predict": (("Polymarket", "funded"),),
            },
        )

        ready, reasons = await server.readiness()

        self.assertFalse(ready)
        self.assertEqual(
            reasons,
            ["funded_market_data_stale:polymarket_predict:Polymarket:12.000"],
        )

    async def test_stale_discovery_only_target_does_not_block_funded_readiness(self) -> None:
        client = _TargetAwareClient(
            {
                "funded": (True, 0.1),
                "discovery-only": (False, 12.0),
            }
        )
        server = ObservabilityServer(
            "127.0.0.1",
            0,
            "test",
            GlobalRiskController(10, 3),
            {"Polymarket": client},
            max_market_data_age_seconds=2.0,
            funded_market_data_targets=lambda: {
                "polymarket_predict": (("Polymarket", "funded"),),
            },
        )

        ready, reasons = await server.readiness()

        self.assertTrue(ready)
        self.assertEqual(reasons, [])

    async def test_stale_funded_route_does_not_block_another_ready_route(self) -> None:
        client = _TargetAwareClient(
            {
                "ready": (True, 0.1),
                "stale": (False, 12.0),
            }
        )
        server = ObservabilityServer(
            "127.0.0.1",
            0,
            "test",
            GlobalRiskController(10, 3),
            {"Polymarket": client},
            max_market_data_age_seconds=2.0,
            funded_market_data_targets=lambda: {
                "polymarket_predict": (("Polymarket", "ready"),),
                "polymarket_sx": (("Polymarket", "stale"),),
            },
        )

        ready, reasons = await server.readiness()

        self.assertTrue(ready)
        self.assertEqual(reasons, [])

    async def test_funded_readiness_fails_when_every_route_is_stale(self) -> None:
        client = _TargetAwareClient(
            {
                "first": (False, 11.0),
                "second": (False, 12.0),
            }
        )
        server = ObservabilityServer(
            "127.0.0.1",
            0,
            "test",
            GlobalRiskController(10, 3),
            {"Polymarket": client},
            max_market_data_age_seconds=2.0,
            funded_market_data_targets=lambda: {
                "polymarket_predict": (("Polymarket", "first"),),
                "polymarket_sx": (("Polymarket", "second"),),
            },
        )

        ready, reasons = await server.readiness()

        self.assertFalse(ready)
        self.assertEqual(
            reasons,
            [
                "funded_market_data_stale:polymarket_predict:Polymarket:11.000",
                "funded_market_data_stale:polymarket_sx:Polymarket:12.000",
            ],
        )

    async def test_funded_readiness_fails_when_route_has_no_selected_targets(self) -> None:
        server = ObservabilityServer(
            "127.0.0.1",
            0,
            "test",
            GlobalRiskController(10, 3),
            {},
            funded_market_data_targets=lambda: {"polymarket_predict": ()},
        )

        ready, reasons = await server.readiness()

        self.assertFalse(ready)
        self.assertEqual(
            reasons,
            ["funded_market_data_targets_missing:polymarket_predict"],
        )

    async def test_readiness_tolerates_moderately_slow_database_probe(self) -> None:
        class SlowRepository:
            async def ping(self) -> bool:
                await asyncio.sleep(1.2)
                return True

            async def has_stale_mappings(self) -> bool:
                return False

        server = ObservabilityServer(
            "127.0.0.1",
            0,
            "test",
            GlobalRiskController(10, 3),
            {},
            repository=SlowRepository(),  # type: ignore[arg-type]
        )

        started = time.monotonic()
        ready, reasons = await server.readiness()

        self.assertTrue(ready)
        self.assertEqual(reasons, [])
        self.assertLess(time.monotonic() - started, 3.0)

    async def test_concurrent_readiness_uses_single_flight_database_probe_and_cache(self) -> None:
        class CountingRepository:
            def __init__(self) -> None:
                self.ping_calls = 0

            async def ping(self) -> bool:
                self.ping_calls += 1
                await asyncio.sleep(0.01)
                return True

        repository = CountingRepository()
        server = ObservabilityServer(
            "127.0.0.1",
            0,
            "test",
            GlobalRiskController(10, 3),
            {},
            repository=repository,  # type: ignore[arg-type]
        )

        results = await asyncio.gather(server.readiness(), server.readiness(), server.readiness())

        self.assertEqual(results, [(True, []), (True, []), (True, [])])
        self.assertEqual(repository.ping_calls, 1)
        self.assertEqual(await server.readiness(), (True, []))
        self.assertEqual(repository.ping_calls, 1)

    async def test_database_probe_exception_is_a_cached_readiness_blocker(self) -> None:
        class FailingRepository:
            def __init__(self) -> None:
                self.ping_calls = 0

            async def ping(self) -> bool:
                self.ping_calls += 1
                raise RuntimeError("database overloaded")

        repository = FailingRepository()
        server = ObservabilityServer(
            "127.0.0.1",
            0,
            "test",
            GlobalRiskController(10, 3),
            {},
            repository=repository,  # type: ignore[arg-type]
        )

        self.assertEqual(await server.readiness(), (False, ["database_unavailable"]))
        self.assertEqual(await server.readiness(), (False, ["database_unavailable"]))
        self.assertEqual(repository.ping_calls, 1)

    async def test_readiness_requires_two_consecutive_database_failures_after_success(self) -> None:
        class IntermittentRepository:
            def __init__(self) -> None:
                self.results = iter((True, False, False, True))

            async def ping(self) -> bool:
                return next(self.results)

        server = ObservabilityServer(
            "127.0.0.1",
            0,
            "test",
            GlobalRiskController(10, 3),
            {},
            repository=IntermittentRepository(),  # type: ignore[arg-type]
        )

        self.assertEqual(await server.readiness(), (True, []))
        server._database_health_checked_at = 0.0  # noqa: SLF001
        self.assertEqual(await server.readiness(), (True, []))
        server._database_health_checked_at = 0.0  # noqa: SLF001
        self.assertEqual(await server.readiness(), (False, ["database_unavailable"]))
        server._database_health_checked_at = 0.0  # noqa: SLF001
        self.assertEqual(await server.readiness(), (True, []))

    async def test_readiness_ignores_stale_mapping_backlog(self) -> None:
        class RepositoryWithStaleMappings:
            async def ping(self) -> bool:
                return True

            async def has_stale_mappings(self) -> bool:
                return True

        server = ObservabilityServer(
            "127.0.0.1",
            0,
            "test",
            GlobalRiskController(10, 3),
            {},
            repository=RepositoryWithStaleMappings(),  # type: ignore[arg-type]
        )

        ready, reasons = await server.readiness()

        self.assertTrue(ready)
        self.assertEqual(reasons, [])


if __name__ == "__main__":
    unittest.main()
