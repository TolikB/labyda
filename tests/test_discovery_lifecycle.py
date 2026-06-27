from __future__ import annotations

import unittest
from types import SimpleNamespace

from arbitrage_engine.discovery_lifecycle import (
    ActiveMarketRegistry,
    DiscoveryCoordinator,
    DiscoveryDiagnostics,
    DiscoveryResult,
    _structural_retry_delay,
)
from arbitrage_engine.engine import ArbitrageEngine
from arbitrage_engine.models import BinarySide, ExecutionMode, MarketSpec


def _market(symbol: str = "BTC") -> MarketSpec:
    return MarketSpec(
        symbol=symbol,
        target_label=f"{symbol} above 100000",
        polymarket_token_id="poly",
        polymarket_side=BinarySide.YES,
        predict_fun_token_id="predict",
        predict_fun_side=BinarySide.NO,
    )


class _Clock:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value


class ActiveMarketRegistryTests(unittest.IsolatedAsyncioTestCase):
    async def test_partial_catalog_is_shadow_visible_but_live_entries_are_blocked(self) -> None:
        market = _market()
        registry = ActiveMarketRegistry([market], missing_routes=["predict_myriad"])

        self.assertEqual(registry.tradable_snapshot(ExecutionMode.SHADOW), (market,))
        self.assertEqual(registry.tradable_snapshot(ExecutionMode.CANARY), ())
        self.assertFalse(registry.ready)

        registry.publish(DiscoveryResult((market,), ()))
        self.assertTrue(registry.ready)
        self.assertEqual(registry.tradable_snapshot(ExecutionMode.CANARY), (market,))

    async def test_failure_keeps_last_snapshot_for_fifteen_minutes_then_blocks_entries(self) -> None:
        clock = _Clock()
        market = _market()
        registry = ActiveMarketRegistry([market], max_stale_seconds=900.0, clock=clock)

        clock.value = 899.0
        registry.record_failure("temporary outage")
        self.assertEqual(registry.snapshot(), (market,))

        clock.value = 901.0
        registry.record_failure("catalog remains unavailable")
        self.assertEqual(registry.snapshot(), ())
        self.assertEqual(registry.missing_routes, ("catalog_stale",))

    async def test_coordinator_atomically_publishes_complete_snapshots(self) -> None:
        old_market = _market("OLD")
        new_market = _market("NEW")
        registry = ActiveMarketRegistry([old_market], missing_routes=["polymarket_predict"])
        published: list[tuple[MarketSpec, ...]] = []

        async def refresh() -> DiscoveryResult:
            return DiscoveryResult((new_market,), ())

        coordinator = DiscoveryCoordinator(registry, refresh, on_publish=published.append)
        self.assertTrue(await coordinator.refresh_once())
        self.assertEqual(registry.snapshot(), (new_market,))
        self.assertEqual(published, [(new_market,)])

    async def test_registry_publishes_diagnostics_with_snapshot(self) -> None:
        registry = ActiveMarketRegistry()
        diagnostics = DiscoveryDiagnostics(stages=(("tradable", 1),), rejection_reasons=(("volume_rejected", 2),))

        registry.publish(DiscoveryResult((_market(),), (), diagnostics))

        self.assertEqual(registry.diagnostics.as_dict()["stages"], {"tradable": 1})

    async def test_position_management_continues_when_discovery_blocks_new_entries(self) -> None:
        class PositionManagerStub:
            def __init__(self) -> None:
                self.calls = 0

            async def run_once(self) -> None:
                self.calls += 1

        position_manager = PositionManagerStub()
        config = SimpleNamespace(
            execution_mode=ExecutionMode.CANARY,
            routes=SimpleNamespace(
                polymarket_predict=True,
                polymarket_myriad=True,
                predict_myriad=True,
            ),
            markets=[],
            max_concurrent_market_evaluations=1,
        )
        engine = ArbitrageEngine(
            config,  # type: ignore[arg-type]
            SimpleNamespace(),  # type: ignore[arg-type]
            None,
            None,
            position_manager=position_manager,  # type: ignore[arg-type]
            market_provider=lambda: (),
        )

        await engine.run_once()

        self.assertEqual(position_manager.calls, 1)

    async def test_structural_parser_failure_enters_quarantine_backoff(self) -> None:
        result = DiscoveryResult(
            (_market(),),
            ("polymarket_predict", "predict_myriad"),
            DiscoveryDiagnostics(
                stages=(("predict_catalog_raw", 2713), ("predict_catalog_parsed", 0)),
                rejection_reasons=(),
            ),
        )

        self.assertEqual(_structural_retry_delay(result, 900.0), 900.0)

    async def test_partial_missing_routes_without_schema_drift_do_not_trigger_quarantine(self) -> None:
        result = DiscoveryResult(
            (_market(),),
            ("predict_myriad",),
            DiscoveryDiagnostics(
                stages=(("predict_catalog_raw", 10), ("predict_catalog_parsed", 3)),
                rejection_reasons=(),
            ),
        )

        self.assertIsNone(_structural_retry_delay(result, 900.0))
