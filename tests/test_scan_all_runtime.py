import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

from arbitrage_engine.config import load_config
from arbitrage_engine.discovery_lifecycle import ActiveMarketRegistry, DiscoveryCoordinator, DiscoveryResult
from arbitrage_engine.main import _resolve_scan_all_snapshot
from arbitrage_engine.market_discovery import GammaMarketResolver
from arbitrage_engine.models import BinarySide, ExecutionMode, MarketSpec


class _RuntimeGammaResolver(GammaMarketResolver):
    def __init__(self, expiry: datetime) -> None:
        super().__init__(scan_all=True)
        self.expiry = expiry
        self.refreshes = 0

    async def _fetch_all_markets(self) -> list[dict[str, object]]:
        self.refreshes += 1
        self._refresh_http_requests = 2
        self._refresh_429s = 1 if self.refreshes == 1 else 0
        self._refresh_pages = 1
        return [
            {
                "id": "poly-btc",
                "question": "Will Bitcoin be above 100000?",
                "conditionId": "condition-btc",
                "endDate": self.expiry.isoformat(),
                "outcomes": '["No", "Yes"]',
                "clobTokenIds": '["poly-no", "poly-yes"]',
                "active": True,
                "closed": False,
                "archived": False,
                "acceptingOrders": True,
                "enableOrderBook": True,
                "volume": 100_000,
            }
        ]


class _Catalog:
    def __init__(self, markets: list[MarketSpec]) -> None:
        self.markets = markets
        self.last_catalog_counts = (len(markets), len(markets))

    def invalidate_cache(self) -> None:
        return None

    async def resolve(self, markets: list[MarketSpec]) -> list[MarketSpec]:
        return list(self.markets) if not markets else markets


class ScanAllRuntimeSimulationTests(unittest.IsolatedAsyncioTestCase):
    async def test_accelerated_five_minute_refresh_has_expected_log_contract(self) -> None:
        expiry = datetime.now(UTC) + timedelta(days=30)
        seed = MarketSpec(
            symbol="BTC-100K",
            target_label="Will BTC be above 100000?",
            polymarket_token_id="",
            polymarket_market_id="poly-btc",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="",
            predict_fun_side=BinarySide.NO,
            myriad_market_id="myriad-btc",
            expires_at=expiry,
            myriad_volume_usd=100_000,
        )
        gamma = _RuntimeGammaResolver(expiry)
        myriad = _Catalog([])
        myriad_catalog = _Catalog([seed])
        predict_catalog = _Catalog([])
        config = replace(
            load_config(Path(__file__).parents[1] / "config.example.json"),
            scan_all=True,
            categories_to_scan=[],
            execution_mode=ExecutionMode.SHADOW,
            min_market_volume_usd=25_000,
            markets=[],
        )

        async def refresh() -> DiscoveryResult:
            return await _resolve_scan_all_snapshot(
                config,
                gamma,
                myriad,  # type: ignore[arg-type]
                myriad_catalog,  # type: ignore[arg-type]
                predict_catalog,  # type: ignore[arg-type]
                None,
                predict_enabled=False,
                myriad_enabled=True,
            )

        with self.assertLogs(level="INFO") as captured:
            initial = await refresh()
            registry = ActiveMarketRegistry(initial.markets, missing_routes=initial.missing_routes)
            coordinator = DiscoveryCoordinator(
                registry,
                refresh,
                refresh_interval_seconds=0.01,
                retry_initial_seconds=0.01,
                retry_max_seconds=0.01,
                jitter=0.0,
            )
            self.assertTrue(await coordinator.refresh_once())

        messages = "\n".join(captured.output)
        self.assertGreaterEqual(gamma.refreshes, 2)
        self.assertTrue(registry.ready)
        self.assertIn("gamma_bulk_refresh_completed", messages)
        self.assertIn("polymarket_market_discovered", messages)
        self.assertIn("discovery_pipeline_summary", messages)
        self.assertIn("active_market_snapshot_published", messages)
        self.assertNotIn("ValueError", messages)
        self.assertNotIn("Traceback", messages)
        await gamma.close()
