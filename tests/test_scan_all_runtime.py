import unittest
from collections.abc import Callable
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
        self.resolve_input_sizes: list[int] = []

    def invalidate_cache(self) -> None:
        return None

    async def resolve(self, markets: list[MarketSpec]) -> list[MarketSpec]:
        self.resolve_input_sizes.append(len(markets))
        return list(self.markets) if not markets else markets


class _TransformCatalog:
    def __init__(self, transform: Callable[[MarketSpec], MarketSpec]) -> None:
        self._transform = transform
        self.last_catalog_counts = (0, 0)

    def invalidate_cache(self) -> None:
        return None

    async def resolve(self, markets: list[MarketSpec]) -> list[MarketSpec]:
        return [self._transform(market) for market in markets]


class _SxRouteGammaResolver(GammaMarketResolver):
    def __init__(self, expiry: datetime) -> None:
        super().__init__(scan_all=True)
        self.expiry = expiry
        self.refreshes = 0

    async def _fetch_all_markets(self) -> list[dict[str, object]]:
        self.refreshes += 1
        self._refresh_http_requests = 1
        self._refresh_429s = 0
        self._refresh_pages = 1
        return [
            {
                "id": "poly-france",
                "question": "Will France win the World Cup?",
                "conditionId": "condition-france",
                "endDate": self.expiry.isoformat(),
                "outcomes": '["The Field", "France"]',
                "clobTokenIds": '["poly-no", "poly-yes"]',
                "active": True,
                "closed": False,
                "archived": False,
                "acceptingOrders": True,
                "enableOrderBook": True,
                "volume": 100_000,
            }
        ]


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
        myriad_catalog = _Catalog([seed])
        predict_catalog = _Catalog([])
        sx_catalog = _Catalog([])
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
                myriad_catalog,  # type: ignore[arg-type]
                predict_catalog,  # type: ignore[arg-type]
                sx_catalog,  # type: ignore[arg-type]
                None,
                predict_enabled=False,
                sx_enabled=False,
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
        self.assertIn("polymarket_scan_all_resolution_summary", messages)
        self.assertIn("discovery_pipeline_summary", messages)
        self.assertIn("active_market_snapshot_published", messages)
        self.assertNotIn("ValueError", messages)
        self.assertNotIn("Traceback", messages)
        self.assertEqual(myriad_catalog.resolve_input_sizes, [0, 1, 0, 1])
        await gamma.close()

    async def test_scan_all_snapshot_can_publish_sx_routes_as_tradable(self) -> None:
        expiry = datetime.now(UTC) + timedelta(days=30)
        gamma = _SxRouteGammaResolver(expiry)
        myriad_catalog = _TransformCatalog(
            lambda market: replace(
                market,
                myriad_market_id="396",
                myriad_side=BinarySide.NO,
                myriad_volume_usd=80_000,
            )
        )
        predict_catalog = _Catalog([])
        sx_seed = MarketSpec(
            symbol="Will France win the World Cup?",
            target_label="France",
            polymarket_token_id="",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="0xsx:NO",
            predict_fun_side=BinarySide.NO,
            venue_b_label="SX Bet",
            predict_fun_market_id="0xsxmarket",
            predict_fun_volume_usd=90_000,
            expires_at=expiry,
            category="sports",
        )
        sx_catalog = _Catalog([sx_seed])
        config = replace(
            load_config(Path(__file__).parents[1] / "config.example.json"),
            scan_all=True,
            enable_sx_bet=True,
            sx_bet=replace(load_config(Path(__file__).parents[1] / "config.example.json").sx_bet, enabled=True),
            myriad_markets=replace(
                load_config(Path(__file__).parents[1] / "config.example.json").myriad_markets,
                enabled=True,
            ),
            routes=replace(
                load_config(Path(__file__).parents[1] / "config.example.json").routes,
                polymarket_sx=True,
                sx_myriad=True,
                polymarket_predict=False,
                predict_myriad=False,
                polymarket_myriad=False,
            ),
            categories_to_scan=[],
            execution_mode=ExecutionMode.SHADOW,
            min_market_volume_usd=25_000,
            markets=[],
        )

        result = await _resolve_scan_all_snapshot(
            config,
            gamma,
            myriad_catalog,  # type: ignore[arg-type]
            predict_catalog,  # type: ignore[arg-type]
            sx_catalog,  # type: ignore[arg-type]
            None,
            predict_enabled=False,
            sx_enabled=True,
            myriad_enabled=True,
        )

        self.assertTrue(result.markets)
        self.assertEqual(result.missing_routes, ())
        self.assertTrue(any(market.venue_b_label == "SX Bet" for market in result.markets))
        self.assertTrue(any(market.polymarket_token_id == "poly-yes" for market in result.markets))
        self.assertTrue(any(market.myriad_market_id == "396" for market in result.markets))
        self.assertGreaterEqual(gamma.refreshes, 1)
        await gamma.close()
