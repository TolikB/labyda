import asyncio
import threading
import time
import unittest
from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch

from sqlalchemy import update

from arbitrage_engine.config import load_config
from arbitrage_engine.connectors.myriad import (
    FUNDED_REFRESH_DEADLINE_MARGIN_FRACTION,
    FUNDED_REFRESH_STEADY_TRIGGER_FRACTION,
    PASSIVE_BOOK_MAX_AGE_SECONDS,
    PROACTIVE_REFRESH_MIN_TIMEOUT_SECONDS,
    PROACTIVE_REFRESH_TIMEOUT_FRACTION,
)
from arbitrage_engine.database import MarketMappingRow, ProductionRepository
from arbitrage_engine.discovery_cpu import run_discovery_cpu
from arbitrage_engine.engine import FUNDED_MARKET_DATA_REFRESH_POLL_FRACTION
from arbitrage_engine.main import _resolve_scan_all_snapshot
from arbitrage_engine.market_discovery import GammaResolutionStats
from arbitrage_engine.models import BinarySide, ExecutionMode, MappingStatus, MarketMapping, MarketSpec


class _StaticCatalog:
    def __init__(self, markets: Sequence[MarketSpec], *, clone_each_resolve: bool = False) -> None:
        self._markets = list(markets)
        self._clone_each_resolve = clone_each_resolve
        self.resolve_results: list[list[MarketSpec]] = []
        self.last_catalog_counts = (len(markets), len(markets))

    def invalidate_cache(self) -> None:
        return None

    async def resolve(self, markets: list[MarketSpec]) -> list[MarketSpec]:
        if markets:
            return list(markets)
        if self._clone_each_resolve:
            result = await run_discovery_cpu(_clone_market_specs, self._markets)
        else:
            result = list(self._markets)
        self.resolve_results.append(result)
        return result


class _PassThroughGamma:
    def __init__(self, market_count: int) -> None:
        self.catalog_size = market_count
        self.last_resolution_stats = GammaResolutionStats(requested=market_count)

    def invalidate_cache(self) -> None:
        return None

    async def bootstrap(self, _markets: Sequence[MarketSpec]) -> None:
        return None

    async def resolve(self, markets: list[MarketSpec]) -> list[MarketSpec]:
        return list(markets)


def _clone_market_specs(markets: Sequence[MarketSpec]) -> list[MarketSpec]:
    return [replace(market) for market in markets]


class DatabaseEventLoopResponsivenessTests(unittest.IsolatedAsyncioTestCase):
    async def test_market_candidate_preparation_does_not_block_event_loop(self) -> None:
        repository = object.__new__(ProductionRepository)
        repository._market_candidate_signatures = {}  # noqa: SLF001
        entered = asyncio.Event()
        release = threading.Event()
        loop = asyncio.get_running_loop()

        def slow_prepare(_markets: object) -> list[object]:
            loop.call_soon_threadsafe(entered.set)
            release.wait(timeout=1.0)
            return []

        market = MarketSpec(
            symbol="candidate",
            target_label="YES",
            polymarket_token_id="poly-token",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="predict-token",
            predict_fun_side=BinarySide.NO,
            expires_at=datetime(2026, 9, 7, tzinfo=UTC),
        )
        with patch("arbitrage_engine.database._prepare_market_candidate_batch", slow_prepare):
            task = asyncio.create_task(repository.upsert_market_candidates([market]))
            try:
                await asyncio.wait_for(entered.wait(), timeout=1.0)
                await asyncio.sleep(0.01)
                self.assertFalse(task.done())
            finally:
                release.set()
            await asyncio.wait_for(task, timeout=1.0)

    async def test_verified_mapping_join_does_not_block_event_loop(self) -> None:
        repository = object.__new__(ProductionRepository)
        entered = asyncio.Event()
        release = threading.Event()
        loop = asyncio.get_running_loop()

        def slow_apply(
            markets: Sequence[MarketSpec],
            _mappings: Sequence[MarketMapping],
            _metadata: dict[str, tuple[str, str, str, datetime]],
        ) -> list[MarketSpec]:
            loop.call_soon_threadsafe(entered.set)
            release.wait(timeout=1.0)
            return list(markets)

        with (
            patch.object(repository, "list_mappings", AsyncMock(return_value=[])),
            patch("arbitrage_engine.database._apply_verified_mapping_snapshot", slow_apply),
        ):
            task = asyncio.create_task(repository.apply_verified_mappings([]))
            try:
                await asyncio.wait_for(entered.wait(), timeout=1.0)
                await asyncio.sleep(0.01)
                self.assertFalse(task.done())
            finally:
                release.set()
            self.assertEqual(await asyncio.wait_for(task, timeout=1.0), [])

    async def test_periodic_eight_thousand_market_discovery_stays_responsive(self) -> None:
        loop = asyncio.get_running_loop()
        previous_debug = loop.get_debug()
        # Production runs uvloop without debug. IsolatedAsyncioTestCase enables
        # debug and formats the complete 8k task result for slow-callback logs;
        # that test-only repr cost would contaminate the event-loop measurement.
        loop.set_debug(False)
        market_count = 8_209
        verified_count = 1_338
        expiry = (datetime.now(UTC) + timedelta(days=1)).replace(microsecond=0)
        markets = [
            MarketSpec(
                symbol=f"market-{index}",
                target_label="YES",
                polymarket_token_id=f"poly-token-{index}",
                polymarket_side=BinarySide.YES,
                predict_fun_token_id=f"predict-token-{index}",
                predict_fun_side=BinarySide.NO,
                polymarket_market_id=f"poly-market-{index}",
                predict_fun_market_id=f"predict-market-{index}",
                venue_b_label="Predict.fun",
                expires_at=expiry,
                cutoff_at=expiry,
                mapping_strategy="exact_id",
                resolution_source="shared source",
                outcome_semantics="YES if the event occurs",
                category="crypto",
                polymarket_volume_usd=100_000,
                predict_fun_volume_usd=100_000,
            )
            for index in range(market_count)
        ]
        base_config = load_config(Path(__file__).parents[1] / "config.example.json")
        config = replace(
            base_config,
            scan_all=True,
            routes=replace(
                base_config.routes,
                polymarket_myriad=False,
                polymarket_predict=True,
                predict_myriad=False,
                predict_sx=False,
                polymarket_sx=False,
                sx_myriad=False,
            ),
            categories_to_scan=[],
            execution_mode=ExecutionMode.SHADOW,
            shadow_require_verified_mappings=True,
            min_market_volume_usd=25_000,
            markets=[],
        )
        gamma = _PassThroughGamma(market_count)
        predict_catalog = _StaticCatalog(markets, clone_each_resolve=True)
        empty_catalog = _StaticCatalog([])
        repository = ProductionRepository("sqlite+aiosqlite:///:memory:")
        try:
            await repository.create_schema()
            await _resolve_scan_all_snapshot(
                config,
                gamma,  # type: ignore[arg-type]
                empty_catalog,  # type: ignore[arg-type]
                predict_catalog,  # type: ignore[arg-type]
                empty_catalog,  # type: ignore[arg-type]
                repository,
                predict_enabled=True,
                sx_enabled=False,
                myriad_enabled=False,
            )
            verified_market_ids = [f"poly-market-{index}" for index in range(verified_count)]
            async with repository.transaction() as session:
                await session.execute(
                    update(MarketMappingRow)
                    .where(MarketMappingRow.left_market_id.in_(verified_market_ids))
                    .values(status=MappingStatus.VERIFIED.value)
                )

            stop = asyncio.Event()
            ticker_ticks = 0
            maximum_gap = 0.0

            async def ticker() -> None:
                nonlocal maximum_gap, ticker_ticks
                previous = time.perf_counter()
                while not stop.is_set():
                    await asyncio.sleep(0.001)
                    current = time.perf_counter()
                    maximum_gap = max(maximum_gap, current - previous)
                    previous = current
                    ticker_ticks += 1

            ticker_task = asyncio.create_task(ticker())
            await asyncio.sleep(0)
            try:
                result = await _resolve_scan_all_snapshot(
                    config,
                    gamma,  # type: ignore[arg-type]
                    empty_catalog,  # type: ignore[arg-type]
                    predict_catalog,  # type: ignore[arg-type]
                    empty_catalog,  # type: ignore[arg-type]
                    repository,
                    predict_enabled=True,
                    sx_enabled=False,
                    myriad_enabled=False,
                )
            finally:
                stop.set()
                await ticker_task

            self.assertEqual(len(result.markets), verified_count)
            self.assertEqual(len(predict_catalog.resolve_results), 2)
            self.assertTrue(
                all(
                    first is not second
                    for first, second in zip(
                        predict_catalog.resolve_results[0],
                        predict_catalog.resolve_results[1],
                        strict=True,
                    )
                )
            )
            self.assertGreater(ticker_ticks, 5)
            freshness_seconds = config.max_orderbook_age_seconds
            self.assertEqual(freshness_seconds, PASSIVE_BOOK_MAX_AGE_SECONDS)
            request_timeout_seconds = max(
                PROACTIVE_REFRESH_MIN_TIMEOUT_SECONDS,
                freshness_seconds * PROACTIVE_REFRESH_TIMEOUT_FRACTION,
            )
            poll_seconds = max(
                0.05,
                min(0.1, freshness_seconds * FUNDED_MARKET_DATA_REFRESH_POLL_FRACTION),
            )
            maximum_gap_budget = freshness_seconds - (
                freshness_seconds * FUNDED_REFRESH_STEADY_TRIGGER_FRACTION
                + request_timeout_seconds
                + freshness_seconds * FUNDED_REFRESH_DEADLINE_MARGIN_FRACTION
                + poll_seconds
            )
            self.assertGreater(maximum_gap_budget, 0)
            self.assertLess(
                maximum_gap,
                maximum_gap_budget,
                f"periodic discovery blocked the event loop for {maximum_gap:.3f}s",
            )
        finally:
            await repository.close()
            loop.set_debug(previous_debug)
