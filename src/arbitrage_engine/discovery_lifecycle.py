from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass

from .models import ExecutionMode, MarketSpec

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class DiscoveryDiagnostics:
    stages: tuple[tuple[str, int], ...] = ()
    rejection_reasons: tuple[tuple[str, int], ...] = ()

    def as_dict(self) -> dict[str, dict[str, int]]:
        return {
            "stages": dict(self.stages),
            "rejection_reasons": dict(self.rejection_reasons),
        }


@dataclass(frozen=True)
class DiscoveryResult:
    markets: tuple[MarketSpec, ...]
    missing_routes: tuple[str, ...] = ()
    diagnostics: DiscoveryDiagnostics = DiscoveryDiagnostics()


class ActiveMarketRegistry:
    """Atomically publishes immutable market snapshots to the engine."""

    def __init__(
        self,
        markets: Sequence[MarketSpec] = (),
        *,
        missing_routes: Sequence[str] = (),
        diagnostics: DiscoveryDiagnostics | None = None,
        max_stale_seconds: float = 900.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._markets = tuple(markets)
        self._missing_routes = tuple(missing_routes)
        self._diagnostics = diagnostics or DiscoveryDiagnostics()
        self._max_stale_seconds = max_stale_seconds
        self._clock = clock
        self._last_success_at = clock() if markets else None
        self._last_error: str | None = None

    @property
    def last_error(self) -> str | None:
        return self._last_error

    @property
    def missing_routes(self) -> tuple[str, ...]:
        return self._missing_routes

    @property
    def diagnostics(self) -> DiscoveryDiagnostics:
        return self._diagnostics

    @property
    def ready(self) -> bool:
        return bool(self._markets) and not self._missing_routes and not self.is_stale

    @property
    def is_stale(self) -> bool:
        return self._last_success_at is not None and self._clock() - self._last_success_at > self._max_stale_seconds

    def snapshot(self) -> tuple[MarketSpec, ...]:
        return self._markets

    def tradable_snapshot(self, execution_mode: ExecutionMode) -> tuple[MarketSpec, ...]:
        if execution_mode.submits_orders and not self.ready:
            return ()
        return self._markets

    def publish(self, result: DiscoveryResult) -> None:
        self._markets = result.markets
        self._missing_routes = result.missing_routes
        self._diagnostics = result.diagnostics
        self._last_success_at = self._clock()
        self._last_error = None

    def record_failure(self, error: BaseException | str) -> None:
        self._last_error = str(error)
        if self.is_stale:
            self._markets = ()
            self._missing_routes = ("catalog_stale",)


RefreshMarkets = Callable[[], Awaitable[DiscoveryResult]]
OnPublish = Callable[[tuple[MarketSpec, ...]], None]


class DiscoveryCoordinator:
    def __init__(
        self,
        registry: ActiveMarketRegistry,
        refresh: RefreshMarkets,
        *,
        on_publish: OnPublish | None = None,
        refresh_interval_seconds: float = 300.0,
        retry_initial_seconds: float = 5.0,
        retry_max_seconds: float = 300.0,
        structural_retry_seconds: float = 900.0,
        jitter: float = 0.20,
        random_value: Callable[[], float] = random.random,
    ) -> None:
        self._registry = registry
        self._refresh = refresh
        self._on_publish = on_publish
        self._refresh_interval_seconds = refresh_interval_seconds
        self._retry_initial_seconds = retry_initial_seconds
        self._retry_max_seconds = retry_max_seconds
        self._structural_retry_seconds = structural_retry_seconds
        self._jitter = jitter
        self._random_value = random_value
        self._task: asyncio.Task[None] | None = None
        self._next_retry_delay_override: float | None = None

    async def refresh_once(self) -> bool:
        try:
            result = await self._refresh()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._registry.record_failure(exc)
            LOGGER.exception("discovery_refresh_failed")
            return False
        self._registry.publish(result)
        self._next_retry_delay_override = _structural_retry_delay(result, self._structural_retry_seconds)
        if self._on_publish is not None:
            self._on_publish(result.markets)
        LOGGER.info(
            "active_market_snapshot_published",
            extra={
                "_active_market_count": len(result.markets),
                "_missing_routes": result.missing_routes,
                "_diagnostics": result.diagnostics.as_dict(),
            },
        )
        if self._next_retry_delay_override is not None:
            LOGGER.warning(
                "discovery_refresh_quarantined",
                extra={
                    "_retry_delay_seconds": self._next_retry_delay_override,
                    "_missing_routes": result.missing_routes,
                    "_diagnostics": result.diagnostics.as_dict(),
                },
            )
        return self._registry.ready

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run(), name="cross-venue-discovery")

    async def close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    async def _run(self) -> None:
        retry_delay = self._retry_initial_seconds
        while True:
            delay = self._refresh_interval_seconds if self._registry.ready else retry_delay
            sample = self._random_value()
            delay *= 1.0 - self._jitter + 2.0 * self._jitter * sample
            await asyncio.sleep(delay)
            ready = await self.refresh_once()
            if ready:
                retry_delay = self._retry_initial_seconds
                self._next_retry_delay_override = None
                continue
            if self._next_retry_delay_override is not None:
                retry_delay = self._next_retry_delay_override
                continue
            retry_delay = min(retry_delay * 2.0, self._retry_max_seconds)


def _structural_retry_delay(result: DiscoveryResult, quarantine_seconds: float) -> float | None:
    diagnostics = result.diagnostics.as_dict()
    stages = diagnostics.get("stages", {})
    if (
        stages.get("predict_catalog_raw", 0) > 0
        and stages.get("predict_catalog_parsed", 0) == 0
        and any(route.startswith("predict_") or route.endswith("_predict") for route in result.missing_routes)
    ):
        return quarantine_seconds
    if (
        stages.get("myriad_catalog_raw", 0) > 0
        and stages.get("myriad_catalog_parsed", 0) == 0
        and any(route.startswith("predict_myriad") or route.endswith("_myriad") for route in result.missing_routes)
    ):
        return quarantine_seconds
    return None
