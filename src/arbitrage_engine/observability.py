from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

from aiohttp import web
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

from .connectors.base import BinaryMarketClient
from .database import ProductionRepository
from .reconciliation import ReconciliationService
from .risk import GlobalRiskController

_LOGGER = logging.getLogger(__name__)
_REPOSITORY_METRICS_REFRESH_SECONDS = 30.0
_REPOSITORY_METRICS_TIMEOUT_SECONDS = 10.0
_DATABASE_HEALTHY_CACHE_SECONDS = 5.0
_DATABASE_UNHEALTHY_CACHE_SECONDS = 1.0
_DATABASE_FAILURES_BEFORE_UNREADY = 2


class ObservabilityServer:
    def __init__(
        self,
        host: str,
        port: int,
        runtime_instance_id: str,
        risk: GlobalRiskController,
        clients: dict[str, BinaryMarketClient],
        *,
        repository: ProductionRepository | None = None,
        reconciliation: ReconciliationService | None = None,
        discovery_ready: Callable[[], bool] | None = None,
        discovery_status: Callable[[], dict[str, Any]] | None = None,
        max_market_data_age_seconds: float = 2.0,
        max_stream_silence_seconds: float | None = None,
        execution_mode: str = "unknown",
    ) -> None:
        self._host = host
        self._port = port
        self._runtime_instance_id = runtime_instance_id
        self._risk = risk
        self._clients = clients
        self._repository = repository
        self._reconciliation = reconciliation
        self._discovery_ready = discovery_ready or (lambda: True)
        self._discovery_status = discovery_status or dict
        self._max_market_data_age_seconds = max_market_data_age_seconds
        self._execution_mode = execution_mode
        self._max_stream_silence_seconds = (
            max_market_data_age_seconds if max_stream_silence_seconds is None else max_stream_silence_seconds
        )
        self._runner: web.AppRunner | None = None
        self._loop_lag_task: asyncio.Task[None] | None = None
        self._repository_metrics_task: asyncio.Task[None] | None = None
        self._database_health_lock = asyncio.Lock()
        self._database_health_checked_at = 0.0
        self._database_healthy = False
        self._database_health_has_succeeded = False
        self._database_health_failures = 0
        self.registry = CollectorRegistry()
        self.ready_gauge = Gauge("arbitrage_ready", "Whether execution prerequisites are ready", registry=self.registry)
        self.risk_paused = Gauge("arbitrage_risk_paused", "Whether global risk is paused", registry=self.registry)
        self.book_age = Gauge(
            "arbitrage_market_data_age_seconds",
            "Age of the latest real market-data event received from the venue",
            ["venue"],
            registry=self.registry,
        )
        self.active_targets = Gauge(
            "arbitrage_market_data_active_targets",
            "Number of active market-data subscription targets tracked for the venue",
            ["venue"],
            registry=self.registry,
        )
        self.event_loop_lag = Gauge(
            "arbitrage_event_loop_lag_seconds",
            "Delay in scheduling the observability event-loop probe",
            registry=self.registry,
        )
        self.api_errors = Counter(
            "arbitrage_observability_errors_total",
            "Errors while collecting readiness state",
            registry=self.registry,
        )
        self.catalog_count = Gauge("arbitrage_canonical_markets", "Canonical market count", registry=self.registry)
        self.mapping_count = Gauge(
            "arbitrage_market_mappings", "Market mappings by status", ["status"], registry=self.registry
        )
        self.order_lifecycle = Gauge(
            "arbitrage_order_intents", "Durable order intents by state", ["status"], registry=self.registry
        )
        self.reconciliation_drift = Gauge(
            "arbitrage_reconciliation_drift_total",
            "Drift count in the latest reconciliation run for each active venue",
            registry=self.registry,
        )
        self.exposure = Gauge("arbitrage_exposure_usd", "Current local notional exposure", registry=self.registry)
        self.realized_daily_loss = Gauge(
            "arbitrage_realized_daily_loss_usd", "Current UTC-day realized loss", registry=self.registry
        )
        self.consecutive_api_errors = Gauge(
            "arbitrage_consecutive_api_errors",
            "Current consecutive execution API errors",
            registry=self.registry,
        )
        self.discovery_stale = Gauge(
            "arbitrage_discovery_stale",
            "Whether the active discovery snapshot exceeded its stale window",
            registry=self.registry,
        )
        self.discovery_missing_routes = Gauge(
            "arbitrage_discovery_missing_routes",
            "Number of enabled routes without an active market",
            registry=self.registry,
        )
        self.discovery_stage_count = Gauge(
            "arbitrage_discovery_stage_count",
            "Number of markets at each discovery pipeline stage",
            ["stage"],
            registry=self.registry,
        )
        self.discovery_rejections = Gauge(
            "arbitrage_discovery_rejections",
            "Markets rejected by discovery reason",
            ["reason"],
            registry=self.registry,
        )
        self._discovery_stage_labels: set[str] = set()
        self._discovery_rejection_labels: set[str] = set()
        self.market_data_events = Gauge(
            "arbitrage_market_data_events_total",
            "Connector market-data events by type",
            ["venue", "event"],
            registry=self.registry,
        )
        self.runtime_instance = Gauge(
            "arbitrage_runtime_instance_info",
            "Static marker for the current runtime instance",
            ["instance"],
            registry=self.registry,
        )
        self.execution_mode = Gauge(
            "arbitrage_execution_mode_info",
            "Static marker for the effective execution mode",
            ["mode"],
            registry=self.registry,
        )
        self.signal_evaluations = Counter(
            "arbitrage_signal_evaluations_total",
            "Strategy evaluation outcomes by enabled route",
            ["route", "outcome"],
            registry=self.registry,
        )
        self.last_signal_net_spread = Gauge(
            "arbitrage_signal_last_net_spread",
            "Last evaluated net spread by route after fees and size impact",
            ["route"],
            registry=self.registry,
        )
        self.best_signal_net_spread = Gauge(
            "arbitrage_signal_best_net_spread",
            "Best net spread observed by route during this process lifetime",
            ["route"],
            registry=self.registry,
        )
        self.executable_depth = Gauge(
            "arbitrage_executable_depth_usd",
            "Executable ask-side depth by route and leg",
            ["route", "leg"],
            registry=self.registry,
        )
        self.fee_cost = Gauge(
            "arbitrage_fee_cost_usd",
            "Estimated total venue fee cost for the latest route evaluation",
            ["route"],
            registry=self.registry,
        )
        self.chain_cost = Gauge(
            "arbitrage_chain_cost_usd",
            "Live gas-price-adjusted chain cost reserved for the latest route preflight",
            ["route"],
            registry=self.registry,
        )
        self.expected_profit = Gauge(
            "arbitrage_expected_profit_usd",
            "Fee and fixed-cost adjusted profit for the latest route evaluation",
            ["route"],
            registry=self.registry,
        )
        self.dynamic_threshold = Gauge(
            "arbitrage_dynamic_threshold",
            "Current route entry spread threshold",
            ["route"],
            registry=self.registry,
        )
        self.adverse_move_reserve = Gauge(
            "arbitrage_adverse_move_reserve",
            "Route adverse-move percentile plus configured safety buffer",
            ["route"],
            registry=self.registry,
        )
        self.preflight_latency = Gauge(
            "arbitrage_preflight_latency_seconds",
            "Latency of the latest signed pre-submit route preflight",
            ["route"],
            registry=self.registry,
        )
        self.calibration_valid_evaluations = Counter(
            "arbitrage_calibration_valid_evaluations_total",
            "Route evaluations with valid books, fees, constraints, and buffered executable depth",
            ["route"],
            registry=self.registry,
        )
        self.calibration_adverse_move = Histogram(
            "arbitrage_calibration_adverse_move_pct",
            "Observed non-negative net-edge deterioration over the route execution-latency horizon",
            ["route"],
            buckets=(0.0, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.02, 0.05, 0.1),
            registry=self.registry,
        )
        self._best_net_spread_by_route: dict[str, float] = {}

    def record_signal_evaluation(self, route: str, outcome: str, net_spread: float | None = None) -> None:
        self.signal_evaluations.labels(route=route, outcome=outcome).inc()
        if net_spread is not None:
            self.last_signal_net_spread.labels(route=route).set(net_spread)
            best = max(net_spread, self._best_net_spread_by_route.get(route, float("-inf")))
            self._best_net_spread_by_route[route] = best
            self.best_signal_net_spread.labels(route=route).set(best)

    def record_market_economics(self, route: str, values: dict[str, float]) -> None:
        gauges = {
            "fee_cost_usd": self.fee_cost,
            "chain_cost_usd": self.chain_cost,
            "expected_profit_usd": self.expected_profit,
            "dynamic_threshold": self.dynamic_threshold,
            "adverse_move_reserve": self.adverse_move_reserve,
            "preflight_latency_seconds": self.preflight_latency,
        }
        for key, gauge in gauges.items():
            value = values.get(key)
            if value is not None:
                gauge.labels(route=route).set(value)
        for leg in ("first", "second"):
            value = values.get(f"{leg}_executable_depth_usd")
            if value is not None:
                self.executable_depth.labels(route=route, leg=leg).set(value)

    def record_route_calibration(self, route: str, adverse_move: float | None) -> None:
        self.calibration_valid_evaluations.labels(route=route).inc()
        if adverse_move is not None:
            self.calibration_adverse_move.labels(route=route).observe(adverse_move)

    async def start(self) -> None:
        app = web.Application()
        app.router.add_get("/health/live", self._live)
        app.router.add_get("/health/ready", self._ready)
        app.router.add_get("/metrics", self._metrics)
        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self._host, self._port)
        await site.start()
        self._loop_lag_task = asyncio.create_task(self._monitor_event_loop_lag())
        if self._repository is not None:
            self._repository_metrics_task = asyncio.create_task(self._monitor_repository_metrics())

    async def close(self) -> None:
        if self._repository_metrics_task is not None:
            self._repository_metrics_task.cancel()
            await asyncio.gather(self._repository_metrics_task, return_exceptions=True)
            self._repository_metrics_task = None
        if self._loop_lag_task is not None:
            self._loop_lag_task.cancel()
            await asyncio.gather(self._loop_lag_task, return_exceptions=True)
            self._loop_lag_task = None
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None

    async def _monitor_event_loop_lag(self) -> None:
        loop = asyncio.get_running_loop()
        expected = loop.time() + 1.0
        while True:
            await asyncio.sleep(max(0.0, expected - loop.time()))
            now = loop.time()
            self.event_loop_lag.set(max(0.0, now - expected))
            expected = now + 1.0

    async def _monitor_repository_metrics(self) -> None:
        while True:
            try:
                snapshot = await self._repository_metrics_snapshot()
                self._apply_repository_metrics_snapshot(snapshot)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.api_errors.inc()
                _LOGGER.warning(
                    "repository_metrics_snapshot_failed",
                    extra={"runtime_instance_id": self._runtime_instance_id},
                    exc_info=True,
                )
            await asyncio.sleep(_REPOSITORY_METRICS_REFRESH_SECONDS)

    def _apply_repository_metrics_snapshot(self, snapshot: dict[str, Any]) -> None:
        self.catalog_count.set(float(snapshot["canonical_markets"]))
        for status, count in snapshot["mappings"].items():
            self.mapping_count.labels(status=status).set(float(count))
        for status, count in snapshot["order_intents"].items():
            self.order_lifecycle.labels(status=status).set(float(count))
        self.reconciliation_drift.set(float(snapshot["reconciliation_drift_total"]))
        self.exposure.set(float(snapshot["exposure_usd"]))

    async def _live(self, request: web.Request) -> web.Response:
        del request
        return web.json_response({"status": "live"})

    async def _ready(self, request: web.Request) -> web.Response:
        del request
        ready, reasons = await self.readiness()
        return web.json_response(
            {
                "status": "ready" if ready else "not_ready",
                "runtime_instance_id": self._runtime_instance_id,
                "reasons": reasons,
                "discovery": self._discovery_status(),
            },
            status=200 if ready else 503,
        )

    async def _metrics(self, request: web.Request) -> web.Response:
        del request
        ready, _ = await self.readiness()
        self.ready_gauge.set(int(ready))
        self.risk_paused.set(int(self._risk.is_paused()))
        self.realized_daily_loss.set(float(self._risk.daily_loss_usd))
        self.consecutive_api_errors.set(self._risk.consecutive_api_errors)
        self.runtime_instance.labels(instance=self._runtime_instance_id).set(1)
        self.execution_mode.labels(mode=self._execution_mode).set(1)
        discovery = self._discovery_status()
        self.discovery_stale.set(int(bool(discovery.get("stale", False))))
        missing_routes = discovery.get("missing_routes", ())
        self.discovery_missing_routes.set(len(missing_routes) if isinstance(missing_routes, (list, tuple)) else 0)
        diagnostics = discovery.get("diagnostics", {})
        if isinstance(diagnostics, dict):
            stages = diagnostics.get("stages", {})
            rejections = diagnostics.get("rejection_reasons", {})
            if isinstance(stages, dict):
                for label in self._discovery_stage_labels - set(stages):
                    self.discovery_stage_count.labels(stage=label).set(0)
                for label, value in stages.items():
                    self.discovery_stage_count.labels(stage=str(label)).set(float(value))
                self._discovery_stage_labels = {str(label) for label in stages}
            if isinstance(rejections, dict):
                for label in self._discovery_rejection_labels - set(rejections):
                    self.discovery_rejections.labels(reason=label).set(0)
                for label, value in rejections.items():
                    self.discovery_rejections.labels(reason=str(label)).set(float(value))
                self._discovery_rejection_labels = {str(label) for label in rejections}
        for venue, client in self._clients.items():
            self.active_targets.labels(venue=venue).set(float(client.active_market_data_target_count()))
            age = client.market_data_age_seconds()
            if age is not None:
                self.book_age.labels(venue=venue).set(age)
            for event, value in client.telemetry_snapshot().items():
                self.market_data_events.labels(venue=venue, event=event).set(value)
        return web.Response(body=generate_latest(self.registry), content_type="text/plain")

    async def readiness(self) -> tuple[bool, list[str]]:
        reasons: list[str] = []
        if self._risk.is_paused():
            reasons.append(f"risk_paused:{self._risk.pause_reason or 'unknown'}")
        if not self._discovery_ready():
            reasons.append("discovery_not_ready")
        if self._repository is not None and not await self._database_ready():
            reasons.append("database_unavailable")
        if self._reconciliation is not None and not self._reconciliation.ready:
            reasons.append(f"reconciliation_not_ready:{self._reconciliation.last_error or 'unknown'}")
        for venue, client in self._clients.items():
            if not client.has_active_market_data_targets():
                continue
            age = client.market_data_age_seconds()
            stream_connected = client.market_data_stream_connected()
            if stream_connected is False:
                reasons.append(f"market_data_disconnected:{venue}")
            if not client.market_data_ready() and age is None and not client.market_data_transitioning():
                reasons.append(f"market_data_invalid:{venue}")
            if stream_connected is None and age is not None and age > self._max_stream_silence_seconds:
                reasons.append(f"market_data_stale:{venue}:{age:.3f}")
        return not reasons, reasons

    async def _database_ready(self) -> bool:
        assert self._repository is not None
        loop = asyncio.get_running_loop()
        now = loop.time()
        cache_seconds = (
            _DATABASE_HEALTHY_CACHE_SECONDS if self._database_healthy else _DATABASE_UNHEALTHY_CACHE_SECONDS
        )
        if now - self._database_health_checked_at < cache_seconds:
            return self._database_healthy
        async with self._database_health_lock:
            now = loop.time()
            cache_seconds = (
                _DATABASE_HEALTHY_CACHE_SECONDS if self._database_healthy else _DATABASE_UNHEALTHY_CACHE_SECONDS
            )
            if now - self._database_health_checked_at < cache_seconds:
                return self._database_healthy
            probe_healthy = False
            try:
                async with asyncio.timeout(3.0):
                    probe_healthy = bool(await self._repository.ping())
            except TimeoutError:
                pass
            except Exception:
                _LOGGER.exception("database_readiness_probe_failed")
            if probe_healthy:
                self._database_healthy = True
                self._database_health_has_succeeded = True
                self._database_health_failures = 0
            else:
                self._database_health_failures += 1
                self.api_errors.inc()
                _LOGGER.warning(
                    "database_readiness_probe_unhealthy",
                    extra={
                        "runtime_instance_id": self._runtime_instance_id,
                        "consecutive_failures": self._database_health_failures,
                    },
                )
                if (
                    not self._database_health_has_succeeded
                    or self._database_health_failures >= _DATABASE_FAILURES_BEFORE_UNREADY
                ):
                    self._database_healthy = False
            self._database_health_checked_at = loop.time()
            return self._database_healthy

    async def _repository_metrics_snapshot(self) -> dict[str, Any]:
        assert self._repository is not None
        async with asyncio.timeout(_REPOSITORY_METRICS_TIMEOUT_SECONDS):
            return await self._repository.metrics_snapshot()
