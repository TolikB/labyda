from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from aiohttp import web
from prometheus_client import CollectorRegistry, Counter, Gauge, generate_latest

from .connectors.base import BinaryMarketClient
from .database import ProductionRepository
from .reconciliation import ReconciliationService
from .risk import GlobalRiskController


class ObservabilityServer:
    def __init__(
        self,
        host: str,
        port: int,
        risk: GlobalRiskController,
        clients: dict[str, BinaryMarketClient],
        *,
        repository: ProductionRepository | None = None,
        reconciliation: ReconciliationService | None = None,
        discovery_ready: Callable[[], bool] | None = None,
        discovery_status: Callable[[], dict[str, Any]] | None = None,
        max_market_data_age_seconds: float = 2.0,
    ) -> None:
        self._host = host
        self._port = port
        self._risk = risk
        self._clients = clients
        self._repository = repository
        self._reconciliation = reconciliation
        self._discovery_ready = discovery_ready or (lambda: True)
        self._discovery_status = discovery_status or dict
        self._max_market_data_age_seconds = max_market_data_age_seconds
        self._runner: web.AppRunner | None = None
        self._loop_lag_task: asyncio.Task[None] | None = None
        self.registry = CollectorRegistry()
        self.ready_gauge = Gauge("arbitrage_ready", "Whether execution prerequisites are ready", registry=self.registry)
        self.risk_paused = Gauge("arbitrage_risk_paused", "Whether global risk is paused", registry=self.registry)
        self.book_age = Gauge(
            "arbitrage_market_data_age_seconds",
            "Age of the latest real market-data event received from the venue",
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
            "arbitrage_reconciliation_drift_total", "Cumulative reconciliation drift records", registry=self.registry
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

    async def close(self) -> None:
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

    async def _live(self, request: web.Request) -> web.Response:
        del request
        return web.json_response({"status": "live"})

    async def _ready(self, request: web.Request) -> web.Response:
        del request
        ready, reasons = await self.readiness()
        return web.json_response(
            {
                "status": "ready" if ready else "not_ready",
                "reasons": reasons,
                "discovery": self._discovery_status(),
            },
            status=200 if ready else 503,
        )

    async def _metrics(self, request: web.Request) -> web.Response:
        del request
        snapshot_task = (
            asyncio.create_task(self._repository_metrics_snapshot()) if self._repository is not None else None
        )
        ready, _ = await self.readiness()
        self.ready_gauge.set(int(ready))
        self.risk_paused.set(int(self._risk.is_paused()))
        self.realized_daily_loss.set(float(self._risk.daily_loss_usd))
        self.consecutive_api_errors.set(self._risk.consecutive_api_errors)
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
            age = client.market_data_age_seconds()
            if age is not None:
                self.book_age.labels(venue=venue).set(age)
            for event, value in client.telemetry_snapshot().items():
                self.market_data_events.labels(venue=venue, event=event).set(value)
        if snapshot_task is not None:
            try:
                snapshot = await snapshot_task
                self.catalog_count.set(float(snapshot["canonical_markets"]))
                for status, count in snapshot["mappings"].items():
                    self.mapping_count.labels(status=status).set(float(count))
                for status, count in snapshot["order_intents"].items():
                    self.order_lifecycle.labels(status=status).set(float(count))
                self.reconciliation_drift.set(float(snapshot["reconciliation_drift_total"]))
                self.exposure.set(float(snapshot["exposure_usd"]))
            except Exception:
                self.api_errors.inc()
        return web.Response(body=generate_latest(self.registry), content_type="text/plain")

    async def readiness(self) -> tuple[bool, list[str]]:
        reasons: list[str] = []
        if self._risk.is_paused():
            reasons.append(f"risk_paused:{self._risk.pause_reason or 'unknown'}")
        if not self._discovery_ready():
            reasons.append("discovery_not_ready")
        if self._repository is not None:
            try:
                async with asyncio.timeout(1.0):
                    if not await self._repository.ping():
                        reasons.append("database_unavailable")
                    elif await self._repository.has_stale_mappings():
                        reasons.append("stale_market_mappings")
            except TimeoutError:
                reasons.append("database_unavailable")
        if self._reconciliation is not None and not self._reconciliation.ready:
            reasons.append(f"reconciliation_not_ready:{self._reconciliation.last_error or 'unknown'}")
        for venue, client in self._clients.items():
            if not client.has_active_market_data_targets():
                continue
            if not client.market_data_ready():
                reasons.append(f"market_data_invalid:{venue}")
            age = client.market_data_age_seconds()
            if age is not None and age > self._max_market_data_age_seconds:
                reasons.append(f"market_data_stale:{venue}:{age:.3f}")
        return not reasons, reasons

    async def _repository_metrics_snapshot(self) -> dict[str, Any]:
        assert self._repository is not None
        async with asyncio.timeout(1.0):
            return await self._repository.metrics_snapshot()
