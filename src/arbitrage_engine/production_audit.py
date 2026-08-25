from __future__ import annotations

import asyncio
import gc
import hashlib
import json
import logging
import os
from collections.abc import Iterable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

from .chain_cost import LiveChainCostEstimator, LiveChainCostUnavailable
from .config import AppConfig
from .connectors.base import BinaryMarketClient
from .connectors.myriad import MyriadClient
from .connectors.polymarket import PolymarketClobClient
from .connectors.predict_fun import PredictFunApiClient
from .connectors.sx_bet import create_sx_bet_client
from .database import ProductionRepository
from .discovery_lifecycle import DiscoveryDiagnostics
from .main import (
    _deduplicate_markets,
    _deduplicate_route_markets,
    _filter_markets_by_volume,
    _market_supports_route,
    _missing_discovery_routes,
    _synthesize_predict_sx_markets,
    _verified_active_markets,
)
from .market_discovery import GammaCacheUnavailable, GammaMarketResolver
from .market_mapping import (
    filter_markets_for_categories,
    filter_markets_for_launch_horizon,
    is_live_mapping_eligible,
    launch_category,
    route_key,
)
from .matcher import normalize_text
from .models import (
    BinarySide,
    ExecutionMode,
    MappingStatus,
    MarketConstraints,
    MarketSpec,
    OrderPreview,
    first_leg_side_for_route,
    first_leg_token_for_route,
    myriad_execution_side_for_route,
    myriad_execution_token_for_route,
    position_key,
    second_leg_side_for_route,
    second_leg_token_for_route,
)
from .myriad_discovery import MyriadMarketResolver
from .predict_fun_discovery import PredictFunMarketResolver
from .sx_bet_discovery import SxBetMarketResolver

LOGGER = logging.getLogger(__name__)

SX_EXPLORER_API_URL = "https://explorerl2.sx.technology/api"
ROUTE_NAMES = (
    "polymarket_myriad",
    "polymarket_predict",
    "predict_myriad",
    "predict_sx",
    "polymarket_sx",
    "sx_myriad",
)
_GAMMA_AUDIT_BOOTSTRAP_BACKOFF_SECONDS = (10.0, 30.0)


@dataclass(frozen=True)
class RouteDiscoverySnapshot:
    enabled_routes: tuple[str, ...]
    source_catalogs: dict[str, tuple[MarketSpec, ...]]
    raw_route_candidates: tuple[MarketSpec, ...]
    route_candidates: tuple[MarketSpec, ...]
    category_markets: tuple[MarketSpec, ...]
    volume_markets: tuple[MarketSpec, ...]
    verified_markets: tuple[MarketSpec, ...]
    tradable_markets: tuple[MarketSpec, ...]
    missing_routes: tuple[str, ...]
    diagnostics: DiscoveryDiagnostics
    pre_horizon_raw_route_candidates: tuple[MarketSpec, ...] = ()
    pre_horizon_route_candidates: tuple[MarketSpec, ...] = ()


def enabled_routes(app_config: AppConfig) -> tuple[str, ...]:
    routes: list[str] = []
    if getattr(app_config.routes, "polymarket_myriad", False):
        routes.append("polymarket_myriad")
    if getattr(app_config.routes, "polymarket_predict", False):
        routes.append("polymarket_predict")
    if getattr(app_config.routes, "predict_myriad", False):
        routes.append("predict_myriad")
    if getattr(app_config.routes, "predict_sx", False):
        routes.append("predict_sx")
    if getattr(app_config.routes, "polymarket_sx", False):
        routes.append("polymarket_sx")
    if getattr(app_config.routes, "sx_myriad", False):
        routes.append("sx_myriad")
    return tuple(routes)


def predict_enabled(app_config: AppConfig) -> bool:
    return bool(
        app_config.enable_predict_fun
        and app_config.predict_fun.enabled
        and app_config.predict_fun.api_key
        and (
            app_config.routes.polymarket_predict
            or app_config.routes.predict_myriad
            or getattr(app_config.routes, "predict_sx", False)
        )
    )


def sx_enabled(app_config: AppConfig) -> bool:
    return bool(
        app_config.enable_sx_bet
        and app_config.sx_bet.enabled
        and (
            app_config.routes.polymarket_sx
            or app_config.routes.sx_myriad
            or getattr(app_config.routes, "predict_sx", False)
        )
    )


def myriad_enabled(app_config: AppConfig) -> bool:
    return bool(
        app_config.myriad_markets.enabled
        and (
            app_config.routes.polymarket_myriad
            or app_config.routes.predict_myriad
            or getattr(app_config.routes, "sx_myriad", False)
        )
    )


def enabled_execution_venues(app_config: AppConfig) -> tuple[str, ...]:
    venues = ["Polymarket"]
    if predict_enabled(app_config):
        venues.append("Predict.fun")
    if sx_enabled(app_config):
        venues.append("SX Bet")
    if myriad_enabled(app_config):
        venues.append("Myriad")
    return tuple(venues)


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _runtime_balance_state(runtime_audit: dict[str, Any] | None, venue: str) -> dict[str, float | None]:
    latest_state: dict[str, Any] = {}
    if runtime_audit is not None:
        latest_state = runtime_audit.get("latest_runtime_balance_state") or {}
    venues = latest_state.get("venues", {}) if isinstance(latest_state, dict) else {}
    venue_state = venues.get(venue, {}) if isinstance(venues, dict) else {}
    if not isinstance(venue_state, dict):
        venue_state = {}
    return {
        "balance_cache_usd": _safe_float(venue_state.get("balance_cache_usd")),
        "optimistic_debits_usd": _safe_float(venue_state.get("optimistic_debits_usd")),
        "capital_reservations_usd": _safe_float(venue_state.get("capital_reservations_usd")),
        "effective_balance_usd": _safe_float(venue_state.get("effective_balance_usd")),
        "available_after_reservations_usd": _safe_float(venue_state.get("available_after_reservations_usd")),
    }


def effective_balance_payload(
    venue: str,
    connector_balance: float,
    *,
    direct_balance: float | None = None,
    runtime_audit: dict[str, Any] | None = None,
) -> dict[str, Any]:
    runtime_state = _runtime_balance_state(runtime_audit, venue)
    effective_balance = runtime_state["effective_balance_usd"]
    available_after_reservations = runtime_state["available_after_reservations_usd"]
    payload: dict[str, Any] = {
        "connector_visible_balance_usd": connector_balance,
        "effective_balance_usd": connector_balance if effective_balance is None else effective_balance,
        "balance_cache_usd": runtime_state["balance_cache_usd"],
        "optimistic_debits_usd": runtime_state["optimistic_debits_usd"],
        "capital_reservations_usd": runtime_state["capital_reservations_usd"],
        "available_after_reservations_usd": available_after_reservations,
    }
    if direct_balance is not None:
        payload["direct_balance_usd"] = direct_balance
        payload["direct_vs_connector_delta_usd"] = round(direct_balance - connector_balance, 12)
        payload["direct_matches_connector"] = abs(direct_balance - connector_balance) < 1e-9
        if payload["effective_balance_usd"] is not None:
            payload["direct_vs_effective_delta_usd"] = round(
                direct_balance - float(payload["effective_balance_usd"]),
                12,
            )
    if runtime_state["balance_cache_usd"] is not None:
        payload["runtime_balance_cache_vs_connector_delta_usd"] = round(
            float(runtime_state["balance_cache_usd"]) - connector_balance,
            12,
        )
    if runtime_audit is not None:
        payload["runtime_audit"] = runtime_audit
    return payload


async def load_runtime_audit(app_config: AppConfig) -> dict[str, Any] | None:
    database_url = app_config.database_url
    if not database_url:
        return None
    repository = ProductionRepository(
        database_url,
        runtime_instance_id=app_config.runtime_instance_id,
        enabled_routes=enabled_routes(app_config),
    )
    try:
        if not await repository.ping():
            return None
        return await repository.runtime_audit_snapshot()
    finally:
        await repository.close()


async def load_mapping_coverage(database_url: str | None, routes: tuple[str, ...]) -> dict[str, Any]:
    coverage = {route: {"has_verified": False, "verified_count": 0} for route in routes}
    if not database_url:
        return {"database_reachable": False, "enabled_routes": coverage}
    repository = ProductionRepository(database_url, enabled_routes=routes)
    try:
        if not await repository.ping():
            return {"database_reachable": False, "enabled_routes": coverage}
        mappings = await repository.list_mappings(MappingStatus.VERIFIED)
    finally:
        await repository.close()
    for mapping in mappings:
        route = route_key(mapping.left_venue, mapping.right_venue)
        if route in coverage:
            coverage[route]["has_verified"] = True
            coverage[route]["verified_count"] = int(coverage[route]["verified_count"]) + 1
    return {"database_reachable": True, "enabled_routes": coverage}


def venue_runtime_audit(snapshot: dict[str, Any] | None, venue: str) -> dict[str, Any]:
    if snapshot is None:
        return {
            "database_reachable": False,
            "note": "DATABASE_URL is missing or unreachable; durable runtime state is unavailable.",
        }
    latest_balances = snapshot.get("latest_balance_snapshots", {})
    unresolved_orders = snapshot.get("unresolved_order_intents", {})
    unresolved_redemptions = snapshot.get("unresolved_redemptions", {})
    positions = snapshot.get("positions", {})
    return {
        "database_reachable": True,
        "latest_balance_snapshot": latest_balances.get(venue, {}),
        "unresolved_order_intents": unresolved_orders.get("by_venue", {}).get(venue, {"count": 0, "by_status": {}}),
        "unresolved_redemptions": unresolved_redemptions.get("by_venue", {}).get(venue, {"count": 0, "by_status": {}}),
        "open_position_entry_notional_usd": positions.get("estimated_entry_notional_by_venue_usd", {}).get(
            venue,
            "0",
        ),
        "position_count": positions.get("count", 0),
        "position_statuses": positions.get("by_status", {}),
        "reconciliation_failures": snapshot.get("reconciliation_failures", []),
        "risk_state": snapshot.get("risk_state"),
        "latest_runtime_balance_state": snapshot.get("latest_runtime_balance_state"),
        "metrics": snapshot.get("metrics", {}),
    }


def _unresolved_count(payload: Any) -> int:
    if isinstance(payload, dict):
        try:
            return int(payload.get("count", 0))
        except (TypeError, ValueError):
            return 0
    return 0


def venue_balance_gate(
    *,
    venue: str,
    minimum_balance_usd: float,
    connector_balance: float,
    direct_balance: float | None,
    third_party_balance: float | None = None,
    third_party_balance_label: str | None = None,
    runtime_audit: dict[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    runtime_state = _runtime_balance_state(runtime_audit, venue)
    if connector_balance < minimum_balance_usd:
        blockers.append("connector_visible_balance_below_minimum")
    if direct_balance is not None and direct_balance < minimum_balance_usd:
        blockers.append("direct_balance_below_minimum")
    if direct_balance is not None and abs(direct_balance - connector_balance) >= 1e-9:
        blockers.append("direct_vs_connector_balance_mismatch")
    if (
        runtime_state["effective_balance_usd"] is not None
        and runtime_state["effective_balance_usd"] < minimum_balance_usd
    ):
        blockers.append("runtime_effective_balance_below_minimum")
    if (
        runtime_state["available_after_reservations_usd"] is not None
        and runtime_state["available_after_reservations_usd"] < minimum_balance_usd
    ):
        blockers.append("runtime_available_balance_below_minimum")
    if (
        direct_balance is not None
        and runtime_state["balance_cache_usd"] is not None
        and abs(direct_balance - runtime_state["balance_cache_usd"]) >= 1e-9
    ):
        blockers.append("direct_vs_runtime_balance_cache_mismatch")
    if (
        runtime_state["balance_cache_usd"] is not None
        and abs(runtime_state["balance_cache_usd"] - connector_balance) >= 1e-9
    ):
        blockers.append("runtime_balance_cache_vs_connector_mismatch")
    if third_party_balance is not None:
        label = third_party_balance_label or "third_party"
        if third_party_balance < minimum_balance_usd:
            blockers.append(f"{label}_balance_below_minimum")
        if direct_balance is not None and abs(third_party_balance - direct_balance) >= 1e-9:
            blockers.append(f"{label}_vs_direct_balance_mismatch")
        if abs(third_party_balance - connector_balance) >= 1e-9:
            blockers.append(f"{label}_vs_connector_balance_mismatch")
    if _unresolved_count(runtime_audit.get("unresolved_order_intents")) > 0:
        blockers.append("unresolved_order_intents_present")
    if _unresolved_count(runtime_audit.get("unresolved_redemptions")) > 0:
        blockers.append("unresolved_redemptions_present")
    if runtime_audit.get("reconciliation_failures"):
        blockers.append("reconciliation_failures_present")
    if isinstance(runtime_audit.get("risk_state"), dict) and runtime_audit["risk_state"].get("paused"):
        blockers.append("risk_paused")
    return {
        "venue": venue,
        "passed": not blockers,
        "minimum_balance_usd": minimum_balance_usd,
        "blocking_reasons": blockers,
    }


def order_preview_readiness(
    *,
    requested: bool,
    private_key_configured: bool,
    market_metadata_found: bool = True,
    canary_gate_passed: bool = True,
) -> dict[str, Any]:
    blockers: list[str] = []
    if not private_key_configured:
        blockers.append("private_key_missing")
    if requested and not market_metadata_found:
        blockers.append("market_metadata_not_found")
    if not canary_gate_passed:
        blockers.append("balance_or_runtime_gate_failed")
    return {
        "requested": requested,
        "ready": not blockers,
        "blocking_reasons": blockers,
    }


def _http_probe(url: str) -> dict[str, Any]:
    request = urllib_request.Request(url, headers={"Accept": "application/json, text/plain, */*"})
    try:
        with urllib_request.urlopen(request, timeout=5) as response:
            body = response.read().decode("utf-8", errors="replace")
            return {"url": url, "status": response.status, "ok": 200 <= response.status < 300, "body": body}
    except urllib_error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return {"url": url, "status": exc.code, "ok": False, "body": body}
    except Exception as exc:
        return {"url": url, "ok": False, "error": str(exc)}


def _sx_explorer_balance(address: str, token_address: str) -> dict[str, Any]:
    query = urllib_parse.urlencode(
        {
            "module": "account",
            "action": "tokenbalance",
            "contractaddress": token_address,
            "address": address,
            "tag": "latest",
        }
    )
    url = f"{SX_EXPLORER_API_URL}?{query}"
    payload = _http_probe(url)
    if not payload.get("ok"):
        return payload
    try:
        body = json.loads(str(payload.get("body") or "{}"))
        raw_balance = str(body.get("result"))
        balance_usd = int(raw_balance) / 1_000_000
    except (TypeError, ValueError, json.JSONDecodeError):
        return {
            "url": url,
            "ok": False,
            "error": f"unexpected explorer payload: {payload.get('body')!r}",
        }
    return {
        "url": url,
        "ok": True,
        "balance_raw": raw_balance,
        "balance_usd": balance_usd,
        "payload": body,
    }


def _metric_lines(payload: str, metric_name: str) -> list[str]:
    return [
        line
        for line in payload.splitlines()
        if line and not line.startswith("#") and line.startswith(metric_name)
    ]


def _extract_metric_scalar(payload: str, metric_name: str) -> float | None:
    for line in _metric_lines(payload, metric_name):
        try:
            return float(line.rsplit(" ", 1)[-1])
        except (TypeError, ValueError):
            continue
    return None


async def probe_observability(host: str, port: int) -> dict[str, Any]:
    base = f"http://{host}:{port}"
    live = await asyncio.to_thread(_http_probe, f"{base}/health/live")
    ready = await asyncio.to_thread(_http_probe, f"{base}/health/ready")
    metrics = await asyncio.to_thread(_http_probe, f"{base}/metrics")
    metrics_body = metrics.get("body", "") if metrics.get("ok") else ""
    return {
        "live": {key: value for key, value in live.items() if key != "body"},
        "ready": {key: value for key, value in ready.items() if key != "body"},
        "metrics": {
            "probe": {key: value for key, value in metrics.items() if key != "body"},
            "arbitrage_ready": _extract_metric_scalar(metrics_body, "arbitrage_ready"),
            "arbitrage_risk_paused": _extract_metric_scalar(metrics_body, "arbitrage_risk_paused"),
            "market_data_age_seconds": _metric_lines(metrics_body, "arbitrage_market_data_age_seconds"),
            "market_data_active_targets": _metric_lines(metrics_body, "arbitrage_market_data_active_targets"),
            "reconnecting_events": _metric_lines(metrics_body, "arbitrage_market_data_events_total"),
        },
    }


def go_no_go_report(
    *,
    routes: tuple[str, ...],
    mapping_coverage: dict[str, Any],
    observability: dict[str, Any],
    venue_gates: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    blockers: list[str] = []
    coverage = mapping_coverage.get("enabled_routes", {})
    for route in routes:
        route_state = coverage.get(route, {})
        if not route_state.get("has_verified", False):
            blockers.append(f"missing_verified_mapping:{route}")
    if not observability.get("live", {}).get("ok", False):
        blockers.append("health_live_failed")
    if not observability.get("ready", {}).get("ok", False):
        blockers.append("health_ready_failed")
    metrics = observability.get("metrics", {})
    if metrics.get("arbitrage_ready") not in (None, 1.0):
        blockers.append("arbitrage_ready_not_1")
    if metrics.get("arbitrage_risk_paused") not in (None, 0.0):
        blockers.append("arbitrage_risk_paused_not_0")
    for venue, gate in venue_gates.items():
        if not gate.get("passed", False):
            blockers.append(f"venue_gate_failed:{venue}")
    return {
        "ready_for_canary": not blockers,
        "blocking_reasons": blockers,
    }


def _build_route_candidates(markets: list[MarketSpec]) -> tuple[list[MarketSpec], list[MarketSpec]]:
    polymarket_family = [market for market in markets if market.venue_a_label == "Polymarket"]
    passthrough = [market for market in markets if market.venue_a_label != "Polymarket"]
    predict_family = _deduplicate_markets(
        [market for market in polymarket_family if market.venue_b_label == "Predict.fun"]
    )
    sx_family = _deduplicate_markets(
        [market for market in polymarket_family if market.venue_b_label == "SX Bet"]
    )
    myriad_family = _deduplicate_markets(
        [market for market in polymarket_family if market.venue_b_label == "Myriad"]
    )
    predict_sx = _synthesize_predict_sx_markets(predict_family, sx_family)
    raw = [*passthrough, *predict_family, *sx_family, *myriad_family, *predict_sx]
    return raw, _deduplicate_route_markets(raw)


async def _bootstrap_gamma_for_audit(
    resolver: GammaMarketResolver,
    markets: list[MarketSpec],
) -> None:
    for attempt, delay in enumerate((*_GAMMA_AUDIT_BOOTSTRAP_BACKOFF_SECONDS, None), start=1):
        try:
            await resolver.bootstrap(markets)
            return
        except asyncio.CancelledError:
            raise
        except GammaCacheUnavailable:
            if delay is None:
                raise
            LOGGER.warning(
                "gamma_audit_bootstrap_retry",
                extra={"_attempt": attempt, "_next_delay_seconds": delay},
            )
            await asyncio.sleep(delay)


def _release_discovery_cache(resolver: Any) -> None:
    invalidate_cache = getattr(resolver, "invalidate_cache", None)
    if callable(invalidate_cache):
        invalidate_cache()


async def resolve_route_discovery_snapshot(
    app_config: AppConfig,
    repository: ProductionRepository | None,
) -> RouteDiscoverySnapshot:
    gamma = GammaMarketResolver(
        scan_all=True,
        sports_horizon_hours=app_config.max_sports_market_horizon_hours,
    )
    myriad_catalog = MyriadMarketResolver(
        app_config.myriad_markets,
        scan_all=True,
        categories_to_scan=app_config.categories_to_scan,
    )
    predict_catalog = PredictFunMarketResolver(
        app_config.predict_fun,
        scan_all=True,
        categories_to_scan=app_config.categories_to_scan,
    )
    sx_catalog = SxBetMarketResolver(
        app_config.sx_bet,
        scan_all=True,
        categories_to_scan=app_config.categories_to_scan,
    )
    try:
        predict_on = predict_enabled(app_config)
        sx_on = sx_enabled(app_config)
        myriad_on = myriad_enabled(app_config)
        myriad_catalog.invalidate_cache()
        predict_catalog.invalidate_cache()
        sx_catalog.invalidate_cache()
        catalog_calls: list[tuple[str, Any]] = []
        if myriad_on:
            catalog_calls.append(("Myriad", myriad_catalog.resolve([])))
        if predict_on:
            catalog_calls.append(("Predict.fun", predict_catalog.resolve([])))
        if sx_on:
            catalog_calls.append(("SX Bet", sx_catalog.resolve([])))
        results = await asyncio.gather(*(call for _, call in catalog_calls), return_exceptions=True)
        source_catalogs: dict[str, tuple[MarketSpec, ...]] = {}
        markets: list[MarketSpec] = []
        available: set[str] = set()
        for (venue, _), result in zip(catalog_calls, results, strict=True):
            if isinstance(result, BaseException):
                continue
            available.add(venue)
            source_catalogs[venue] = tuple(result)
            markets.extend(result)

        # The parsed MarketSpec catalogs above are sufficient Gamma seeds. Do
        # not retain each venue's much larger raw JSON cache at the same time as
        # the full Polymarket catalog; venue enrichment can refetch after Gamma
        # has been released.
        for source_resolver in (myriad_catalog, predict_catalog, sx_catalog):
            _release_discovery_cache(source_resolver)
        gc.collect()

        await _bootstrap_gamma_for_audit(gamma, markets)
        markets = await gamma.resolve(markets)
        gamma_stats = gamma.last_resolution_stats
        gamma_catalog_size = gamma.catalog_size
        _release_discovery_cache(gamma)
        await gamma.close()
        gc.collect()
        if "Predict.fun" in available:
            markets = await predict_catalog.resolve(markets)
            _release_discovery_cache(predict_catalog)
            await predict_catalog.close()
            gc.collect()
        if "SX Bet" in available:
            markets = await sx_catalog.resolve(markets)
            _release_discovery_cache(sx_catalog)
            await sx_catalog.close()
            gc.collect()
        if "Myriad" in available:
            markets = await myriad_catalog.resolve(markets)
            _release_discovery_cache(myriad_catalog)
            await myriad_catalog.close()
            gc.collect()

        all_raw_route_candidates, all_route_candidates = _build_route_candidates(markets)
        myriad_metadata = _myriad_settlement_metadata_index(
            markets,
            source_catalogs.get("Myriad", ()),
        )
        all_raw_route_candidates = _enrich_markets_with_myriad_settlement_metadata(
            all_raw_route_candidates,
            myriad_metadata,
        )
        all_route_candidates = _enrich_markets_with_myriad_settlement_metadata(
            all_route_candidates,
            myriad_metadata,
        )
        if app_config.market_horizon_filter_enabled:
            horizon_now = datetime.now(UTC)
            raw_route_candidates = filter_markets_for_launch_horizon(
                all_raw_route_candidates,
                app_config.categories_to_scan,
                sports_horizon_hours=app_config.max_sports_market_horizon_hours,
                crypto_horizon_hours=app_config.max_crypto_market_horizon_hours,
                category_horizon_hours=app_config.max_market_horizon_hours_by_category,
                now=horizon_now,
            )
            route_candidates = filter_markets_for_launch_horizon(
                all_route_candidates,
                app_config.categories_to_scan,
                sports_horizon_hours=app_config.max_sports_market_horizon_hours,
                crypto_horizon_hours=app_config.max_crypto_market_horizon_hours,
                category_horizon_hours=app_config.max_market_horizon_hours_by_category,
                now=horizon_now,
            )
        else:
            raw_route_candidates = all_raw_route_candidates
            route_candidates = all_route_candidates
        category_markets = filter_markets_for_categories(
            route_candidates,
            app_config.categories_to_scan,
            app_config.execution_mode,
        )
        category_markets = _enrich_markets_with_myriad_settlement_metadata(category_markets, myriad_metadata)
        volume_markets = _filter_markets_by_volume(category_markets, app_config)
        volume_markets = _enrich_markets_with_myriad_settlement_metadata(volume_markets, myriad_metadata)
        verified_markets = list(volume_markets)
        if repository is not None:
            await repository.upsert_market_candidates(route_candidates)
            verified_markets = await repository.apply_verified_mappings(verified_markets)
        verified_markets = _enrich_markets_with_myriad_settlement_metadata(verified_markets, myriad_metadata)
        tradable_markets = list(verified_markets)
        if app_config.execution_mode.submits_orders:
            tradable_markets = _verified_active_markets(replace(app_config, markets=tradable_markets))
        tradable_markets = _enrich_markets_with_myriad_settlement_metadata(tradable_markets, myriad_metadata)
        snapshot_config = replace(app_config, markets=tradable_markets)
        missing_routes = tuple(_missing_discovery_routes(snapshot_config))
        myriad_raw, myriad_parsed = myriad_catalog.last_catalog_counts
        predict_raw, predict_parsed = predict_catalog.last_catalog_counts
        sx_raw, sx_parsed = sx_catalog.last_catalog_counts
        stages = {
            "myriad_catalog_raw": myriad_raw,
            "myriad_catalog_parsed": myriad_parsed,
            "predict_catalog_raw": predict_raw,
            "predict_catalog_parsed": predict_parsed,
            "sx_catalog_raw": sx_raw,
            "sx_catalog_parsed": sx_parsed,
            "seed_catalog": myriad_parsed + predict_parsed + sx_parsed,
            "polymarket_catalog": gamma_catalog_size,
            "exact_id_matches": gamma_stats.exact_id_matches,
            "exact_title_matches": gamma_stats.exact_title_matches,
            "structured_sports_matches": getattr(gamma_stats, "structured_sports_matches", 0),
            "semantic_matches": gamma_stats.semantic_matches,
            "cross_venue_candidates": len(all_route_candidates),
            "horizon_accepted": len(route_candidates),
            "category_accepted": len(category_markets),
            "volume_accepted": len(volume_markets),
            "verified_mapping_markets": sum(bool(market.verified_routes) for market in verified_markets),
            "tradable": len(tradable_markets),
        }
        rejection_reasons = dict(gamma_stats.rejection_reasons)
        rejection_reasons["horizon_rejected"] = max(0, len(all_route_candidates) - len(route_candidates))
        rejection_reasons["category_rejected"] = max(0, len(route_candidates) - len(category_markets))
        rejection_reasons["volume_rejected"] = max(0, len(category_markets) - len(volume_markets))
        diagnostics = DiscoveryDiagnostics(
            stages=tuple(stages.items()),
            rejection_reasons=tuple((key, value) for key, value in sorted(rejection_reasons.items()) if value),
        )
        return RouteDiscoverySnapshot(
            enabled_routes=enabled_routes(app_config),
            source_catalogs=source_catalogs,
            raw_route_candidates=tuple(raw_route_candidates),
            route_candidates=tuple(route_candidates),
            category_markets=tuple(category_markets),
            volume_markets=tuple(volume_markets),
            verified_markets=tuple(verified_markets),
            tradable_markets=tuple(tradable_markets),
            missing_routes=missing_routes,
            diagnostics=diagnostics,
            pre_horizon_raw_route_candidates=tuple(all_raw_route_candidates),
            pre_horizon_route_candidates=tuple(all_route_candidates),
        )
    finally:
        for closable_resolver in (gamma, myriad_catalog, predict_catalog, sx_catalog):
            _release_discovery_cache(closable_resolver)
        await asyncio.gather(
            gamma.close(),
            myriad_catalog.close(),
            predict_catalog.close(),
            sx_catalog.close(),
            return_exceptions=True,
        )


def _market_title_row(market: MarketSpec) -> dict[str, Any]:
    return {
        "symbol": market.symbol,
        "target_label": market.target_label,
        "category": market.category,
        "expires_at": market.expires_at.isoformat() if market.expires_at else None,
    }


def _category_counts(markets: Iterable[MarketSpec]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for market in markets:
        category = launch_category(market)
        counts[category] = counts.get(category, 0) + 1
    return dict(sorted(counts.items()))


def _volume_distribution(values: Iterable[float | None]) -> dict[str, float | int | None]:
    reported = sorted(float(value) for value in values if value is not None and float(value) >= 0)
    if not reported:
        return {
            "reported_count": 0,
            "sum_usd": 0.0,
            "min_usd": None,
            "median_usd": None,
            "p75_usd": None,
            "max_usd": None,
        }

    def percentile(fraction: float) -> float:
        position = (len(reported) - 1) * fraction
        lower = int(position)
        upper = min(lower + 1, len(reported) - 1)
        weight = position - lower
        return reported[lower] * (1.0 - weight) + reported[upper] * weight

    return {
        "reported_count": len(reported),
        "sum_usd": round(sum(reported), 6),
        "min_usd": round(reported[0], 6),
        "median_usd": round(percentile(0.5), 6),
        "p75_usd": round(percentile(0.75), 6),
        "max_usd": round(reported[-1], 6),
    }


def _route_leg_volume_usd(market: MarketSpec, route: str, *, second_leg: bool) -> float | None:
    venue = _route_leg_venues(route)[1 if second_leg else 0]
    if venue == "Myriad":
        return market.myriad_volume_usd
    if second_leg:
        return market.predict_fun_volume_usd
    return market.polymarket_volume_usd


def _route_category_volume_coverage(markets: Iterable[MarketSpec], route: str) -> dict[str, Any]:
    unique_pairs: dict[tuple[str, str], MarketSpec] = {}
    for market in markets:
        first_identity = _market_id_for_route_leg(market, route, second_leg=False) or (
            _token_for_route_leg(market, route, second_leg=False) or ""
        )
        second_identity = _market_id_for_route_leg(market, route, second_leg=True) or (
            _token_for_route_leg(market, route, second_leg=True) or ""
        )
        unique_pairs.setdefault((first_identity, second_identity), market)

    grouped: dict[str, list[MarketSpec]] = {}
    for market in unique_pairs.values():
        grouped.setdefault(launch_category(market), []).append(market)

    result: dict[str, Any] = {}
    for category, category_markets in sorted(grouped.items()):
        first_volumes = [
            _route_leg_volume_usd(market, route, second_leg=False)
            for market in category_markets
        ]
        second_volumes = [
            _route_leg_volume_usd(market, route, second_leg=True)
            for market in category_markets
        ]
        minimum_leg_volumes = [
            min(first, second)
            for first, second in zip(first_volumes, second_volumes, strict=True)
            if first is not None and second is not None
        ]
        result[category] = {
            "market_pair_count": len(category_markets),
            "both_legs_reported_count": len(minimum_leg_volumes),
            "first_leg_volume_usd": _volume_distribution(first_volumes),
            "second_leg_volume_usd": _volume_distribution(second_volumes),
            "minimum_leg_volume_usd": _volume_distribution(minimum_leg_volumes),
        }
    return result


def _route_source_venue(route: str) -> str:
    if route in {"polymarket_predict", "predict_myriad", "predict_sx"}:
        return "Predict.fun"
    if route in {"polymarket_sx", "sx_myriad"}:
        return "SX Bet"
    return "Myriad"


def _source_identity_for_market(market: MarketSpec, venue: str) -> tuple[str, str, str] | None:
    if venue == "Predict.fun":
        if market.venue_a_label == "Predict.fun":
            market_id = market.polymarket_market_id or ""
            token = market.polymarket_token_id or ""
        else:
            market_id = market.predict_fun_market_id or ""
            token = market.predict_fun_token_id or ""
        return (venue, market_id, token) if market_id or token else None
    if venue == "SX Bet":
        market_id = market.predict_fun_market_id or ""
        token = market.predict_fun_token_id if market.venue_b_label == "SX Bet" else market.polymarket_token_id
        return (venue, market_id, token) if market_id or token else None
    if venue == "Myriad":
        market_id = market.myriad_market_id or ""
        token = myriad_execution_token_for_route(market, "polymarket_myriad") or ""
        return (venue, market_id, token) if market_id or token else None
    return None


def _source_identity_for_route_market(market: MarketSpec, route: str) -> tuple[str, str, str] | None:
    return _source_identity_for_market(market, _route_source_venue(route))


def _market_identity_payload(market: MarketSpec) -> dict[str, Any]:
    return {
        "rules_fingerprint": market.rules_fingerprint,
        "symbol": market.symbol,
        "target_label": market.target_label,
        "category": launch_category(market),
        "expires_at": market.expires_at.isoformat() if market.expires_at else None,
        "cutoff_at": market.cutoff_at.isoformat() if market.cutoff_at else None,
    }


def _market_audit_identity(market: MarketSpec) -> tuple[str, ...]:
    return (
        market.rules_fingerprint or "",
        market.symbol,
        market.target_label,
        market.condition_id or "",
        market.polymarket_market_id or "",
        market.predict_fun_market_id or "",
        market.myriad_market_id or "",
        market.polymarket_token_id or "",
        market.predict_fun_token_id or "",
    )


def _myriad_settlement_metadata_index(*market_groups: Iterable[MarketSpec]) -> dict[str, tuple[str | None, str | None]]:
    metadata: dict[str, tuple[str | None, str | None]] = {}
    for markets in market_groups:
        for market in markets:
            market_id = market.myriad_market_id
            if not market_id:
                continue
            existing_condition, existing_collateral = metadata.get(market_id, (None, None))
            condition_id = existing_condition or market.myriad_condition_id
            collateral_token = existing_collateral or market.myriad_collateral_token
            if condition_id or collateral_token:
                metadata[market_id] = (condition_id, collateral_token)
    return metadata


def _enrich_market_with_myriad_settlement_metadata(
    market: MarketSpec,
    metadata_by_market_id: dict[str, tuple[str | None, str | None]],
) -> MarketSpec:
    market_id = market.myriad_market_id
    if not market_id:
        return market
    if market.myriad_condition_id and market.myriad_collateral_token:
        return market
    condition_id, collateral_token = metadata_by_market_id.get(market_id, (None, None))
    if not condition_id and not collateral_token:
        return market
    return replace(
        market,
        myriad_condition_id=market.myriad_condition_id or condition_id,
        myriad_collateral_token=market.myriad_collateral_token or collateral_token,
    )


def _enrich_markets_with_myriad_settlement_metadata(
    markets: Iterable[MarketSpec],
    metadata_by_market_id: dict[str, tuple[str | None, str | None]],
) -> list[MarketSpec]:
    return [_enrich_market_with_myriad_settlement_metadata(market, metadata_by_market_id) for market in markets]


def build_route_overlap_report(
    snapshot: RouteDiscoverySnapshot,
    *,
    unmatched_limit: int = 10,
) -> dict[str, Any]:
    routes: dict[str, Any] = {}
    for route in snapshot.enabled_routes:
        pre_horizon_raw = snapshot.pre_horizon_raw_route_candidates or snapshot.raw_route_candidates
        pre_horizon_matched = snapshot.pre_horizon_route_candidates or snapshot.route_candidates
        raw_candidates = [
            market
            for market in pre_horizon_raw
            if _market_supports_route(market, route, require_verified=False)
        ]
        matched = [
            market
            for market in pre_horizon_matched
            if _market_supports_route(market, route, require_verified=False)
        ]
        post_horizon = [
            market
            for market in snapshot.route_candidates
            if _market_supports_route(market, route, require_verified=False)
        ]
        post_volume = [
            market
            for market in snapshot.volume_markets
            if _market_supports_route(market, route, require_verified=False)
        ]
        verified = [
            market
            for market in snapshot.tradable_markets
            if _market_supports_route(market, route, require_verified=True)
        ]
        source_catalog = snapshot.source_catalogs.get(_route_source_venue(route), ())
        matched_source = {
            identity
            for market in matched
            if (identity := _source_identity_for_route_market(market, route)) is not None
        }
        unmatched_rows: list[dict[str, Any]] = []
        for market in source_catalog:
            identity = _source_identity_for_market(market, _route_source_venue(route))
            if identity is None or identity in matched_source:
                continue
            unmatched_rows.append(
                {
                    **_market_title_row(market),
                    "source_market_id": identity[1],
                    "source_token_id": identity[2],
                }
            )
            if len(unmatched_rows) >= unmatched_limit:
                break
        routes[route] = {
            "discovered_candidate_count": len(raw_candidates),
            "engine_safe_matched_count": len(matched),
            "post_horizon_filter_count": len(post_horizon),
            "post_volume_filter_count": len(post_volume),
            "verified_tradable_count": len(verified),
            "category_coverage": {
                "source_catalog": _category_counts(source_catalog),
                "discovered_candidates": _category_counts(raw_candidates),
                "engine_safe_matched": _category_counts(matched),
                "post_horizon_filter": _category_counts(post_horizon),
                "post_volume_filter": _category_counts(post_volume),
                "verified_tradable": _category_counts(verified),
            },
            "volume_coverage": {
                "first_venue": _route_leg_venues(route)[0],
                "second_venue": _route_leg_venues(route)[1],
                "engine_safe_matched": _route_category_volume_coverage(matched, route),
                "post_horizon_filter": _route_category_volume_coverage(post_horizon, route),
                "post_volume_filter": _route_category_volume_coverage(post_volume, route),
                "verified_tradable": _route_category_volume_coverage(verified, route),
            },
            "missing_route": route in snapshot.missing_routes,
            "matched_samples": [_market_title_row(market) for market in matched[: min(5, len(matched))]],
            "unmatched_samples": unmatched_rows,
        }
    return {
        "discovery_snapshot_id": discovery_snapshot_id(snapshot),
        "enabled_routes": snapshot.enabled_routes,
        "missing_routes": snapshot.missing_routes,
        "diagnostics": snapshot.diagnostics.as_dict(),
        "routes": routes,
    }


def discovery_snapshot_id(snapshot: RouteDiscoverySnapshot) -> str:
    def market_key(market: MarketSpec) -> tuple[str, ...]:
        return (
            market.venue_a_label,
            market.venue_b_label,
            market.polymarket_market_id or "",
            market.condition_id or "",
            market.predict_fun_market_id or "",
            market.myriad_market_id or "",
            market.polymarket_token_id,
            market.predict_fun_token_id,
            market.mapping_status.value,
            ",".join(sorted(market.verified_routes)),
        )

    payload = {
        "enabled_routes": snapshot.enabled_routes,
        "source_catalogs": {
            venue: sorted(market_key(market) for market in markets)
            for venue, markets in sorted(snapshot.source_catalogs.items())
        },
        "raw_route_candidates": sorted(market_key(market) for market in snapshot.raw_route_candidates),
        "route_candidates": sorted(market_key(market) for market in snapshot.route_candidates),
        "pre_horizon_raw_route_candidates": sorted(
            market_key(market) for market in snapshot.pre_horizon_raw_route_candidates
        ),
        "pre_horizon_route_candidates": sorted(
            market_key(market) for market in snapshot.pre_horizon_route_candidates
        ),
        "volume_markets": sorted(market_key(market) for market in snapshot.volume_markets),
        "verified_markets": sorted(market_key(market) for market in snapshot.verified_markets),
        "tradable_markets": sorted(market_key(market) for market in snapshot.tradable_markets),
        "missing_routes": snapshot.missing_routes,
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("ascii")).hexdigest()


def _route_leg_venues(route: str) -> tuple[str, str]:
    if route == "polymarket_myriad":
        return "Polymarket", "Myriad"
    if route == "polymarket_predict":
        return "Polymarket", "Predict.fun"
    if route == "predict_myriad":
        return "Predict.fun", "Myriad"
    if route == "predict_sx":
        return "Predict.fun", "SX Bet"
    if route == "polymarket_sx":
        return "Polymarket", "SX Bet"
    if route == "sx_myriad":
        return "SX Bet", "Myriad"
    raise ValueError(f"Unsupported route: {route}")


def _token_for_route_leg(market: MarketSpec, route: str, *, second_leg: bool) -> str | None:
    if not second_leg:
        return first_leg_token_for_route(market, route)
    if route in {"polymarket_myriad", "predict_myriad", "sx_myriad"}:
        return myriad_execution_token_for_route(market, route)
    return second_leg_token_for_route(market, route)


def _side_for_route_leg(market: MarketSpec, route: str, *, second_leg: bool) -> BinarySide | None:
    if not second_leg:
        return first_leg_side_for_route(market, route)
    if route in {"polymarket_myriad", "predict_myriad", "sx_myriad"}:
        return myriad_execution_side_for_route(market, route)
    return second_leg_side_for_route(market, route)


def _market_id_for_route_leg(market: MarketSpec, route: str, *, second_leg: bool) -> str | None:
    venue = _route_leg_venues(route)[1 if second_leg else 0]
    if venue == "Polymarket":
        return market.polymarket_market_id or market.condition_id
    if venue == "Predict.fun":
        if not second_leg and route in {"predict_myriad", "predict_sx"}:
            return market.polymarket_market_id or market.predict_fun_market_id
        return market.predict_fun_market_id
    if venue == "SX Bet":
        return market.predict_fun_market_id
    if venue == "Myriad":
        return market.myriad_market_id
    return None


def _condition_id_for_route_leg(market: MarketSpec, route: str, *, second_leg: bool) -> str | None:
    venue = _route_leg_venues(route)[1 if second_leg else 0]
    if venue == "Polymarket":
        return market.condition_id
    return None


def _register_route_markets(
    markets: Iterable[MarketSpec],
    clients: dict[str, BinaryMarketClient],
) -> None:
    predict_client = clients.get("Predict.fun")
    sx_client = clients.get("SX Bet")
    for market in markets:
        if predict_client is not None:
            register_market = getattr(predict_client, "register_market", None)
            if callable(register_market):
                if market.venue_a_label == "Predict.fun" and market.polymarket_market_id and market.polymarket_token_id:
                    register_market(
                        market.polymarket_token_id,
                        market.polymarket_market_id,
                        market.polymarket_side,
                        market.predict_fun_fee_rate_bps,
                        market.predict_fun_price_precision,
                    )
                if (
                    market.venue_b_label == "Predict.fun"
                    and market.predict_fun_market_id
                    and market.predict_fun_token_id
                ):
                    register_market(
                        market.predict_fun_token_id,
                        market.predict_fun_market_id,
                        market.predict_fun_side,
                        market.predict_fun_fee_rate_bps,
                        market.predict_fun_price_precision,
                    )
        if sx_client is not None:
            register_market = getattr(sx_client, "register_market", None)
            if callable(register_market):
                if market.venue_a_label == "SX Bet" and market.predict_fun_market_id and market.polymarket_token_id:
                    register_market(market.polymarket_token_id, market.predict_fun_market_id, market.polymarket_side)
                if market.venue_b_label == "SX Bet" and market.predict_fun_market_id and market.predict_fun_token_id:
                    register_market(market.predict_fun_token_id, market.predict_fun_market_id, market.predict_fun_side)


def _serialize_constraints(constraints: MarketConstraints | None) -> dict[str, Any] | None:
    if constraints is None:
        return None
    return {
        "fee_rate_bps": constraints.fee_rate_bps,
        "fee_exponent": str(constraints.fee_exponent),
        "tick_size": str(constraints.tick_size),
        "lot_size": str(constraints.lot_size),
        "minimum_notional": str(constraints.minimum_notional),
        "fetched_at": constraints.fetched_at.isoformat(),
    }


def _book_summary(book: Any) -> dict[str, Any]:
    bids = getattr(book, "bids", [])
    asks = getattr(book, "asks", [])
    return {
        "has_bids": bool(bids),
        "has_asks": bool(asks),
        "best_bid": getattr(bids[0], "price", None) if bids else None,
        "best_ask": getattr(asks[0], "price", None) if asks else None,
        "bid_levels": len(bids),
        "ask_levels": len(asks),
        "timestamp": getattr(book, "timestamp", None),
    }


def _serialize_order_preview(preview: OrderPreview) -> dict[str, Any]:
    return {
        "executable": preview.executable,
        "requested_contracts": str(preview.requested_contracts),
        "limit_price": str(preview.limit_price),
        "average_price": str(preview.average_price),
        "notional_usd": str(preview.notional_usd),
        "available_depth_usd": str(preview.available_depth_usd),
        "price_impact_pct": str(preview.price_impact_pct),
        "expected_fee_usd": str(preview.expected_fee_usd),
        "fee_model": preview.fee_quote.model if preview.fee_quote is not None else None,
        "fee_rate_bps": preview.fee_quote.fee_rate_bps if preview.fee_quote is not None else None,
        "fee_exponent": (
            str(preview.fee_quote.fee_exponent) if preview.fee_quote is not None else None
        ),
        "fee_source": preview.fee_quote.source if preview.fee_quote is not None else None,
        "fee_metadata_verified": preview.fee_quote.verified if preview.fee_quote is not None else False,
        "signing_validated": preview.signing_validated,
        "payload_fingerprint": preview.payload_fingerprint,
        "blockers": list(preview.blockers),
    }


async def _collect_leg_preview(
    *,
    client: BinaryMarketClient,
    venue: str,
    token_id: str,
    side: BinarySide,
    condition_id: str | None,
    leg_notional_usd: Decimal,
    required_depth_usd: Decimal,
    max_price_impact: Decimal,
    max_orderbook_age_seconds: float,
) -> tuple[dict[str, Any], tuple[str, ...]]:
    blockers: list[str] = []
    constraints = await client.get_market_constraints(token_id, condition_id)
    if constraints is None:
        blockers.append(f"constraints_unavailable:{venue}")

    samples: list[dict[str, Any]] = []
    last_book: Any = None
    last_limit_price = Decimal(0)
    for sample_index in range(3):
        try:
            book = await client.watch_order_book(token_id)
            last_book = book
            asks = getattr(book, "asks", ())
            status = str(getattr(getattr(book, "status", None), "value", "VALID"))
            sample_blockers: list[str] = []
            if status != "VALID":
                sample_blockers.append(f"orderbook_status:{status.lower()}")
            try:
                if not client.is_order_book_execution_fresh(
                    token_id,
                    book,
                    max_orderbook_age_seconds,
                ):
                    sample_blockers.append("orderbook_stale")
            except Exception:
                sample_blockers.append("orderbook_freshness_check_failed")
            if not asks:
                sample_blockers.append("asks_unavailable")
                best_ask = Decimal(0)
                executable_depth = Decimal(0)
            else:
                best_ask = Decimal(str(asks[0].price))
                last_limit_price = min(Decimal(1), best_ask * (Decimal(1) + max_price_impact))
                executable_depth = sum(
                    (
                        Decimal(str(level.price)) * Decimal(str(level.size))
                        for level in asks
                        if Decimal(str(level.price)) <= last_limit_price
                        and Decimal(str(level.price)) > 0
                        and Decimal(str(level.size)) > 0
                    ),
                    Decimal(0),
                )
                if executable_depth < required_depth_usd:
                    sample_blockers.append("depth_below_required_buffer")
            samples.append(
                {
                    "sample": sample_index + 1,
                    "ok": not sample_blockers,
                    "book": _book_summary(book),
                    "status": status,
                    "executable_depth_usd": str(executable_depth),
                    "required_depth_usd": str(required_depth_usd),
                    "blockers": sample_blockers,
                }
            )
            blockers.extend(f"sample_{sample_index + 1}:{item}:{venue}" for item in sample_blockers)
        except Exception as exc:
            samples.append({"sample": sample_index + 1, "ok": False, "error": str(exc)})
            blockers.append(f"sample_{sample_index + 1}:orderbook_unavailable:{venue}")
        if sample_index < 2:
            await asyncio.sleep(0.15)

    preview: OrderPreview | None = None
    if last_book is not None and bool(last_book.asks) and constraints is not None:
        best_ask = Decimal(str(last_book.asks[0].price))
        contracts = leg_notional_usd / best_ask if best_ask > 0 else Decimal(0)
        try:
            preview = await client.preview_buy(
                token_id,
                side,
                contracts,
                last_limit_price,
                condition_id=condition_id,
                tick_size=str(constraints.tick_size),
            )
            blockers.extend(f"preview:{item}:{venue}" for item in preview.blockers)
            if preview.fee_quote is None or not preview.fee_quote.verified:
                blockers.append(f"preview:fee_metadata_unverified:{venue}")
            # Connectors intentionally skip signing when an earlier preflight gate
            # already rejected the order. Report signing as the root blocker only
            # when the otherwise-valid preview could not produce a signature.
            if not preview.signing_validated and not preview.blockers:
                blockers.append(f"signature_preview_unavailable:{venue}")
            if preview.available_depth_usd < required_depth_usd:
                blockers.append(f"preview_depth_below_required_buffer:{venue}")
        except Exception as exc:
            blockers.append(f"signed_preview_failed:{venue}")
            return (
                {
                    "constraints": _serialize_constraints(constraints),
                    "samples": samples,
                    "preview": None,
                    "preview_error": str(exc),
                },
                tuple(dict.fromkeys(blockers)),
            )
    return (
        {
            "constraints": _serialize_constraints(constraints),
            "samples": samples,
            "preview": _serialize_order_preview(preview) if preview is not None else None,
        },
        tuple(dict.fromkeys(blockers)),
    )


def _paired_preview_neg_risk(
    market: MarketSpec,
    venue: str,
    *,
    second_leg: bool,
) -> bool | None:
    if venue in {"Polymarket", "Predict.fun"}:
        return market.predict_fun_neg_risk if second_leg else market.neg_risk
    return None


def _paired_preview_numeric_valid(preview: OrderPreview) -> bool:
    values = (
        preview.requested_contracts,
        preview.limit_price,
        preview.average_price,
        preview.notional_usd,
        preview.available_depth_usd,
        preview.price_impact_pct,
        preview.expected_fee_usd,
    )
    return bool(
        all(value.is_finite() for value in values)
        and preview.requested_contracts > 0
        and 0 < preview.limit_price <= 1
        and 0 < preview.average_price <= 1
        and preview.notional_usd >= 0
        and preview.available_depth_usd >= 0
        and preview.price_impact_pct >= 0
        and preview.expected_fee_usd >= 0
    )


async def _collect_exact_paired_previews(
    *,
    market: MarketSpec,
    route: str,
    leg_rows: list[dict[str, Any]],
    clients: dict[str, BinaryMarketClient],
    leg_notional_usd: Decimal,
    required_depth_usd: Decimal,
    timeout_seconds: float,
) -> tuple[list[dict[str, Any]], tuple[str, ...]]:
    if len(leg_rows) != 2:
        return leg_rows, ("paired_preview:route_shape_invalid",)
    screening_previews = [row.get("preview") for row in leg_rows]
    if not all(isinstance(preview, dict) for preview in screening_previews):
        return leg_rows, ("paired_preview:screening_preview_missing",)
    try:
        requested_contracts = [
            Decimal(str(cast(dict[str, Any], preview)["requested_contracts"]))
            for preview in screening_previews
        ]
        limit_prices = [
            Decimal(str(cast(dict[str, Any], preview)["limit_price"]))
            for preview in screening_previews
        ]
    except (KeyError, ArithmeticError, ValueError):
        return leg_rows, ("paired_preview:screening_preview_invalid",)
    if (
        not all(value.is_finite() and value > 0 for value in requested_contracts)
        or not all(value.is_finite() and 0 < value <= 1 for value in limit_prices)
    ):
        return leg_rows, ("paired_preview:screening_preview_invalid",)
    # Size both orders to the same payout quantity while respecting each leg's
    # worst-case limit-price budget, not just its current VWAP.
    budget_contracts = [leg_notional_usd / limit_price for limit_price in limit_prices]
    common_contracts = min((*requested_contracts, *budget_contracts))

    async def _preview_leg(index: int) -> OrderPreview:
        row = leg_rows[index]
        venue = str(row.get("venue") or "")
        token_id = str(row.get("token_id") or "")
        client = clients[venue]
        side = _side_for_route_leg(market, route, second_leg=index == 1)
        if side is None:
            raise ValueError(f"missing side for {venue}")
        constraints = row.get("constraints")
        tick_size = None
        if venue == "Polymarket" and isinstance(constraints, dict):
            raw_tick_size = constraints.get("tick_size")
            tick_size = str(raw_tick_size) if raw_tick_size not in (None, "") else None
        return await asyncio.wait_for(
            client.preview_buy(
                token_id,
                side,
                common_contracts,
                limit_prices[index],
                condition_id=str(row.get("condition_id") or "") or None,
                tick_size=tick_size,
                neg_risk=_paired_preview_neg_risk(market, venue, second_leg=index == 1),
            ),
            timeout=timeout_seconds,
        )

    target_clients: dict[str, BinaryMarketClient] = {}
    target_tokens: dict[str, set[str]] = {}
    for row in leg_rows:
        venue = str(row.get("venue") or "")
        token_id = str(row.get("token_id") or "")
        if venue in clients and token_id:
            target_clients[venue] = clients[venue]
            target_tokens.setdefault(venue, set()).add(token_id)
    try:
        for venue, client in target_clients.items():
            sync_targets = getattr(client, "sync_market_data_targets", None)
            if callable(sync_targets):
                sync_targets(target_tokens[venue])
        for venue, client in target_clients.items():
            prime_targets = getattr(client, "prime_market_data_targets", None)
            if callable(prime_targets):
                try:
                    await asyncio.wait_for(prime_targets(), timeout=timeout_seconds)
                except Exception:
                    return leg_rows, (f"paired_preview:prime_failed:{venue}",)
        results = await asyncio.gather(
            *(_preview_leg(index) for index in range(2)),
            return_exceptions=True,
        )
    finally:
        for client in target_clients.values():
            sync_targets = getattr(client, "sync_market_data_targets", None)
            if callable(sync_targets):
                sync_targets(set())
    updated_rows = [dict(row) for row in leg_rows]
    blockers: list[str] = []
    exact_previews: list[OrderPreview] = []
    for index, result in enumerate(results):
        venue = str(leg_rows[index].get("venue") or "unknown")
        updated_rows[index]["screening_preview"] = leg_rows[index].get("preview")
        if isinstance(result, BaseException):
            updated_rows[index]["paired_preview"] = None
            updated_rows[index]["paired_preview_error"] = type(result).__name__
            blockers.append(f"paired_preview:failed:{venue}")
            continue
        preview = result
        exact_previews.append(preview)
        serialized = _serialize_order_preview(preview)
        updated_rows[index]["paired_preview"] = serialized
        updated_rows[index]["preview"] = serialized
        numeric_valid = _paired_preview_numeric_valid(preview)
        if not numeric_valid:
            blockers.append(f"paired_preview:numeric_invalid:{venue}")
        if not preview.executable:
            blockers.extend(f"paired_preview:{item}:{venue}" for item in preview.blockers)
            if not preview.blockers:
                blockers.append(f"paired_preview:not_executable:{venue}")
        if preview.fee_quote is None or not preview.fee_quote.verified:
            blockers.append(f"paired_preview:fee_metadata_unverified:{venue}")
        if not preview.signing_validated or not preview.payload_fingerprint:
            blockers.append(f"paired_preview:signature_unavailable:{venue}")
        if numeric_valid:
            if preview.available_depth_usd < required_depth_usd:
                blockers.append(f"paired_preview:depth_below_required_buffer:{venue}")
            if preview.requested_contracts * preview.limit_price > leg_notional_usd:
                blockers.append(f"paired_preview:leg_notional_above_limit:{venue}")
    if len(exact_previews) == 2 and exact_previews[0].requested_contracts != exact_previews[1].requested_contracts:
        blockers.append("paired_preview:quantity_mismatch")
    return updated_rows, tuple(dict.fromkeys(blockers))


def _route_preview_economics(
    route: str,
    leg_rows: list[dict[str, Any]],
    app_config: AppConfig,
    fixed_chain_cost_usd: Decimal,
) -> tuple[dict[str, Any] | None, tuple[str, ...]]:
    if len(leg_rows) != 2:
        return None, ("route_preview_incomplete",)
    previews = [row.get("preview") for row in leg_rows]
    if not all(isinstance(preview, dict) for preview in previews):
        return None, ("route_preview_incomplete",)
    first, second = previews
    assert isinstance(first, dict)
    assert isinstance(second, dict)
    try:
        first_contracts = Decimal(str(first["requested_contracts"]))
        second_contracts = Decimal(str(second["requested_contracts"]))
        first_average = Decimal(str(first["average_price"]))
        second_average = Decimal(str(second["average_price"]))
        first_fee = Decimal(str(first["expected_fee_usd"]))
        second_fee = Decimal(str(second["expected_fee_usd"]))
        fixed_cost = Decimal(str(fixed_chain_cost_usd))
        numeric_values = (
            first_contracts,
            second_contracts,
            first_average,
            second_average,
            first_fee,
            second_fee,
            fixed_cost,
        )
        if not all(value.is_finite() for value in numeric_values):
            raise ValueError("route preview contains non-finite numeric values")
        if (
            first_contracts <= 0
            or second_contracts <= 0
            or first_average <= 0
            or first_average > 1
            or second_average <= 0
            or second_average > 1
            or first_fee < 0
            or second_fee < 0
            or fixed_cost < 0
        ):
            raise ValueError("route preview contains out-of-range numeric values")
        payout_contracts = min(first_contracts, second_contracts)
        first_fee_per_contract = first_fee / first_contracts
        second_fee_per_contract = second_fee / second_contracts
    except (KeyError, ArithmeticError, ValueError) as exc:
        return {"error": str(exc)}, ("route_economics_unavailable",)
    variable_cost = payout_contracts * (first_fee_per_contract + second_fee_per_contract)
    total_cost = payout_contracts * (first_average + second_average) + variable_cost + fixed_cost
    expected_profit = payout_contracts - total_cost
    net_edge = expected_profit / payout_contracts
    threshold = Decimal(str(app_config.spread_policy.threshold_for(route)))
    minimum_profit = max(
        Decimal(str(app_config.spread_policy.min_expected_profit_usd)),
        variable_cost * Decimal(2),
    )
    if not all(
        value.is_finite()
        for value in (variable_cost, total_cost, expected_profit, net_edge, threshold, minimum_profit)
    ):
        return {"error": "route economics produced non-finite values"}, ("route_economics_unavailable",)
    blockers: list[str] = []
    if net_edge < threshold:
        blockers.append("net_edge_below_dynamic_threshold")
    if expected_profit < minimum_profit:
        blockers.append("expected_profit_below_minimum")
    return (
        {
            "payout_contracts": str(payout_contracts),
            "first_leg_vwap": str(first_average),
            "second_leg_vwap": str(second_average),
            "variable_fee_cost_usd": str(variable_cost),
            "fixed_chain_cost_usd": str(fixed_cost),
            "expected_profit_usd": str(expected_profit),
            "minimum_profit_usd": str(minimum_profit),
            "net_edge": str(net_edge),
            "dynamic_threshold": str(threshold),
        },
        tuple(blockers),
    )


_ECONOMIC_OPENABILITY_BLOCKERS = frozenset(
    {
        "expected_profit_below_minimum",
        "net_edge_below_dynamic_threshold",
    }
)


def _is_economic_openability_blocker(blocker: str) -> bool:
    return blocker.rsplit(":", maxsplit=1)[-1] in _ECONOMIC_OPENABILITY_BLOCKERS


def _recent_shadow_preflight_evidence(
    *,
    route: str,
    app_config: AppConfig,
    runtime_snapshot: dict[str, Any] | None,
    eligible_markets_by_key: dict[str, MarketSpec],
    now: datetime | None = None,
    expected_release_sha: str | None = None,
) -> dict[str, Any]:
    evidence_by_route = (
        (runtime_snapshot or {}).get("latest_shadow_preflight_evidence_by_route") or {}
    )
    raw_evidence = evidence_by_route.get(route) if isinstance(evidence_by_route, dict) else None
    blockers: list[str] = []
    if not isinstance(raw_evidence, dict):
        return {"accepted": False, "blockers": ["recent_shadow_evidence_missing"]}
    evidence = dict(raw_evidence)
    release_sha = str(expected_release_sha or os.getenv("CI_VERIFIED_COMMIT_SHA") or "").strip()
    if not release_sha:
        blockers.append("expected_release_sha_missing")
    if str(evidence.get("release_sha") or "").strip() != release_sha:
        blockers.append("release_sha_mismatch")
    if str(evidence.get("runtime_instance_id") or "") != app_config.runtime_instance_id:
        blockers.append("runtime_instance_mismatch")
    if str(evidence.get("route") or "") != route:
        blockers.append("route_mismatch")
    market_key = str(evidence.get("market_key") or "")
    market = eligible_markets_by_key.get(market_key)
    if market is None:
        blockers.append("market_not_currently_execution_eligible")

    current_time = now or datetime.now(UTC)
    raw_recorded_at = evidence.get("recorded_at") or evidence.get("captured_at")
    evidence_age_seconds: float | None = None
    try:
        recorded_at = datetime.fromisoformat(str(raw_recorded_at).replace("Z", "+00:00"))
        if recorded_at.tzinfo is None:
            recorded_at = recorded_at.replace(tzinfo=UTC)
        raw_age_seconds = (current_time - recorded_at.astimezone(UTC)).total_seconds()
        if raw_age_seconds < -5.0:
            blockers.append("recorded_at_in_future")
        evidence_age_seconds = max(0.0, raw_age_seconds)
    except (TypeError, ValueError):
        blockers.append("recorded_at_invalid")
    if (
        evidence_age_seconds is not None
        and evidence_age_seconds > app_config.shadow_preflight_evidence_ttl_seconds
    ):
        blockers.append("recent_shadow_evidence_expired")

    required_samples = app_config.shadow_preflight_samples
    try:
        completed_samples = int(evidence.get("completed_samples", 0))
        recorded_required_samples = int(evidence.get("required_samples", 0))
    except (TypeError, ValueError):
        completed_samples = 0
        recorded_required_samples = 0
    samples = evidence.get("samples")
    if (
        completed_samples < required_samples
        or recorded_required_samples < required_samples
        or not isinstance(samples, list)
        or len(samples) < required_samples
    ):
        blockers.append("consecutive_signed_samples_incomplete")
        samples = []

    required_depth = (
        Decimal(str(app_config.position_size_usd))
        / Decimal(2)
        * Decimal(str(app_config.spread_policy.depth_buffer))
    )
    threshold = Decimal(str(app_config.spread_policy.threshold_for(route)))
    minimum_profit_floor = Decimal(str(app_config.spread_policy.min_expected_profit_usd))
    for index, sample in enumerate(samples[:required_samples], start=1):
        if not isinstance(sample, dict) or not bool(sample.get("signed_preview_validated")):
            blockers.append(f"sample_{index}:signed_preview_invalid")
            continue
        for leg_name in ("first_leg", "second_leg"):
            leg = sample.get(leg_name)
            if not isinstance(leg, dict):
                blockers.append(f"sample_{index}:{leg_name}_missing")
                continue
            if not bool(leg.get("fee_verified")):
                blockers.append(f"sample_{index}:{leg_name}_fee_unverified")
            try:
                depth = Decimal(str(leg.get("executable_depth_usd")))
                preview_depth = Decimal(str(leg.get("signed_preview_depth_usd")))
            except (ArithmeticError, ValueError):
                blockers.append(f"sample_{index}:{leg_name}_depth_invalid")
                continue
            if not depth.is_finite() or not preview_depth.is_finite():
                blockers.append(f"sample_{index}:{leg_name}_depth_invalid")
                continue
            if depth < required_depth or preview_depth < required_depth:
                blockers.append(f"sample_{index}:{leg_name}_depth_below_required_buffer")
        economics = sample.get("economics")
        if not isinstance(economics, dict):
            blockers.append(f"sample_{index}:economics_missing")
            continue
        try:
            expected_profit = Decimal(str(economics.get("expected_profit_usd")))
            minimum_profit = Decimal(str(economics.get("minimum_profit_usd")))
            net_edge = Decimal(str(economics.get("net_edge")))
            chain_cost = Decimal(str(economics.get("fixed_chain_cost_usd")))
        except (ArithmeticError, ValueError):
            blockers.append(f"sample_{index}:economics_invalid")
            continue
        if not all(
            value.is_finite()
            for value in (expected_profit, minimum_profit, net_edge, chain_cost)
        ) or chain_cost < 0:
            blockers.append(f"sample_{index}:economics_invalid")
            continue
        if expected_profit < max(minimum_profit_floor, minimum_profit):
            blockers.append(f"sample_{index}:expected_profit_below_minimum")
        if net_edge < threshold:
            blockers.append(f"sample_{index}:net_edge_below_dynamic_threshold")
        if app_config.spread_policy.require_live_gas_estimate and chain_cost <= 0:
            blockers.append(f"sample_{index}:live_chain_cost_unavailable")

    unique_blockers = list(dict.fromkeys(blockers))
    technical_blockers = [
        blocker for blocker in unique_blockers if not _is_economic_openability_blocker(blocker)
    ]
    economic_blockers = [
        blocker for blocker in unique_blockers if _is_economic_openability_blocker(blocker)
    ]
    return {
        "accepted": not unique_blockers,
        "technical_accepted": not unique_blockers,
        "mechanical_preflight_accepted": not technical_blockers,
        "economically_openable": not unique_blockers,
        "blockers": unique_blockers,
        "technical_blockers": technical_blockers,
        "economic_blockers": economic_blockers,
        "age_seconds": evidence_age_seconds,
        "ttl_seconds": app_config.shadow_preflight_evidence_ttl_seconds,
        "market_key": market_key,
        "market": _market_identity_payload(market) if market is not None else None,
        "release_sha": evidence.get("release_sha"),
        "runtime_instance_id": evidence.get("runtime_instance_id"),
        "completed_samples": completed_samples,
        "required_samples": required_samples,
        "evidence": evidence,
    }
async def collect_venue_balance_audit(
    app_config: AppConfig,
    runtime_snapshot: dict[str, Any] | None,
) -> dict[str, Any]:
    clients: dict[str, BinaryMarketClient] = {"Polymarket": PolymarketClobClient(app_config.polymarket)}
    if predict_enabled(app_config):
        clients["Predict.fun"] = PredictFunApiClient(app_config.predict_fun)
    if sx_enabled(app_config):
        clients["SX Bet"] = create_sx_bet_client(app_config.sx_bet)
    if myriad_enabled(app_config):
        clients["Myriad"] = MyriadClient(app_config.myriad_markets)
    try:
        report: dict[str, Any] = {}
        for venue, client in clients.items():
            runtime_audit = venue_runtime_audit(runtime_snapshot, venue)
            try:
                connector_balance = await client.get_cash_balance()
                direct_balance: float | None = None
                third_party_balance: float | None = None
                extra: dict[str, Any] = {}
                if venue == "Predict.fun":
                    details = await client.get_cash_balance_details()  # type: ignore[attr-defined]
                    direct_balance = float(details["balance"])
                    extra = {
                        "wallet_address": details["wallet_address"],
                        "collateral_token_address": details["collateral_token_address"],
                    }
                elif venue == "SX Bet":
                    details = await client.get_cash_balance_details()  # type: ignore[attr-defined]
                    direct_balance = float(details["balance"])
                    explorer = await asyncio.to_thread(
                        _sx_explorer_balance,
                        str(details["wallet_address"]),
                        str(details["base_token_address"]),
                    )
                    if explorer.get("ok"):
                        third_party_balance = float(explorer["balance_usd"])
                    extra = {
                        "wallet_address": details["wallet_address"],
                        "base_token_address": details["base_token_address"],
                        "explorer_balance": explorer,
                    }
                elif venue == "Myriad":
                    balances = await client.get_balances()
                    symbol = app_config.myriad_markets.collateral_symbol
                    direct = balances.get(symbol)
                    direct_balance = float(direct) if direct is not None else None
                    extra = {"configured_collateral_symbol": symbol}
                gate = venue_balance_gate(
                    venue=venue,
                    minimum_balance_usd=app_config.min_venue_balance_usd,
                    connector_balance=connector_balance,
                    direct_balance=direct_balance,
                    third_party_balance=third_party_balance,
                    third_party_balance_label="explorer" if venue == "SX Bet" else None,
                    runtime_audit=runtime_audit,
                )
                report[venue] = {
                    "connector_visible_balance_usd": connector_balance,
                    "effective_balance": effective_balance_payload(
                        venue,
                        connector_balance,
                        direct_balance=direct_balance,
                        runtime_audit=runtime_audit,
                    ),
                    "canary_gate": gate,
                    **extra,
                }
            except Exception as exc:
                report[venue] = {
                    "balance_probe_error": str(exc),
                    "canary_gate": {
                        "venue": venue,
                        "passed": False,
                        "minimum_balance_usd": app_config.min_venue_balance_usd,
                        "blocking_reasons": [_balance_probe_blocker(venue)],
                    },
                }
        return report
    finally:
        await asyncio.gather(*(client.close() for client in clients.values()), return_exceptions=True)


async def collect_all_market_audit(
    app_config: AppConfig,
    snapshot: RouteDiscoverySnapshot,
    runtime_snapshot: dict[str, Any] | None,
    *,
    orderbook_timeout_seconds: float = 15.0,
    max_preview_concurrency: int | None = None,
    max_preview_concurrency_per_venue: int | None = None,
    max_market_data_targets_per_window: int = 100,
) -> dict[str, Any]:
    venue_balances = await collect_venue_balance_audit(app_config, runtime_snapshot)
    clients: dict[str, BinaryMarketClient] = {"Polymarket": PolymarketClobClient(app_config.polymarket)}
    if predict_enabled(app_config):
        clients["Predict.fun"] = PredictFunApiClient(app_config.predict_fun)
    if sx_enabled(app_config):
        clients["SX Bet"] = create_sx_bet_client(app_config.sx_bet)
    if myriad_enabled(app_config):
        clients["Myriad"] = MyriadClient(app_config.myriad_markets)
    chain_cost_estimator = LiveChainCostEstimator(app_config)
    chain_cost_quotes: dict[str, dict[str, Any]] = {}
    chain_costs: dict[str, Decimal] = {}
    for route in snapshot.enabled_routes:
        try:
            quote = await chain_cost_estimator.estimate(
                route,
                require_live=app_config.spread_policy.require_live_gas_estimate,
            )
            chain_costs[route] = quote.reserved_cost_usd
            chain_cost_quotes[route] = quote.as_dict()
        except LiveChainCostUnavailable as exc:
            chain_cost_quotes[route] = {"route": route, "live": False, "error": str(exc)}
    _register_route_markets(snapshot.volume_markets, clients)
    preview_cache: dict[tuple[str, str, str | None], tuple[dict[str, Any], tuple[str, ...]]] = {}
    preview_requests: dict[
        tuple[str, str, str | None],
        tuple[BinaryMarketClient, str, str, BinarySide, str | None],
    ] = {}
    resolved_global_concurrency = max(
        1,
        max_preview_concurrency
        if max_preview_concurrency is not None
        else min(32, max(8, app_config.max_concurrent_market_evaluations)),
    )
    resolved_per_venue_concurrency = max(
        1,
        min(
            resolved_global_concurrency,
            max_preview_concurrency_per_venue
            if max_preview_concurrency_per_venue is not None
            else max(4, resolved_global_concurrency // 2),
        ),
    )
    global_preview_semaphore = asyncio.Semaphore(resolved_global_concurrency)
    venue_preview_semaphores = {
        venue: asyncio.Semaphore(resolved_per_venue_concurrency) for venue in clients
    }
    verified_market_lookup = {_market_audit_identity(market): market for market in snapshot.verified_markets}
    tradable_market_lookup = {_market_audit_identity(market): market for market in snapshot.tradable_markets}
    myriad_metadata = _myriad_settlement_metadata_index(
        snapshot.source_catalogs.get("Myriad", ()),
        snapshot.volume_markets,
        snapshot.verified_markets,
        snapshot.tradable_markets,
    )

    def _resolved_market(market: MarketSpec) -> MarketSpec:
        resolved = tradable_market_lookup.get(
            _market_audit_identity(market),
            verified_market_lookup.get(_market_audit_identity(market), market),
        )
        return _enrich_market_with_myriad_settlement_metadata(resolved, myriad_metadata)

    async def _collect_bounded_preview(
        *,
        client: BinaryMarketClient,
        venue: str,
        token_id: str,
        side: BinarySide,
        condition_id: str | None,
    ) -> tuple[dict[str, Any], tuple[str, ...]]:
        try:
            async with global_preview_semaphore:
                async with venue_preview_semaphores[venue]:
                    return await asyncio.wait_for(
                        _collect_leg_preview(
                            client=client,
                            venue=venue,
                            token_id=token_id,
                            side=side,
                            condition_id=condition_id,
                            leg_notional_usd=Decimal(str(app_config.position_size_usd)) / Decimal(2),
                            required_depth_usd=(
                                Decimal(str(app_config.position_size_usd))
                                / Decimal(2)
                                * Decimal(str(app_config.spread_policy.depth_buffer))
                            ),
                            max_price_impact=Decimal(str(app_config.max_production_price_impact)),
                            max_orderbook_age_seconds=app_config.max_orderbook_age_seconds,
                        ),
                        timeout=orderbook_timeout_seconds * 4,
                    )
        except Exception as exc:
            return (
                {"samples": [], "preview": None, "error": str(exc)},
                (f"preview_failed:{venue}",),
            )

    try:
        # Schedule only execution-eligible mappings. Candidate/stale mappings remain
        # visible in the report but cannot become openable and must not be signed.
        for route in snapshot.enabled_routes:
            for source_market in snapshot.volume_markets:
                if not _market_supports_route(source_market, route, require_verified=False):
                    continue
                market = _resolved_market(source_market)
                if not is_live_mapping_eligible(market, ExecutionMode.CANARY, route):
                    continue
                first_venue, second_venue = _route_leg_venues(route)
                for second_leg, venue in ((False, first_venue), (True, second_venue)):
                    token = _token_for_route_leg(market, route, second_leg=second_leg) or ""
                    side = _side_for_route_leg(market, route, second_leg=second_leg)
                    condition_id = _condition_id_for_route_leg(market, route, second_leg=second_leg)
                    if not token or side is None or venue not in clients:
                        continue
                    key = (venue, token, condition_id)
                    if key not in preview_requests:
                        preview_requests[key] = (
                            clients[venue],
                            venue,
                            token,
                            side,
                            condition_id,
                        )

        worker_count = min(resolved_global_concurrency, len(preview_requests))
        preview_items = list(preview_requests.items())
        # Priming and preview execution have different resource limits. Venue
        # clients already batch market-data bootstrap safely (Predict accepts up
        # to 100 IDs per request), while the semaphores below keep signed preview
        # work at the configured concurrency. Tying both limits together made a
        # full Predict audit take more than 30 minutes despite low CPU usage.
        target_window_size = max(1, max_market_data_targets_per_window)
        try:
            for start in range(0, len(preview_items), target_window_size):
                window = preview_items[start : start + target_window_size]
                window_targets: dict[str, set[str]] = {}
                for _, request in window:
                    window_targets.setdefault(request[1], set()).add(request[2])
                for venue, client in clients.items():
                    sync_targets = getattr(client, "sync_market_data_targets", None)
                    if callable(sync_targets):
                        sync_targets(window_targets.get(venue, set()))

                async def _prime_venue(venue: str) -> None:
                    prime_targets = getattr(clients[venue], "prime_market_data_targets", None)
                    if not callable(prime_targets):
                        return
                    try:
                        await asyncio.wait_for(
                            prime_targets(),
                            timeout=orderbook_timeout_seconds * 2,
                        )
                    except Exception as exc:
                        LOGGER.warning(
                            "market_data_target_prime_failed",
                            extra={"_venue": venue, "_reason": str(exc)},
                        )

                await asyncio.gather(*(_prime_venue(venue) for venue in window_targets))
                results = await asyncio.gather(
                    *(
                        _collect_bounded_preview(
                            client=request[0],
                            venue=request[1],
                            token_id=request[2],
                            side=request[3],
                            condition_id=request[4],
                        )
                        for _, request in window
                    )
                )
                for (key, _), result in zip(window, results, strict=True):
                    preview_cache[key] = result
        finally:
            for client in clients.values():
                sync_targets = getattr(client, "sync_market_data_targets", None)
                if callable(sync_targets):
                    sync_targets(set())

        rows: list[dict[str, Any]] = []
        route_summary: dict[str, dict[str, Any]] = {}
        for route in snapshot.enabled_routes:
            route_markets = [
                market
                for market in snapshot.volume_markets
                if _market_supports_route(market, route, require_verified=False)
            ]
            resolved_route_markets: list[MarketSpec] = []
            technical_openable_count = 0
            economically_openable_count = 0
            canary_openable_count = 0
            technical_blocker_counts: dict[str, int] = {}
            economic_blocker_counts: dict[str, int] = {}
            canary_blocker_counts: dict[str, int] = {}
            category_summary: dict[str, dict[str, int]] = {}
            first_venue, second_venue = _route_leg_venues(route)
            route_canary_gate_blockers: list[str] = []
            for venue in (first_venue, second_venue):
                gate = venue_balances.get(venue, {}).get("canary_gate", {})
                if not gate.get("passed", False):
                    route_canary_gate_blockers.append(f"venue_gate_failed:{venue}")
            if not app_config.live_trading_confirmed:
                route_canary_gate_blockers.append("live_trading_confirmation_missing")
            route_canary_gate_blockers = list(dict.fromkeys(route_canary_gate_blockers))
            for source_market in route_markets:
                market = _resolved_market(source_market)
                resolved_route_markets.append(market)
                market_key = position_key(market)
                category = launch_category(market)
                category_state = category_summary.setdefault(
                    category,
                    {
                        "market_count": 0,
                        "verified_count": 0,
                        "technical_openable_count": 0,
                        "economically_openable_count": 0,
                        "canary_openable_count": 0,
                        "openable_count": 0,
                        "recent_technical_evidence_count": 0,
                    },
                )
                category_state["market_count"] += 1
                technical_blockers: list[str] = []
                execution_eligible = is_live_mapping_eligible(market, ExecutionMode.CANARY, route)
                if execution_eligible:
                    category_state["verified_count"] += 1
                if not execution_eligible:
                    technical_blockers.append("route_not_execution_eligible")
                if not app_config.spread_policy.has_route_calibration(route):
                    technical_blockers.append("adverse_move_calibration_missing")
                if app_config.spread_policy.require_live_gas_estimate and route not in chain_costs:
                    technical_blockers.append("live_chain_cost_unavailable")
                first_token = _token_for_route_leg(market, route, second_leg=False) or ""
                second_token = _token_for_route_leg(market, route, second_leg=True) or ""
                first_market_id = _market_id_for_route_leg(market, route, second_leg=False)
                second_market_id = _market_id_for_route_leg(market, route, second_leg=True)
                if not first_token:
                    technical_blockers.append(f"missing_token:{first_venue}")
                if not second_token:
                    technical_blockers.append(f"missing_token:{second_venue}")
                if not first_market_id:
                    technical_blockers.append(f"missing_market_id:{first_venue}")
                if not second_market_id:
                    technical_blockers.append(f"missing_market_id:{second_venue}")

                leg_rows: list[dict[str, Any]] = []
                for second_leg, venue, token in (
                    (False, first_venue, first_token),
                    (True, second_venue, second_token),
                ):
                    market_id = _market_id_for_route_leg(market, route, second_leg=second_leg)
                    side = _side_for_route_leg(market, route, second_leg=second_leg)
                    condition_id = _condition_id_for_route_leg(market, route, second_leg=second_leg)
                    leg_state: dict[str, Any]
                    leg_blockers: tuple[str, ...]
                    if not execution_eligible:
                        leg_state = {
                            "venue": venue,
                            "market_id": market_id,
                            "token_id": token,
                            "side": side.value if side is not None else None,
                            "condition_id": condition_id,
                            "samples": [],
                            "preview": None,
                        }
                        leg_blockers = ()
                    elif not token or venue not in clients:
                        leg_state = {
                            "venue": venue,
                            "market_id": market_id,
                            "token_id": token,
                            "side": side.value if side is not None else None,
                            "condition_id": condition_id,
                            "samples": [],
                            "preview": None,
                        }
                        leg_blockers = (f"orderbook_unavailable:{venue}",)
                    elif side is None:
                        leg_state = {
                            "venue": venue,
                            "market_id": market_id,
                            "token_id": token,
                            "side": None,
                            "condition_id": condition_id,
                            "samples": [],
                            "preview": None,
                        }
                        leg_blockers = (f"missing_side:{venue}",)
                    else:
                        key = (venue, token, condition_id)
                        if key not in preview_cache:
                            preview_cache[key] = await _collect_bounded_preview(
                                client=clients[venue],
                                venue=venue,
                                token_id=token,
                                side=side,
                                condition_id=condition_id,
                            )
                        cached_state, leg_blockers = preview_cache[key]
                        leg_state = {
                            "venue": venue,
                            "market_id": market_id,
                            "token_id": token,
                            "side": side.value,
                            "condition_id": condition_id,
                            **cached_state,
                        }
                    technical_blockers.extend(leg_blockers)
                    leg_rows.append(leg_state)
                fixed_chain_cost = chain_costs.get(
                    route,
                    Decimal(str(app_config.spread_policy.fixed_chain_cost_for(route))),
                )
                route_economics, economics_blockers = _route_preview_economics(
                    route,
                    leg_rows,
                    app_config,
                    fixed_chain_cost,
                )
                economic_blockers = [
                    blocker
                    for blocker in economics_blockers
                    if _is_economic_openability_blocker(blocker)
                ]
                technical_blockers.extend(
                    blocker
                    for blocker in economics_blockers
                    if not _is_economic_openability_blocker(blocker)
                )
                economic_blockers = list(dict.fromkeys(economic_blockers))
                paired_preview_status = "not_attempted"
                paired_preview_validated = False
                # Screening previews are independently sized and can look
                # unprofitable even when the common-quantity pair is valid.
                # Mechanical screening gates decide whether exact previewing is
                # safe; exact paired economics then becomes authoritative.
                if not technical_blockers:
                    paired_preview_status = "rejected"
                    leg_rows, paired_preview_blockers = await _collect_exact_paired_previews(
                        market=market,
                        route=route,
                        leg_rows=leg_rows,
                        clients=clients,
                        leg_notional_usd=Decimal(str(app_config.position_size_usd)) / Decimal(2),
                        required_depth_usd=(
                            Decimal(str(app_config.position_size_usd))
                            / Decimal(2)
                            * Decimal(str(app_config.spread_policy.depth_buffer))
                        ),
                        timeout_seconds=orderbook_timeout_seconds,
                    )
                    technical_blockers.extend(paired_preview_blockers)
                    if not paired_preview_blockers:
                        paired_preview_status = "validated"
                        paired_preview_validated = True
                        route_economics, economics_blockers = _route_preview_economics(
                            route,
                            leg_rows,
                            app_config,
                            fixed_chain_cost,
                        )
                        economic_blockers = [
                            blocker
                            for blocker in economics_blockers
                            if _is_economic_openability_blocker(blocker)
                        ]
                        technical_blockers.extend(
                            blocker
                            for blocker in economics_blockers
                            if not _is_economic_openability_blocker(blocker)
                        )
                        economic_blockers = list(dict.fromkeys(economic_blockers))
                    else:
                        economic_blockers = []
                # Technical openability is the complete executable preflight
                # without operator/runtime gates. It includes current route
                # economics; a mechanically valid but loss-making order is not
                # technically openable for funded arbitrage.
                technical_blockers = list(
                    dict.fromkeys((*technical_blockers, *economic_blockers))
                )
                canary_blockers = list(
                    dict.fromkeys((*technical_blockers, *route_canary_gate_blockers))
                )
                technical_openable = not technical_blockers
                # Retained as a compatibility alias for v3 report consumers.
                economically_openable = technical_openable
                canary_openable = not canary_blockers
                if technical_openable:
                    technical_openable_count += 1
                    category_state["technical_openable_count"] += 1
                else:
                    for blocker in technical_blockers:
                        technical_blocker_counts[blocker] = technical_blocker_counts.get(blocker, 0) + 1
                if economically_openable:
                    economically_openable_count += 1
                    category_state["economically_openable_count"] += 1
                else:
                    for blocker in economic_blockers:
                        economic_blocker_counts[blocker] = economic_blocker_counts.get(blocker, 0) + 1
                if canary_openable:
                    canary_openable_count += 1
                    category_state["canary_openable_count"] += 1
                    category_state["openable_count"] += 1
                else:
                    for blocker in canary_blockers:
                        canary_blocker_counts[blocker] = canary_blocker_counts.get(blocker, 0) + 1
                rows.append(
                    {
                        "route": route,
                        "market_key": market_key,
                        "canonical_identity": _market_identity_payload(market),
                        "mapping_status": market.mapping_status.value,
                        "verified_routes": sorted(market.verified_routes),
                        "first_leg": leg_rows[0],
                        "second_leg": leg_rows[1],
                        "route_economics": route_economics,
                        "route_economics_basis": (
                            "exact_paired_preview" if paired_preview_validated else "screening_preview"
                        ),
                        "paired_preview_status": paired_preview_status,
                        "paired_preview_validated": paired_preview_validated,
                        "technical_preview_feasible": technical_openable,
                        "economically_openable": economically_openable,
                        "canary_preview_feasible": canary_openable,
                        "technical_preview_blockers": technical_blockers,
                        "economic_preview_blockers": economic_blockers,
                        "canary_preview_blockers": canary_blockers,
                        # Backward-compatible fail-closed aliases.
                        "preview_feasible": canary_openable,
                        "preview_blockers": canary_blockers,
                    }
                )
            current_technical_openable_count = technical_openable_count
            current_economically_openable_count = economically_openable_count
            current_canary_openable_count = canary_openable_count
            eligible_markets_by_key = {
                position_key(market): market
                for market in resolved_route_markets
                if is_live_mapping_eligible(market, ExecutionMode.CANARY, route)
            }
            recent_evidence = _recent_shadow_preflight_evidence(
                route=route,
                app_config=app_config,
                runtime_snapshot=runtime_snapshot,
                eligible_markets_by_key=eligible_markets_by_key,
            )
            recent_evidence_market_key = str(recent_evidence.get("market_key") or "")
            recent_technical_evidence_count = 0
            recent_canary_evidence_count = 0
            recent_technical_accepted = bool(recent_evidence.get("accepted"))
            if recent_technical_accepted and recent_evidence_market_key:
                evidence_market = eligible_markets_by_key.get(recent_evidence_market_key)
                if evidence_market is not None:
                    category_state = category_summary[launch_category(evidence_market)]
                    recent_technical_evidence_count = 1
                    category_state["recent_technical_evidence_count"] += 1
            technical_blocker_samples = [
                {
                    "blocker": blocker,
                    "count": count,
                }
                for blocker, count in sorted(
                    technical_blocker_counts.items(),
                    key=lambda item: (-item[1], item[0]),
                )[:10]
            ]
            canary_blocker_samples = [
                {
                    "blocker": blocker,
                    "count": count,
                }
                for blocker, count in sorted(
                    canary_blocker_counts.items(),
                    key=lambda item: (-item[1], item[0]),
                )[:10]
            ]
            economic_blocker_samples = [
                {
                    "blocker": blocker,
                    "count": count,
                }
                for blocker, count in sorted(
                    economic_blocker_counts.items(),
                    key=lambda item: (-item[1], item[0]),
                )[:10]
            ]
            route_summary[route] = {
                "market_count": len(route_markets),
                "verified_count": sum(route in market.verified_routes for market in resolved_route_markets),
                "current_technical_openable_count": current_technical_openable_count,
                "technical_openable_count": technical_openable_count,
                "recent_technical_evidence_count": recent_technical_evidence_count,
                "current_economically_openable_count": current_economically_openable_count,
                "economically_openable_count": economically_openable_count,
                "current_canary_openable_count": current_canary_openable_count,
                "canary_openable_count": canary_openable_count,
                "recent_canary_evidence_count": recent_canary_evidence_count,
                # Backward-compatible fail-closed alias.
                "openable_count": canary_openable_count,
                "recent_shadow_preflight_evidence": recent_evidence,
                "category_summary": dict(sorted(category_summary.items())),
                "technical_blocker_samples": technical_blocker_samples,
                "economic_blocker_samples": economic_blocker_samples,
                "canary_blocker_samples": canary_blocker_samples,
                "blocker_samples": canary_blocker_samples,
            }
        return {
            "discovery_snapshot_id": discovery_snapshot_id(snapshot),
            "enabled_routes": snapshot.enabled_routes,
            "openability_model": "technical_and_canary_v4",
            "preview_policy": {
                "global_concurrency": resolved_global_concurrency,
                "per_venue_concurrency": resolved_per_venue_concurrency,
                "target_window_size": target_window_size,
                "worker_count": worker_count,
                "unique_preview_count": len(preview_requests),
                "consecutive_samples_required": 3,
                "exact_paired_preview_required_for_openable": True,
            },
            "route_summary": route_summary,
            "chain_cost_quotes": chain_cost_quotes,
            "venue_balances": venue_balances,
            "markets": rows,
        }
    finally:
        await asyncio.gather(
            *(client.close() for client in clients.values()),
            chain_cost_estimator.close(),
            return_exceptions=True,
        )


def live_window_has_real_order_evidence(report: dict[str, Any], route: str | None = None) -> bool:
    continuity = report.get("monitoring_continuity")
    if isinstance(continuity, dict) and not bool(continuity.get("passed", False)):
        return False
    if report.get("final_database_snapshot_ok") is False:
        return False
    if report.get("window_completed") is False:
        return False
    if route is not None:
        route_evidence = report.get("route_evidence")
        if not isinstance(route_evidence, dict):
            return False
        evidence = route_evidence.get(route)
        return isinstance(evidence, dict) and bool(evidence.get("has_live_evidence"))
    if bool(report.get("observed_real_fill_or_open_position")):
        return True
    try:
        if int(report.get("real_recent_fill_count", 0)) > 0:
            return True
    except (TypeError, ValueError):
        pass
    try:
        if int(report.get("real_open_position_count", 0)) > 0:
            return True
    except (TypeError, ValueError):
        pass
    return False


def _balance_probe_blocker(venue: str) -> str:
    mapping = {
        "Polymarket": "polymarket_balance_probe_failed",
        "Predict.fun": "predict_fun_balance_probe_failed",
        "SX Bet": "sx_bet_balance_probe_failed",
        "Myriad": "myriad_balance_probe_failed",
    }
    return mapping.get(venue, f"{normalize_text(venue).replace('.', '_')}_balance_probe_failed")
