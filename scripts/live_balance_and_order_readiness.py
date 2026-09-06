from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sys
from dataclasses import replace
from decimal import Decimal
from typing import Any, TextIO
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

from eth_account import Account

from arbitrage_engine.config import AppConfig, load_config, load_operator_env
from arbitrage_engine.connectors.myriad import ERC20_BALANCE_ABI, MyriadClient, _outcome_id
from arbitrage_engine.connectors.polymarket import PolymarketClobClient
from arbitrage_engine.connectors.predict_fun import PredictFunApiClient
from arbitrage_engine.connectors.sx_bet import create_sx_bet_client
from arbitrage_engine.database import ProductionRepository
from arbitrage_engine.market_mapping import route_key
from arbitrage_engine.models import BinarySide, MappingStatus
from arbitrage_engine.predict_fun_discovery import PredictFunMarketResolver
from arbitrage_engine.production_audit import (
    ROUTE_NAMES,
    RouteDiscoverySnapshot,
    build_route_overlap_report,
    collect_all_market_audit,
    funded_routes,
    require_operator_catalog_context,
    resolve_route_discovery_snapshot,
)
from arbitrage_engine.production_audit import (
    enabled_routes as discovery_routes,
)
from arbitrage_engine.redaction import redact_signing_material

SX_EXPLORER_API_URL = "https://explorerl2.sx.technology/api"


def _json_default(value: Any) -> str:
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _redacted_signed_payload(payload: Any, *, detached_signature: str | None = None) -> dict[str, Any]:
    canonical_payload = json.dumps(
        {"payload": payload, "detached_signature": detached_signature},
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    ).encode("utf-8")
    embedded_signature = payload.get("signature") if isinstance(payload, dict) else None
    return {
        "signed_preview_created": True,
        "signed_payload_sha256": hashlib.sha256(canonical_payload).hexdigest(),
        "signature_present": bool(detached_signature or embedded_signature),
    }


def _write_json_report(payload: dict[str, Any], stream: TextIO) -> None:
    json.dump(payload, stream, indent=2, default=_json_default)
    stream.write("\n")


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


def _effective_balance_payload(
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


async def _load_runtime_audit(
    app_config: Any,
    *,
    managed_routes: tuple[str, ...] | None = None,
) -> dict[str, Any] | None:
    database_url = getattr(app_config, "database_url", None)
    if not database_url:
        return None
    repository = ProductionRepository(
        database_url,
        runtime_instance_id=getattr(app_config, "runtime_instance_id", "global"),
        enabled_routes=managed_routes or discovery_routes(app_config),
    )
    try:
        if not await repository.ping():
            return None
        await repository.configure_managed_reconciliation_venues(funded_routes(app_config))
        return await repository.runtime_audit_snapshot()
    finally:
        await repository.close()


async def _load_mapping_coverage(database_url: str | None, enabled_routes: tuple[str, ...]) -> dict[str, Any]:
    coverage = {route: {"has_verified": False, "verified_count": 0} for route in enabled_routes}
    if not database_url:
        return {"database_reachable": False, "enabled_routes": coverage}
    repository = ProductionRepository(database_url, enabled_routes=enabled_routes)
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


def _venue_runtime_audit(snapshot: dict[str, Any] | None, venue: str) -> dict[str, Any]:
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
        "note": (
            "Durable DB state is shown here. When the live bot is persisting runtime balance state, "
            "process-local balance cache, optimistic debits, and capital reservations are included too."
        ),
    }


def _enabled_routes(app_config: Any) -> tuple[str, ...]:
    route_config = app_config.funded_routes
    if route_config is None:
        route_config = app_config.routes
    routes: list[str] = []
    if route_config.polymarket_myriad:
        routes.append("polymarket_myriad")
    if route_config.polymarket_predict:
        routes.append("polymarket_predict")
    if route_config.predict_myriad:
        routes.append("predict_myriad")
    if getattr(route_config, "predict_sx", False):
        routes.append("predict_sx")
    if getattr(route_config, "polymarket_sx", False):
        routes.append("polymarket_sx")
    if getattr(route_config, "sx_myriad", False):
        routes.append("sx_myriad")
    return tuple(routes)


def _select_audit_routes(
    configured_routes: tuple[str, ...],
    requested_routes: list[str] | None,
) -> tuple[str, ...]:
    if not requested_routes:
        return configured_routes
    requested = tuple(dict.fromkeys(requested_routes))
    disabled = tuple(route for route in requested if route not in configured_routes)
    if disabled:
        raise ValueError(f"routes are not enabled in the selected config: {', '.join(disabled)}")
    return requested


def _route_venues(routes: tuple[str, ...]) -> set[str]:
    venues_by_route = {
        "polymarket_myriad": ("Polymarket", "Myriad"),
        "polymarket_predict": ("Polymarket", "Predict.fun"),
        "predict_myriad": ("Predict.fun", "Myriad"),
        "predict_sx": ("Predict.fun", "SX Bet"),
        "polymarket_sx": ("Polymarket", "SX Bet"),
        "sx_myriad": ("SX Bet", "Myriad"),
    }
    return {venue for route in routes for venue in venues_by_route[route]}


def _scope_app_config(app_config: AppConfig, routes: tuple[str, ...]) -> AppConfig:
    route_flags = {route: route in routes for route in ROUTE_NAMES}
    scoped_routes = replace(app_config.routes, **route_flags)
    venues = _route_venues(routes)
    changes: dict[str, Any] = {
        "routes": scoped_routes,
        "enable_predict_fun": app_config.enable_predict_fun and "Predict.fun" in venues,
        "enable_sx_bet": app_config.enable_sx_bet and "SX Bet" in venues,
        "myriad_markets": replace(
            app_config.myriad_markets,
            enabled=app_config.myriad_markets.enabled and "Myriad" in venues,
        ),
    }
    if hasattr(app_config, "funded_routes"):
        changes["funded_routes"] = scoped_routes
    return replace(app_config, **changes)


def _scope_discovery_snapshot(
    snapshot: RouteDiscoverySnapshot,
    routes: tuple[str, ...],
) -> RouteDiscoverySnapshot:
    return replace(
        snapshot,
        enabled_routes=routes,
        missing_routes=tuple(route for route in snapshot.missing_routes if route in routes),
    )


def _predict_enabled(app_config: Any) -> bool:
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


def _sx_enabled(app_config: Any) -> bool:
    return bool(
        app_config.enable_sx_bet
        and app_config.sx_bet.enabled
        and (
            app_config.routes.polymarket_sx
            or app_config.routes.sx_myriad
            or getattr(app_config.routes, "predict_sx", False)
        )
    )


def _myriad_enabled(app_config: Any) -> bool:
    return bool(
        app_config.myriad_markets.enabled
        and (
            app_config.routes.polymarket_myriad
            or app_config.routes.predict_myriad
            or getattr(app_config.routes, "sx_myriad", False)
        )
    )


def _unresolved_count(payload: Any) -> int:
    if isinstance(payload, dict):
        try:
            return int(payload.get("count", 0))
        except (TypeError, ValueError):
            return 0
    return 0


def _venue_balance_gate(
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


def _full_capacity_fee_headroom_by_venue(
    all_market_audit: dict[str, Any],
    *,
    venues: set[str],
    max_positions: int,
) -> dict[str, dict[str, Any]]:
    maximum_fee_by_venue: dict[str, Decimal | None] = {venue: None for venue in venues}
    for market in all_market_audit.get("markets", ()):
        if not isinstance(market, dict) or not market.get("paired_preview_validated", False):
            continue
        for leg_name in ("first_leg", "second_leg"):
            leg = market.get(leg_name)
            if not isinstance(leg, dict):
                continue
            venue = str(leg.get("venue") or "")
            preview = leg.get("preview")
            if venue not in maximum_fee_by_venue or not isinstance(preview, dict):
                continue
            if not preview.get("signing_validated", False) or not preview.get("fee_metadata_verified", False):
                continue
            try:
                expected_fee = Decimal(str(preview["expected_fee_usd"]))
                maximum_fee_raw = preview.get("maximum_fee_usd")
                maximum_fee = (
                    Decimal(str(maximum_fee_raw))
                    if maximum_fee_raw not in (None, "")
                    else expected_fee
                )
                fee = max(expected_fee, maximum_fee)
            except (KeyError, TypeError, ValueError, ArithmeticError):
                continue
            if not fee.is_finite() or fee < 0:
                continue
            current = maximum_fee_by_venue[venue]
            if current is None or fee > current:
                maximum_fee_by_venue[venue] = fee

    result: dict[str, dict[str, Any]] = {}
    for venue, maximum_fee_per_leg in maximum_fee_by_venue.items():
        result[venue] = {
            "fee_headroom_verified": maximum_fee_per_leg is not None,
            "max_signed_preview_fee_per_leg_usd": maximum_fee_per_leg,
            "fee_headroom_usd": (
                maximum_fee_per_leg * Decimal(max_positions)
                if maximum_fee_per_leg is not None
                else None
            ),
            "preview_position_count": max_positions,
        }
    return result


def _apply_full_capacity_balance_gate(
    details: dict[str, Any],
    *,
    principal_required_usd: Decimal,
    fee_headroom: dict[str, Any],
    max_positions: int,
) -> None:
    gate = details["canary_gate"]
    blockers = list(gate.get("blocking_reasons", ()))
    fee_verified = bool(fee_headroom.get("fee_headroom_verified", False))
    fee_headroom_usd = fee_headroom.get("fee_headroom_usd")
    required_balance = (
        principal_required_usd + Decimal(str(fee_headroom_usd))
        if fee_verified and fee_headroom_usd is not None
        else None
    )
    if required_balance is None:
        blockers.append("full_capacity_fee_headroom_unverified")
    else:
        effective_balance = details.get("effective_balance") or {}
        balance_fields = {
            "connector_visible_balance_usd": "connector_visible_balance_below_full_capacity",
            "direct_balance_usd": "direct_balance_below_full_capacity",
            "effective_balance_usd": "runtime_effective_balance_below_full_capacity",
            "available_after_reservations_usd": "runtime_available_balance_below_full_capacity",
        }
        for field, blocker in balance_fields.items():
            balance = _safe_float(effective_balance.get(field))
            if balance is not None and Decimal(str(balance)) < required_balance:
                blockers.append(blocker)
    gate.update(
        {
            "passed": not blockers,
            "full_capacity_positions": max_positions,
            "principal_capacity_usd": principal_required_usd,
            "fee_headroom_verified": fee_verified,
            "max_signed_preview_fee_per_leg_usd": fee_headroom.get(
                "max_signed_preview_fee_per_leg_usd"
            ),
            "fee_headroom_usd": fee_headroom_usd,
            "required_balance_usd": required_balance,
            "blocking_reasons": list(dict.fromkeys(blockers)),
        }
    )


def _full_capacity_funding_readiness(
    *,
    enabled_routes: tuple[str, ...],
    venue_reports: dict[str, dict[str, Any]],
    route_summary: dict[str, Any],
    max_positions: int,
) -> dict[str, Any]:
    blockers: list[str] = []
    waiting_reasons: list[str] = []
    venue_results: dict[str, Any] = {}
    for venue in sorted(_route_venues(enabled_routes)):
        gate = venue_reports.get(venue, {}).get("canary_gate", {})
        # The funded wrapper deliberately evaluates this report while the runtime
        # is paused-shadow. Every other balance/runtime blocker remains fatal.
        relevant_blockers = [
            str(blocker)
            for blocker in gate.get("blocking_reasons", ())
            if blocker != "risk_paused"
        ]
        venue_ready = bool(gate) and not relevant_blockers
        venue_results[venue] = {
            **gate,
            "funding_ready_while_paused": venue_ready,
            "funding_blocking_reasons": relevant_blockers,
        }
        if not venue_ready:
            blockers.append(f"venue_not_funded_for_full_capacity:{venue}")

    natural_positive_route_count = 0
    for route in enabled_routes:
        route_state = route_summary.get(route, {})
        mechanical_count = int(route_state.get("mechanically_openable_count", 0))
        if mechanical_count <= 0:
            waiting_reasons.append(f"no_mechanically_openable_market:{route}")
        technical_count = int(
            route_state.get("technical_openable_count", route_state.get("openable_count", 0))
        )
        if "economically_openable_count" in route_state:
            technical_count = min(technical_count, int(route_state["economically_openable_count"]))
        if technical_count <= 0:
            waiting_reasons.append(f"no_natural_positive_openable_market:{route}")
        else:
            natural_positive_route_count += 1
    if natural_positive_route_count <= 0:
        blockers.append("no_natural_positive_openable_market_for_target")

    return {
        "ready": not blockers,
        "max_positions": max_positions,
        "venue_readiness": venue_results,
        "blocking_reasons": blockers,
        "non_blocking_waiting_reasons": waiting_reasons,
    }


def _order_preview_readiness(
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


def _failed_venue_report(
    *,
    venue: str,
    minimum_balance_usd: float,
    runtime_audit: dict[str, Any],
    error: str,
    blocking_reason: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    report = {
        "balance_probe_error": error,
        "effective_balance": {
            "connector_visible_balance_usd": None,
            "effective_balance_usd": None,
            "balance_cache_usd": None,
            "optimistic_debits_usd": None,
            "capital_reservations_usd": None,
            "available_after_reservations_usd": None,
            "runtime_audit": runtime_audit,
        },
        "canary_gate": {
            "venue": venue,
            "passed": False,
            "minimum_balance_usd": minimum_balance_usd,
            "blocking_reasons": [blocking_reason],
        },
    }
    if payload:
        report.update(payload)
    return report


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


async def _probe_observability(host: str, port: int) -> dict[str, Any]:
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


def _go_no_go_report(
    *,
    enabled_routes: tuple[str, ...],
    mapping_coverage: dict[str, Any],
    observability: dict[str, Any],
    venue_gates: dict[str, dict[str, Any]],
    route_overlap: dict[str, Any] | None = None,
    route_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    technical_blockers: list[str] = []
    canary_blockers: list[str] = []
    waiting_reasons: list[str] = []
    naturally_openable_routes = 0
    coverage = mapping_coverage.get("enabled_routes", {})
    for route in enabled_routes:
        route_state = coverage.get(route, {})
        if not route_state.get("has_verified", False):
            blocker = f"missing_verified_mapping:{route}"
            technical_blockers.append(blocker)
            canary_blockers.append(blocker)
        if route_overlap is not None:
            overlap_state = route_overlap.get("routes", {}).get(route, {})
            if int(overlap_state.get("verified_tradable_count", 0)) <= 0:
                blocker = f"no_verified_tradable_market:{route}"
                technical_blockers.append(blocker)
                canary_blockers.append(blocker)
        if route_summary is not None:
            openability_state = route_summary.get(route, {})
            technical_count = int(
                openability_state.get(
                    "technical_openable_count",
                    openability_state.get("openable_count", 0),
                )
            )
            mechanical_count = int(
                openability_state.get("mechanically_openable_count", technical_count)
            )
            if mechanical_count <= 0:
                waiting_reasons.append(f"no_mechanically_openable_market:{route}")
            canary_count = int(
                openability_state.get(
                    "canary_openable_count",
                    openability_state.get("openable_count", 0),
                )
            )
            has_economic_count = "economically_openable_count" in openability_state
            if has_economic_count:
                # Fail closed for v3 reports where technical excluded current
                # route economics. In v4 both values are identical.
                technical_count = min(
                    technical_count,
                    int(openability_state["economically_openable_count"]),
                )
            if technical_count <= 0:
                waiting_reasons.append(f"no_natural_positive_openable_market:{route}")
            else:
                naturally_openable_routes += 1
                if canary_count <= 0:
                    canary_blockers.append(f"canary_route_gate_failed:{route}")
    if route_summary is not None and naturally_openable_routes <= 0:
        canary_blockers.append("no_natural_positive_openable_market_for_target")
    if not observability.get("live", {}).get("ok", False):
        canary_blockers.append("health_live_failed")
    if not observability.get("ready", {}).get("ok", False):
        canary_blockers.append("health_ready_failed")
    metrics = observability.get("metrics", {})
    if metrics.get("arbitrage_ready") not in (None, 1.0):
        canary_blockers.append("arbitrage_ready_not_1")
    if metrics.get("arbitrage_risk_paused") not in (None, 0.0):
        canary_blockers.append("arbitrage_risk_paused_not_0")
    for venue, gate in venue_gates.items():
        if not gate.get("passed", False):
            canary_blockers.append(f"venue_gate_failed:{venue}")
    return {
        "technical_routes_ready": not technical_blockers,
        "technical_blocking_reasons": technical_blockers,
        "ready_for_canary": not canary_blockers,
        "blocking_reasons": canary_blockers,
        "non_blocking_waiting_reasons": waiting_reasons,
    }


async def _myriad_balances(client: MyriadClient) -> dict[str, dict[str, Any]]:
    web3_client = client._get_web3_client()
    account = web3_client.account
    if account is None:
        raise RuntimeError("MYRIAD_PRIVATE_KEY is required")
    balances: dict[str, dict[str, Any]] = {}
    for symbol, token_address in client._config.collateral_tokens.items():
        token = web3_client.contract(token_address, ERC20_BALANCE_ABI)
        raw_balance = int(await token.functions.balanceOf(account.address).call())
        decimals = int(await token.functions.decimals().call())
        balances[symbol] = {
            "token_address": token_address,
            "decimals": decimals,
            "balance_raw": str(raw_balance),
            "balance": raw_balance / float(10**decimals),
        }
    return balances


async def _predict_market_metadata(
    client: PredictFunApiClient,
    *,
    market_id: str | None,
    token_id: str | None,
) -> dict[str, Any] | None:
    if not market_id and not token_id:
        return None
    resolver = PredictFunMarketResolver(client._config, scan_all=True)
    try:
        payloads = await resolver._fetch_markets()  # noqa: SLF001
    finally:
        await resolver.close()
    for payload in payloads:
        payload_market_id = str(
            payload.get("id")
            or payload.get("marketId")
            or payload.get("market_id")
            or payload.get("conditionId")
            or payload.get("condition_id")
            or ""
        )
        outcome_tokens = {
            str(value)
            for key in ("yesTokenId", "yes_token_id", "yesToken", "noTokenId", "no_token_id", "noToken")
            if (value := payload.get(key)) not in (None, "")
        }
        if market_id and payload_market_id == market_id:
            return payload
        if token_id and token_id in outcome_tokens:
            return payload
        for outcome in payload.get("outcomes", []):
            if not isinstance(outcome, dict):
                continue
            outcome_token = (
                outcome.get("tokenId")
                or outcome.get("token_id")
                or outcome.get("onChainId")
                or outcome.get("on_chain_id")
                or outcome.get("assetId")
                or outcome.get("asset_id")
                or outcome.get("id")
            )
            if token_id and outcome_token is not None and str(outcome_token) == token_id:
                return payload
    return None


async def _sx_market_metadata(client: Any, market_hash: str) -> dict[str, Any] | None:
    payload = await client._request_json(  # noqa: SLF001
        "GET",
        "/markets/find",
        query_params={"marketHashes": market_hash},
    )
    data = payload.get("data") if isinstance(payload, dict) else None
    if isinstance(data, list) and data:
        return next((item for item in data if isinstance(item, dict)), None)
    return None


async def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Report live balance visibility and order-submit readiness for "
            "Polymarket, Predict.fun, SX Bet, and Myriad"
        )
    )
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--polymarket-condition-id")
    parser.add_argument("--polymarket-token-id")
    parser.add_argument("--polymarket-side", choices=("BUY", "SELL"))
    parser.add_argument("--polymarket-price", type=float)
    parser.add_argument("--polymarket-size", type=float)
    parser.add_argument("--myriad-market-id", type=int)
    parser.add_argument("--myriad-outcome-side", choices=("YES", "NO"))
    parser.add_argument("--myriad-order-side", choices=("BUY", "SELL"))
    parser.add_argument("--myriad-price", type=float)
    parser.add_argument("--myriad-size", type=float)
    parser.add_argument("--predict-market-id")
    parser.add_argument("--predict-token-id")
    parser.add_argument("--predict-order-side", choices=("BUY", "SELL"))
    parser.add_argument("--predict-price", type=float)
    parser.add_argument("--predict-size", type=float)
    parser.add_argument("--sx-market-hash")
    parser.add_argument("--sx-token-id")
    parser.add_argument("--sx-outcome-side", choices=("YES", "NO"))
    parser.add_argument("--sx-order-side", choices=("BUY", "SELL"))
    parser.add_argument("--sx-price", type=float)
    parser.add_argument("--sx-size", type=float)
    parser.add_argument("--all-markets", action="store_true")
    parser.add_argument(
        "--route",
        action="append",
        choices=ROUTE_NAMES,
        help="Limit --all-markets diagnostics to one enabled route; repeat for multiple routes.",
    )
    args = parser.parse_args()

    if args.route and not args.all_markets:
        parser.error("--route requires --all-markets")
    if args.all_markets:
        try:
            require_operator_catalog_context("all-market readiness audit")
        except RuntimeError as exc:
            parser.error(str(exc))

    load_operator_env(args.config)
    app_config = load_config(args.config)
    managed_routes = discovery_routes(app_config)
    configured_enabled_routes = _enabled_routes(app_config)
    try:
        selected_routes = _select_audit_routes(configured_enabled_routes, args.route)
    except ValueError as exc:
        parser.error(str(exc))
    app_config = _scope_app_config(app_config, selected_routes)
    polymarket = PolymarketClobClient(app_config.polymarket)
    predict_fun = PredictFunApiClient(app_config.predict_fun) if _predict_enabled(app_config) else None
    sx_bet: Any = create_sx_bet_client(app_config.sx_bet) if _sx_enabled(app_config) else None
    myriad = MyriadClient(app_config.myriad_markets) if _myriad_enabled(app_config) else None
    discovery_repository: ProductionRepository | None = None
    try:
        runtime_snapshot = await _load_runtime_audit(app_config, managed_routes=managed_routes)
        enabled_routes = selected_routes
        mapping_coverage = await _load_mapping_coverage(app_config.database_url, enabled_routes)
        observability = await _probe_observability("127.0.0.1", app_config.observability_port)
        report: dict[str, Any] = {
            "config_path": args.config,
            "configured_enabled_routes": configured_enabled_routes,
            "enabled_routes": enabled_routes,
            "mapping_coverage": mapping_coverage,
            "observability": observability,
        }

        polymarket_preview_requested = bool(
            args.polymarket_condition_id
            and args.polymarket_token_id
            and args.polymarket_side
            and args.polymarket_price is not None
            and args.polymarket_size is not None
        )
        polymarket_runtime_audit = _venue_runtime_audit(runtime_snapshot, "Polymarket")
        try:
            if app_config.polymarket.private_key:
                signer = Account.from_key(app_config.polymarket.private_key).address
            else:
                signer = None
            pm_balance = await polymarket.get_cash_balance()
            pm_sdk = polymarket._get_sdk_client()
            pm_balance_payload: Any | None = None
            try:
                from py_clob_client_v2 import AssetType, BalanceAllowanceParams  # type: ignore[import-untyped]

                pm_balance_payload = polymarket._sdk_call(
                    lambda client: client.get_balance_allowance(
                        BalanceAllowanceParams(
                            asset_type=AssetType.COLLATERAL,
                            signature_type=app_config.polymarket.signature_type,
                        )
                    )
                )
            except Exception:
                pm_balance_payload = None
            report["polymarket"] = {
                "signer_address": signer,
                "signature_type": app_config.polymarket.signature_type,
                "funder": app_config.polymarket.funder,
                "visible_balance_usd": pm_balance,
                "balance_payload": pm_balance_payload,
                "effective_balance": _effective_balance_payload(
                    "Polymarket",
                    pm_balance,
                    runtime_audit=polymarket_runtime_audit,
                ),
                "uses_configured_creds": polymarket._sdk_client_uses_configured_creds,
                "forced_derived_creds": polymarket._sdk_client_forced_derived_creds,
                "open_orders_count": len(pm_sdk.get_open_orders()),
                "canary_gate": _venue_balance_gate(
                    venue="Polymarket",
                    minimum_balance_usd=app_config.min_venue_balance_usd,
                    connector_balance=pm_balance,
                    direct_balance=None,
                    runtime_audit=polymarket_runtime_audit,
                ),
            }

            if polymarket_preview_requested:
                try:
                    from py_clob_client_v2 import OrderArgs, PartialCreateOrderOptions
                    from py_clob_client_v2.order_builder.constants import BUY, SELL  # type: ignore[import-untyped]
                except ImportError as exc:
                    raise RuntimeError("py-clob-client-v2 is required for Polymarket order previews") from exc
                market = pm_sdk.get_market(args.polymarket_condition_id)
                side = BUY if args.polymarket_side == "BUY" else SELL
                order = pm_sdk.create_order(
                    OrderArgs(
                        token_id=args.polymarket_token_id,
                        price=args.polymarket_price,
                        size=args.polymarket_size,
                        side=side,
                    ),
                    options=PartialCreateOrderOptions(
                        tick_size=str(market["minimum_tick_size"]),
                        neg_risk=bool(market["neg_risk"]),
                    ),
                )
                report["polymarket"]["order_preview"] = {
                    "market_question": market.get("question"),
                    "requested_side": args.polymarket_side,
                    "requested_price": args.polymarket_price,
                    "requested_size": args.polymarket_size,
                    "maker": order.maker,
                    "signer": order.signer,
                    "signature_type": int(order.signatureType),
                    "maker_amount": order.makerAmount,
                    "taker_amount": order.takerAmount,
                    **_redacted_signed_payload(
                        str(order),
                        detached_signature=order.signature,
                    ),
                }
        except Exception as exc:
            report["polymarket"] = _failed_venue_report(
                venue="Polymarket",
                minimum_balance_usd=app_config.min_venue_balance_usd,
                runtime_audit=polymarket_runtime_audit,
                error=str(exc),
                blocking_reason="polymarket_balance_probe_failed",
                payload={
                    "signer_address": None,
                    "signature_type": app_config.polymarket.signature_type,
                    "funder": app_config.polymarket.funder,
                    "visible_balance_usd": None,
                    "balance_payload": None,
                    "uses_configured_creds": None,
                    "forced_derived_creds": None,
                    "open_orders_count": None,
                },
            )
        report["polymarket"]["order_preview_readiness"] = _order_preview_readiness(
            requested=polymarket_preview_requested,
            private_key_configured=bool(app_config.polymarket.private_key),
            canary_gate_passed=bool(report["polymarket"]["canary_gate"]["passed"]),
        )

        if predict_fun is not None:
            predict_metadata: dict[str, Any] | None = None
            predict_preview_requested = bool(
                args.predict_market_id
                and args.predict_token_id
                and args.predict_order_side
                and args.predict_price is not None
                and args.predict_size is not None
            )
            predict_runtime_audit = _venue_runtime_audit(runtime_snapshot, "Predict.fun")
            if not app_config.predict_fun.private_key:
                report["predict_fun"] = _failed_venue_report(
                    venue="Predict.fun",
                    minimum_balance_usd=app_config.min_venue_balance_usd,
                    runtime_audit=predict_runtime_audit,
                    error="PREDICT_FUN_PRIVATE_KEY is not configured",
                    blocking_reason="predict_fun_private_key_missing",
                    payload={
                        "wallet_address": None,
                        "collateral_token_address": app_config.predict_fun.collateral_token_address,
                        "balance_function": app_config.predict_fun.balance_function,
                        "balance_raw": None,
                        "decimals": None,
                        "direct_balance_usd": None,
                        "connector_visible_balance_usd": None,
                    },
                )
            else:
                try:
                    predict_balance = await predict_fun.get_cash_balance()
                    predict_balance_details = await predict_fun.get_cash_balance_details()
                    report["predict_fun"] = {
                        "wallet_address": predict_balance_details["wallet_address"],
                        "collateral_token_address": predict_balance_details["collateral_token_address"],
                        "balance_function": predict_balance_details["balance_function"],
                        "balance_raw": predict_balance_details["balance_raw"],
                        "decimals": predict_balance_details["decimals"],
                        "direct_balance_usd": predict_balance_details["balance"],
                        "connector_visible_balance_usd": predict_balance,
                        "effective_balance": _effective_balance_payload(
                            "Predict.fun",
                            predict_balance,
                            direct_balance=float(predict_balance_details["balance"]),
                            runtime_audit=predict_runtime_audit,
                        ),
                        "canary_gate": _venue_balance_gate(
                            venue="Predict.fun",
                            minimum_balance_usd=app_config.min_venue_balance_usd,
                            connector_balance=predict_balance,
                            direct_balance=float(predict_balance_details["balance"]),
                            runtime_audit=predict_runtime_audit,
                        ),
                    }
                    if predict_preview_requested:
                        metadata_error: str | None = None
                        try:
                            predict_metadata = await _predict_market_metadata(
                                predict_fun,
                                market_id=args.predict_market_id,
                                token_id=args.predict_token_id,
                            )
                        except Exception as exc:
                            predict_metadata = None
                            metadata_error = str(exc)
                        if metadata_error is not None:
                            report["predict_fun"]["order_preview_metadata_error"] = metadata_error
                        if predict_metadata is not None:
                            fee_rate_bps = (
                                int(predict_metadata["feeRateBps"])
                                if predict_metadata.get("feeRateBps") not in (None, "")
                                else app_config.predict_fun.fee_rate_bps
                            )
                            neg_risk = False
                            for key in ("isNegRisk", "negRisk", "neg_risk"):
                                if isinstance(predict_metadata.get(key), bool):
                                    neg_risk = bool(predict_metadata[key])
                                    break
                            predict_book = await predict_fun.watch_order_book(args.predict_token_id)
                            signed_payload = predict_fun._build_signed_order_payload(  # noqa: SLF001
                                token_id=args.predict_token_id,
                                contracts=args.predict_size,
                                limit_price=args.predict_price,
                                sdk_side_name=args.predict_order_side,
                                neg_risk=neg_risk,
                                book=predict_book,
                                fee_rate_bps=fee_rate_bps,
                            )
                            report["predict_fun"]["order_preview"] = {
                                "market_id": args.predict_market_id,
                                "market_question": predict_metadata.get("question"),
                                "requested_side": args.predict_order_side,
                                "requested_price": args.predict_price,
                                "requested_size": args.predict_size,
                                "fee_rate_bps": fee_rate_bps,
                                "neg_risk": neg_risk,
                                **_redacted_signed_payload(signed_payload.signed_order),
                                "amount_wei": signed_payload.amount_wei,
                                "price_per_share_wei": signed_payload.price_per_share_wei,
                                "slippage_bps": signed_payload.slippage_bps,
                                "is_min_amount_out": signed_payload.is_min_amount_out,
                            }
                except Exception as exc:
                    report["predict_fun"] = _failed_venue_report(
                        venue="Predict.fun",
                        minimum_balance_usd=app_config.min_venue_balance_usd,
                        runtime_audit=predict_runtime_audit,
                        error=str(exc),
                        blocking_reason="predict_fun_balance_probe_failed",
                        payload={
                            "wallet_address": None,
                            "collateral_token_address": app_config.predict_fun.collateral_token_address,
                            "balance_function": app_config.predict_fun.balance_function,
                            "balance_raw": None,
                            "decimals": None,
                            "direct_balance_usd": None,
                            "connector_visible_balance_usd": None,
                        },
                    )
            report["predict_fun"]["order_preview_readiness"] = _order_preview_readiness(
                requested=predict_preview_requested,
                private_key_configured=bool(app_config.predict_fun.private_key),
                market_metadata_found=(
                    predict_metadata is not None if args.predict_market_id or args.predict_token_id else True
                ),
                canary_gate_passed=bool(report["predict_fun"]["canary_gate"]["passed"]),
            )

        if sx_bet is not None:
            sx_runtime_audit = _venue_runtime_audit(runtime_snapshot, "SX Bet")
            if app_config.sx_bet.private_key:
                try:
                    sx_balance = await sx_bet.get_cash_balance()
                    sx_balance_details = await sx_bet.get_cash_balance_details()
                    if app_config.sx_bet.api_version == "v3":
                        sx_explorer_balance = {
                            "ok": False,
                            "skipped": True,
                            "reason": "SX Bet V3 proxy balance is read from /user/balance-v3",
                        }
                    else:
                        sx_explorer_balance = await asyncio.to_thread(
                            _sx_explorer_balance,
                            str(sx_balance_details["wallet_address"]),
                            str(sx_balance_details["base_token_address"]),
                        )
                    sx_explorer_balance_usd = (
                        float(str(sx_explorer_balance["balance_usd"])) if sx_explorer_balance.get("ok") else None
                    )
                    report["sx_bet"] = {
                        "wallet_address": sx_balance_details["wallet_address"],
                        "base_token_address": sx_balance_details["base_token_address"],
                        "balance_raw": sx_balance_details["balance_raw"],
                        "decimals": sx_balance_details["decimals"],
                        "direct_balance_usd": sx_balance_details["balance"],
                        "connector_visible_balance_usd": sx_balance,
                        "explorer_balance": sx_explorer_balance,
                        "effective_balance": _effective_balance_payload(
                            "SX Bet",
                            sx_balance,
                            direct_balance=float(sx_balance_details["balance"]),
                            runtime_audit=sx_runtime_audit,
                        ),
                        "canary_gate": _venue_balance_gate(
                            venue="SX Bet",
                            minimum_balance_usd=app_config.min_venue_balance_usd,
                            connector_balance=sx_balance,
                            direct_balance=float(sx_balance_details["balance"]),
                            third_party_balance=sx_explorer_balance_usd,
                            third_party_balance_label="explorer",
                            runtime_audit=sx_runtime_audit,
                        ),
                    }
                except Exception as exc:
                    report["sx_bet"] = _failed_venue_report(
                        venue="SX Bet",
                        minimum_balance_usd=app_config.min_venue_balance_usd,
                        runtime_audit=sx_runtime_audit,
                        error=str(exc),
                        blocking_reason="sx_bet_balance_probe_failed",
                        payload={
                            "wallet_address": None,
                            "base_token_address": app_config.sx_bet.base_token_address,
                            "balance_raw": None,
                            "decimals": None,
                            "direct_balance_usd": None,
                            "connector_visible_balance_usd": None,
                            "explorer_balance": None,
                        },
                    )
            else:
                report["sx_bet"] = {
                    "wallet_address": None,
                    "base_token_address": app_config.sx_bet.base_token_address,
                    "balance_raw": None,
                    "decimals": None,
                    "direct_balance_usd": None,
                    "connector_visible_balance_usd": None,
                    "explorer_balance": None,
                    "balance_probe_error": "SX_BET_PRIVATE_KEY is not configured",
                    "effective_balance": {
                        "connector_visible_balance_usd": None,
                        "effective_balance_usd": None,
                        "balance_cache_usd": None,
                        "optimistic_debits_usd": None,
                        "capital_reservations_usd": None,
                        "available_after_reservations_usd": None,
                        "runtime_audit": sx_runtime_audit,
                    },
                    "canary_gate": {
                        "venue": "SX Bet",
                        "passed": False,
                        "minimum_balance_usd": app_config.min_venue_balance_usd,
                        "blocking_reasons": ["sx_private_key_missing"],
                    },
                }
            sx_metadata: dict[str, Any] | None = None
            if (
                args.sx_market_hash
                and args.sx_token_id
                and args.sx_outcome_side
                and args.sx_order_side
                and args.sx_price is not None
                and args.sx_size is not None
            ):
                sx_bet.register_market(args.sx_token_id, args.sx_market_hash, BinarySide(args.sx_outcome_side))
                sx_metadata_error: str | None = None
                try:
                    sx_metadata = await _sx_market_metadata(sx_bet, args.sx_market_hash)
                except Exception as exc:
                    sx_metadata = None
                    sx_metadata_error = str(exc)
                preview_metadata = {
                    "market_hash": args.sx_market_hash,
                    "market_question": sx_metadata.get("question") if isinstance(sx_metadata, dict) else None,
                    "league_label": sx_metadata.get("leagueLabel") if isinstance(sx_metadata, dict) else None,
                    "outcome_one_name": sx_metadata.get("outcomeOneName") if isinstance(sx_metadata, dict) else None,
                    "outcome_two_name": sx_metadata.get("outcomeTwoName") if isinstance(sx_metadata, dict) else None,
                    "token_id": args.sx_token_id,
                    "outcome_side": args.sx_outcome_side,
                    "requested_side": args.sx_order_side,
                    "requested_price": args.sx_price,
                    "requested_size": args.sx_size,
                }
                report["sx_bet"]["order_preview_metadata"] = preview_metadata
                if sx_metadata_error is not None:
                    report["sx_bet"]["order_preview_metadata_error"] = sx_metadata_error
                if app_config.sx_bet.private_key:
                    try:
                        preview = await sx_bet.build_order_preview(
                            token_id=args.sx_token_id,
                            side=BinarySide(args.sx_outcome_side),
                            contracts=args.sx_size,
                            limit_price=args.sx_price,
                            action=args.sx_order_side,
                        )
                        report["sx_bet"]["order_preview"] = {
                            **preview_metadata,
                            **redact_signing_material(preview),
                        }
                    except Exception as exc:
                        report["sx_bet"]["order_preview_error"] = str(exc)
                else:
                    report["sx_bet"]["order_preview_error"] = "SX_BET_PRIVATE_KEY is not configured"
            sx_preview_requested = bool(
                args.sx_market_hash
                and args.sx_token_id
                and args.sx_outcome_side
                and args.sx_order_side
                and args.sx_price is not None
                and args.sx_size is not None
            )
            report["sx_bet"]["order_preview_readiness"] = _order_preview_readiness(
                requested=sx_preview_requested,
                private_key_configured=bool(app_config.sx_bet.private_key),
                market_metadata_found=sx_metadata is not None if sx_preview_requested else True,
                canary_gate_passed=bool(report["sx_bet"]["canary_gate"]["passed"]),
            )

        if myriad is not None and app_config.myriad_markets.private_key:
            trader = Account.from_key(app_config.myriad_markets.private_key).address
        else:
            trader = None
        if myriad is not None:
            myriad_runtime_audit = _venue_runtime_audit(runtime_snapshot, "Myriad")
            myriad_preview_requested = bool(
                args.myriad_market_id is not None
                and args.myriad_outcome_side
                and args.myriad_order_side
                and args.myriad_price is not None
                and args.myriad_size is not None
            )
            if not app_config.myriad_markets.private_key:
                report["myriad"] = _failed_venue_report(
                    venue="Myriad",
                    minimum_balance_usd=app_config.min_venue_balance_usd,
                    runtime_audit=myriad_runtime_audit,
                    error="MYRIAD_PRIVATE_KEY is not configured",
                    blocking_reason="myriad_private_key_missing",
                    payload={
                        "trader_address": None,
                        "configured_collateral_symbol": app_config.myriad_markets.collateral_symbol,
                        "visible_balance_usd": None,
                        "all_collateral_balances": {},
                    },
                )
            else:
                try:
                    my_balances = await _myriad_balances(myriad)
                    my_balance = await myriad.get_cash_balance()
                    configured_collateral = my_balances.get(app_config.myriad_markets.collateral_symbol)
                    direct_myriad_balance = (
                        float(configured_collateral["balance"]) if configured_collateral is not None else None
                    )
                    report["myriad"] = {
                        "trader_address": trader,
                        "configured_collateral_symbol": app_config.myriad_markets.collateral_symbol,
                        "visible_balance_usd": my_balance,
                        "all_collateral_balances": my_balances,
                        "effective_balance": _effective_balance_payload(
                            "Myriad",
                            my_balance,
                            direct_balance=direct_myriad_balance,
                            runtime_audit=myriad_runtime_audit,
                        ),
                        "canary_gate": _venue_balance_gate(
                            venue="Myriad",
                            minimum_balance_usd=app_config.min_venue_balance_usd,
                            connector_balance=my_balance,
                            direct_balance=direct_myriad_balance,
                            runtime_audit=myriad_runtime_audit,
                        ),
                    }
                except Exception as exc:
                    report["myriad"] = _failed_venue_report(
                        venue="Myriad",
                        minimum_balance_usd=app_config.min_venue_balance_usd,
                        runtime_audit=myriad_runtime_audit,
                        error=str(exc),
                        blocking_reason="myriad_balance_probe_failed",
                        payload={
                            "trader_address": trader,
                            "configured_collateral_symbol": app_config.myriad_markets.collateral_symbol,
                            "visible_balance_usd": None,
                            "all_collateral_balances": {},
                        },
                    )
            if (
                args.myriad_market_id is not None
                and args.myriad_outcome_side
                and args.myriad_order_side
                and args.myriad_price is not None
                and args.myriad_size is not None
            ):
                try:
                    outcome_side = BinarySide(args.myriad_outcome_side)
                    orderbook = await myriad.get_orderbook(args.myriad_market_id, _outcome_id(outcome_side))
                    signed_order = await myriad.sign_order(
                        market_id=args.myriad_market_id,
                        outcome_id=_outcome_id(outcome_side),
                        side=0 if args.myriad_order_side == "BUY" else 1,
                        contracts=args.myriad_size,
                        price=args.myriad_price,
                    )
                    report["myriad"]["order_preview"] = {
                        "market_id": args.myriad_market_id,
                        "outcome_side": args.myriad_outcome_side,
                        "order_side": args.myriad_order_side,
                        "price": args.myriad_price,
                        "size": args.myriad_size,
                        "orderbook": orderbook,
                        **_redacted_signed_payload(
                            signed_order.order,
                            detached_signature=signed_order.signature,
                        ),
                    }
                except Exception as exc:
                    report["myriad"]["order_preview_error"] = str(exc)
            report["myriad"]["order_preview_readiness"] = _order_preview_readiness(
                requested=myriad_preview_requested,
                private_key_configured=bool(app_config.myriad_markets.private_key),
                canary_gate_passed=bool(report["myriad"]["canary_gate"]["passed"]),
            )

        report["mismatch_checks"] = {
            "predict_fun_direct_matches_connector": (
                report["predict_fun"]["effective_balance"].get("direct_matches_connector")
                if "predict_fun" in report
                else None
            ),
            "sx_bet_direct_matches_connector": (
                report["sx_bet"]["effective_balance"].get("direct_matches_connector") if "sx_bet" in report else None
            ),
            "myriad_direct_matches_connector": (
                report["myriad"]["effective_balance"].get("direct_matches_connector") if "myriad" in report else None
            ),
            "database_reachable": bool(runtime_snapshot),
            "reconciliation_clean": not bool((runtime_snapshot or {}).get("reconciliation_failures")),
            "unresolved_order_intents_zero": (
                (runtime_snapshot or {}).get("unresolved_order_intents", {}).get("count", 0) == 0
            ),
            "unresolved_redemptions_zero": (
                (runtime_snapshot or {}).get("unresolved_redemptions", {}).get("count", 0) == 0
            ),
            "risk_paused": bool(((runtime_snapshot or {}).get("risk_state") or {}).get("paused", False)),
            "runtime_balance_state_present": bool((runtime_snapshot or {}).get("latest_runtime_balance_state")),
        }
        venue_gate_rows = [("Polymarket", report["polymarket"])]
        if "predict_fun" in report:
            venue_gate_rows.append(("Predict.fun", report["predict_fun"]))
        if "sx_bet" in report:
            venue_gate_rows.append(("SX Bet", report["sx_bet"]))
        if "myriad" in report:
            venue_gate_rows.append(("Myriad", report["myriad"]))

        report["next_live_steps"] = {
            "polymarket": (
                "Use signature_type=2 and funder=0x6f93865A536BcF6ef4B79e527de67ECdce0F989A; "
                "submit only after a previewed SAFE-mode order looks correct."
            ),
        }
        if "predict_fun" in report:
            report["next_live_steps"]["predict_fun"] = (
                "Require direct collateral balance parity, one VERIFIED mapping for each enabled Predict.fun route, "
                "and a clean signed-order preview before enabling `polymarket_predict`, `predict_myriad`, "
                "or `predict_sx`."
            )
        if "sx_bet" in report:
            report["next_live_steps"]["sx_bet"] = (
                "Require direct base-token balance parity, one VERIFIED mapping for each enabled SX route, "
                "and a clean signed fill preview before enabling `polymarket_sx`, `predict_sx`, or `sx_myriad`."
            )
        if "myriad" in report:
            report["next_live_steps"]["myriad"] = (
                "Use collateral_symbol=USD1 for this wallet; submit only after the signed order preview "
                "matches the intended market, side, price, and size."
            )
        if args.all_markets:
            if app_config.database_url:
                candidate = ProductionRepository(
                    app_config.database_url,
                    runtime_instance_id=app_config.runtime_instance_id,
                    enabled_routes=enabled_routes,
                )
                if await candidate.ping():
                    discovery_repository = candidate
                else:
                    await candidate.close()
            snapshot = _scope_discovery_snapshot(
                await resolve_route_discovery_snapshot(app_config, discovery_repository),
                enabled_routes,
            )
            report["route_overlap"] = build_route_overlap_report(snapshot)
            report["all_market_audit"] = await collect_all_market_audit(
                app_config,
                snapshot,
                runtime_snapshot,
            )
            principal_required = (
                Decimal(str(app_config.position_size_usd))
                / Decimal(2)
                * Decimal(app_config.max_open_positions)
            )
            enabled_venues = _route_venues(enabled_routes)
            fee_headroom_by_venue = _full_capacity_fee_headroom_by_venue(
                report["all_market_audit"],
                venues=enabled_venues,
                max_positions=app_config.max_open_positions,
            )
            venue_report_by_name = {
                venue: details
                for venue, details in venue_gate_rows
                if venue in enabled_venues
            }
            for venue, details in venue_report_by_name.items():
                _apply_full_capacity_balance_gate(
                    details,
                    principal_required_usd=principal_required,
                    fee_headroom=fee_headroom_by_venue[venue],
                    max_positions=app_config.max_open_positions,
                )
            report["full_capacity_funding_readiness"] = _full_capacity_funding_readiness(
                enabled_routes=enabled_routes,
                venue_reports=venue_report_by_name,
                route_summary=report["all_market_audit"].get("route_summary", {}),
                max_positions=app_config.max_open_positions,
            )
            # Full Predict catalogs retain tens of thousands of MarketSpec objects.
            # The report no longer needs that graph and must release it before
            # streaming a potentially large all-market JSON artifact.
            del snapshot
        report["canary_go_no_go"] = _go_no_go_report(
            enabled_routes=enabled_routes,
            mapping_coverage=mapping_coverage,
            observability=observability,
            venue_gates={
                venue: details["canary_gate"]
                for venue, details in venue_gate_rows
                if venue in _route_venues(enabled_routes)
            },
            route_overlap=report.get("route_overlap") if args.all_markets else None,
            route_summary=(
                report.get("all_market_audit", {}).get("route_summary")
                if args.all_markets
                else None
            ),
        )
        _write_json_report(report, sys.stdout)
    finally:
        await polymarket.close()
        if predict_fun is not None:
            await predict_fun.close()
        if sx_bet is not None:
            await sx_bet.close()
        if myriad is not None:
            await myriad.close()
        if discovery_repository is not None:
            await discovery_repository.close()


if __name__ == "__main__":
    asyncio.run(main())
