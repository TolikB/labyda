from __future__ import annotations

import argparse
import asyncio
import json
from typing import Any
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

from arbitrage_engine.config import load_config, load_operator_env
from arbitrage_engine.connectors.sx_bet import create_sx_bet_client
from arbitrage_engine.database import ProductionRepository
from arbitrage_engine.models import BinarySide
from arbitrage_engine.production_audit import enabled_routes

SX_EXPLORER_API_URL = "https://explorerl2.sx.technology/api"


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
    connector_balance: float | None,
    *,
    direct_balance: float | None = None,
    runtime_audit: dict[str, Any] | None = None,
) -> dict[str, Any]:
    runtime_state = _runtime_balance_state(runtime_audit, venue)
    effective_balance = runtime_state["effective_balance_usd"]
    payload: dict[str, Any] = {
        "connector_visible_balance_usd": connector_balance,
        "effective_balance_usd": connector_balance if effective_balance is None else effective_balance,
        "balance_cache_usd": runtime_state["balance_cache_usd"],
        "optimistic_debits_usd": runtime_state["optimistic_debits_usd"],
        "capital_reservations_usd": runtime_state["capital_reservations_usd"],
        "available_after_reservations_usd": runtime_state["available_after_reservations_usd"],
    }
    if direct_balance is not None and connector_balance is not None:
        payload["direct_balance_usd"] = direct_balance
        payload["direct_vs_connector_delta_usd"] = round(direct_balance - connector_balance, 12)
        payload["direct_matches_connector"] = abs(direct_balance - connector_balance) < 1e-9
        if payload["effective_balance_usd"] is not None:
            payload["direct_vs_effective_delta_usd"] = round(
                direct_balance - float(payload["effective_balance_usd"]),
                12,
            )
    if runtime_state["balance_cache_usd"] is not None and connector_balance is not None:
        payload["runtime_balance_cache_vs_connector_delta_usd"] = round(
            float(runtime_state["balance_cache_usd"]) - connector_balance,
            12,
        )
    if runtime_audit is not None:
        payload["runtime_audit"] = runtime_audit
    return payload


async def _load_runtime_audit(app_config: Any) -> dict[str, Any] | None:
    database_url = getattr(app_config, "database_url", None)
    if not database_url:
        return None
    repository = ProductionRepository(
        database_url,
        runtime_instance_id=getattr(app_config, "runtime_instance_id", "global"),
        enabled_routes=enabled_routes(app_config),
    )
    try:
        if not await repository.ping():
            return None
        return await repository.runtime_audit_snapshot()
    finally:
        await repository.close()


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
        "unresolved_redemptions": unresolved_redemptions.get("by_venue", {}).get(
            venue, {"count": 0, "by_status": {}}
        ),
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


def _unresolved_count(payload: Any) -> int:
    if isinstance(payload, dict):
        try:
            return int(payload.get("count", 0))
        except (TypeError, ValueError):
            return 0
    return 0


def _sx_canary_gate(
    *,
    minimum_balance_usd: float,
    connector_balance: float,
    direct_balance: float,
    explorer_balance: dict[str, Any] | None,
    runtime_audit: dict[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    runtime_state = _runtime_balance_state(runtime_audit, "SX Bet")
    if direct_balance < minimum_balance_usd:
        blockers.append("direct_balance_below_minimum")
    if connector_balance < minimum_balance_usd:
        blockers.append("connector_visible_balance_below_minimum")
    if abs(direct_balance - connector_balance) >= 1e-9:
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
        runtime_state["balance_cache_usd"] is not None
        and abs(runtime_state["balance_cache_usd"] - connector_balance) >= 1e-9
    ):
        blockers.append("runtime_balance_cache_vs_connector_mismatch")
    if (
        runtime_state["balance_cache_usd"] is not None
        and abs(runtime_state["balance_cache_usd"] - direct_balance) >= 1e-9
    ):
        blockers.append("direct_vs_runtime_balance_cache_mismatch")
    if explorer_balance is not None and explorer_balance.get("ok"):
        explorer_balance_usd = float(explorer_balance["balance_usd"])
        if explorer_balance_usd < minimum_balance_usd:
            blockers.append("explorer_balance_below_minimum")
        if abs(explorer_balance_usd - direct_balance) >= 1e-9:
            blockers.append("explorer_vs_direct_balance_mismatch")
        if abs(explorer_balance_usd - connector_balance) >= 1e-9:
            blockers.append("explorer_vs_connector_balance_mismatch")
    if _unresolved_count(runtime_audit.get("unresolved_order_intents")) > 0:
        blockers.append("unresolved_order_intents_present")
    if _unresolved_count(runtime_audit.get("unresolved_redemptions")) > 0:
        blockers.append("unresolved_redemptions_present")
    if runtime_audit.get("reconciliation_failures"):
        blockers.append("reconciliation_failures_present")
    if isinstance(runtime_audit.get("risk_state"), dict) and runtime_audit["risk_state"].get("paused"):
        blockers.append("risk_paused")
    return {
        "passed": not blockers,
        "minimum_balance_usd": minimum_balance_usd,
        "blocking_reasons": blockers,
    }


def _sx_order_preview_readiness(
    *,
    requested: bool,
    private_key_configured: bool,
    canary_gate_passed: bool,
) -> dict[str, Any]:
    blockers: list[str] = []
    if not private_key_configured:
        blockers.append("sx_private_key_missing")
    if not canary_gate_passed:
        blockers.append("sx_balance_or_runtime_gate_failed")
    return {
        "requested": requested,
        "ready": not blockers,
        "blocking_reasons": blockers,
    }


def _sx_failure_report(
    *,
    app_config: Any,
    runtime_audit: dict[str, Any],
    error: str,
    blocking_reason: str,
) -> dict[str, Any]:
    return {
        "config_path": None,
        "wallet_address": None,
        "base_token_address": app_config.sx_bet.base_token_address,
        "balance_raw": None,
        "decimals": None,
        "direct_balance_usd": None,
        "connector_visible_balance_usd": None,
        "direct_vs_connector_delta_usd": None,
        "explorer_balance": None,
        "balance_probe_error": error,
        "effective_balance": _effective_balance_payload("SX Bet", None, runtime_audit=runtime_audit),
        "canary_gate": {
            "passed": False,
            "minimum_balance_usd": app_config.min_venue_balance_usd,
            "blocking_reasons": [blocking_reason],
        },
    }


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


def _http_json(url: str) -> Any:
    request = urllib_request.Request(url, headers={"Accept": "application/json"})
    with urllib_request.urlopen(request, timeout=15) as response:
        return json.loads(response.read().decode("utf-8"))


async def _sx_explorer_balance(address: str, token_address: str) -> dict[str, Any]:
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
    try:
        payload = await asyncio.to_thread(_http_json, url)
    except (urllib_error.HTTPError, urllib_error.URLError, TimeoutError, ValueError) as exc:
        return {"ok": False, "error": str(exc), "url": url}
    result = payload.get("result") if isinstance(payload, dict) else None
    try:
        raw_balance = str(result)
        scaled = int(raw_balance) / 1_000_000
    except (TypeError, ValueError):
        return {"ok": False, "error": f"unexpected explorer payload: {payload!r}", "url": url}
    return {
        "ok": True,
        "url": url,
        "balance_raw": raw_balance,
        "balance_usd": scaled,
        "payload": payload,
    }


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Report SX Bet live balance visibility and optional signed-order preview"
    )
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--market-hash")
    parser.add_argument("--token-id")
    parser.add_argument("--outcome-side", choices=("YES", "NO"))
    parser.add_argument("--order-side", choices=("BUY", "SELL"))
    parser.add_argument("--price", type=float)
    parser.add_argument("--size", type=float)
    args = parser.parse_args()

    load_operator_env(args.config)
    app_config = load_config(args.config)
    client: Any = create_sx_bet_client(app_config.sx_bet)
    try:
        runtime_audit = _venue_runtime_audit(await _load_runtime_audit(app_config), "SX Bet")
        preview_requested = bool(
            args.market_hash
            and args.token_id
            and args.outcome_side
            and args.order_side
            and args.price is not None
            and args.size is not None
        )
        report: dict[str, Any] = {"config_path": args.config}
        balance_credentials_ready = bool(
            app_config.sx_bet.api_key
            if app_config.sx_bet.api_version == "v3"
            else app_config.sx_bet.private_key
        )
        if balance_credentials_ready:
            try:
                balance_details = await client.get_cash_balance_details()
                connector_balance = await client.get_cash_balance()
                explorer_balance = None
                if app_config.sx_bet.api_version == "v2":
                    explorer_balance = await _sx_explorer_balance(
                        str(balance_details["wallet_address"]),
                        str(balance_details["base_token_address"]),
                    )
                canary_gate = _sx_canary_gate(
                    minimum_balance_usd=app_config.min_venue_balance_usd,
                    connector_balance=connector_balance,
                    direct_balance=float(balance_details["balance"]),
                    explorer_balance=explorer_balance,
                    runtime_audit=runtime_audit,
                )
                report.update(
                    {
                        "wallet_address": balance_details["wallet_address"],
                        "base_token_address": balance_details["base_token_address"],
                        "balance_raw": balance_details["balance_raw"],
                        "decimals": balance_details["decimals"],
                        "direct_balance_usd": balance_details["balance"],
                        "connector_visible_balance_usd": connector_balance,
                        "direct_vs_connector_delta_usd": round(
                            float(balance_details["balance"]) - connector_balance,
                            12,
                        ),
                        "explorer_balance": explorer_balance,
                        "effective_balance": _effective_balance_payload(
                            "SX Bet",
                            connector_balance,
                            direct_balance=float(balance_details["balance"]),
                            runtime_audit=runtime_audit,
                        ),
                        "canary_gate": canary_gate,
                    }
                )
            except Exception as exc:
                canary_gate = {
                    "passed": False,
                    "minimum_balance_usd": app_config.min_venue_balance_usd,
                    "blocking_reasons": ["sx_bet_balance_probe_failed"],
                }
                report = _sx_failure_report(
                    app_config=app_config,
                    runtime_audit=runtime_audit,
                    error=str(exc),
                    blocking_reason="sx_bet_balance_probe_failed",
                )
                report["config_path"] = args.config
        else:
            canary_gate = {
                "passed": False,
                "minimum_balance_usd": app_config.min_venue_balance_usd,
                    "blocking_reasons": [
                        "sx_v3_api_key_missing"
                        if app_config.sx_bet.api_version == "v3"
                        else "sx_private_key_missing"
                    ],
            }
            report.update(
                {
                    "wallet_address": None,
                    "base_token_address": app_config.sx_bet.base_token_address,
                    "balance_raw": None,
                    "decimals": None,
                    "direct_balance_usd": None,
                    "connector_visible_balance_usd": None,
                    "direct_vs_connector_delta_usd": None,
                    "explorer_balance": None,
                    "effective_balance": _effective_balance_payload("SX Bet", None, runtime_audit=runtime_audit),
                    "canary_gate": canary_gate,
                    "balance_probe_error": (
                        "SX_BET_API_KEY is not configured for V3"
                        if app_config.sx_bet.api_version == "v3"
                        else "SX_BET_PRIVATE_KEY is not configured"
                    ),
                }
            )

        if preview_requested:
            client.register_market(args.token_id, args.market_hash, BinarySide(args.outcome_side))
            if app_config.sx_bet.api_version == "v3" and args.order_side == "SELL":
                opposite = BinarySide.NO if args.outcome_side == "YES" else BinarySide.YES
                client.register_market(
                    f"{args.market_hash}:{opposite.value}",
                    args.market_hash,
                    opposite,
                )
            market_metadata: dict[str, Any] | None = None
            market_metadata_error: str | None = None
            try:
                market_metadata = await _sx_market_metadata(client, args.market_hash)
            except Exception as exc:
                market_metadata_error = str(exc)
            report["order_preview_metadata"] = {
                "market_hash": args.market_hash,
                "market_question": market_metadata.get("question") if isinstance(market_metadata, dict) else None,
                "league_label": market_metadata.get("leagueLabel") if isinstance(market_metadata, dict) else None,
                "outcome_one_name": (
                    market_metadata.get("outcomeOneName") if isinstance(market_metadata, dict) else None
                ),
                "outcome_two_name": (
                    market_metadata.get("outcomeTwoName") if isinstance(market_metadata, dict) else None
                ),
                "token_id": args.token_id,
                "outcome_side": args.outcome_side,
                "requested_side": args.order_side,
                "requested_price": args.price,
                "requested_size": args.size,
            }
            if market_metadata_error is not None:
                report["order_preview_metadata_error"] = market_metadata_error
            if app_config.sx_bet.private_key:
                try:
                    preview = await client.build_order_preview(
                        token_id=args.token_id,
                        side=BinarySide(args.outcome_side),
                        contracts=args.size,
                        limit_price=args.price,
                        action=args.order_side,
                    )
                    report["order_preview"] = {
                        **report["order_preview_metadata"],
                        **preview,
                    }
                except Exception as exc:
                    report["order_preview_error"] = str(exc)

        report["order_preview_readiness"] = _sx_order_preview_readiness(
            requested=preview_requested,
            private_key_configured=bool(app_config.sx_bet.private_key),
            canary_gate_passed=bool(canary_gate["passed"]),
        )

        print(json.dumps(report, indent=2))
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
