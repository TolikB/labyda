from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from decimal import Decimal
from typing import Any

from arbitrage_engine.config import load_config, load_operator_env
from arbitrage_engine.connectors.predict_fun import PredictFunApiClient
from arbitrage_engine.database import ProductionRepository
from arbitrage_engine.models import BinarySide
from arbitrage_engine.predict_fun_discovery import PredictFunMarketResolver, _token_id_for_side
from arbitrage_engine.production_audit import enabled_routes


def _json_default(value: Any) -> str:
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _report_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, default=_json_default)


def _redacted_signed_preview(signed: Any) -> dict[str, Any]:
    canonical_payload = json.dumps(
        signed.signed_order,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return {
        "signed_preview_created": True,
        "signed_order_sha256": hashlib.sha256(canonical_payload).hexdigest(),
        "signature_present": bool(signed.signed_order.get("signature")),
        "amount_wei": signed.amount_wei,
        "price_per_share_wei": signed.price_per_share_wei,
        "slippage_bps": signed.slippage_bps,
        "is_min_amount_out": signed.is_min_amount_out,
    }


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _runtime_balance_state(runtime_audit: dict[str, Any], venue: str) -> dict[str, float | None]:
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
        if market_id and payload_market_id == market_id:
            return payload
        if token_id and _payload_contains_token(payload, token_id):
            return payload
    return None


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


def _predict_runtime_audit(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    if snapshot is None:
        return {
            "database_reachable": False,
            "note": "DATABASE_URL is missing or unreachable; only direct and connector-visible balances are shown.",
        }
    latest_balances = snapshot.get("latest_balance_snapshots", {})
    unresolved_orders = snapshot.get("unresolved_order_intents", {})
    unresolved_redemptions = snapshot.get("unresolved_redemptions", {})
    positions = snapshot.get("positions", {})
    return {
        "database_reachable": True,
        "latest_balance_snapshot": latest_balances.get("Predict.fun", {}),
        "unresolved_order_intents": unresolved_orders.get("by_venue", {}).get(
            "Predict.fun",
            {"count": 0, "by_status": {}},
        ),
        "unresolved_redemptions": unresolved_redemptions.get("by_venue", {}).get(
            "Predict.fun",
            {"count": 0, "by_status": {}},
        ),
        "open_position_entry_notional_usd": positions.get("estimated_entry_notional_by_venue_usd", {}).get(
            "Predict.fun",
            "0",
        ),
        "position_count": positions.get("count", 0),
        "position_statuses": positions.get("by_status", {}),
        "reconciliation_failures": snapshot.get("reconciliation_failures", []),
        "risk_state": snapshot.get("risk_state"),
        "latest_runtime_balance_state": snapshot.get("latest_runtime_balance_state"),
        "metrics": snapshot.get("metrics", {}),
        "note": (
            "Standalone preview can read durable DB state. When the live bot is persisting runtime balance state, "
            "process-local balance cache, optimistic debits, and capital reservations are included too."
        ),
    }


def _payload_contains_token(payload: dict[str, Any], token_id: str) -> bool:
    direct_keys = (
        "yesTokenId",
        "yes_token_id",
        "yesToken",
        "noTokenId",
        "no_token_id",
        "noToken",
    )
    if any(str(payload.get(key)) == token_id for key in direct_keys if payload.get(key) not in (None, "")):
        return True
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
        if outcome_token is not None and str(outcome_token) == token_id:
            return True
    return False


def _unresolved_count(payload: Any) -> int:
    if isinstance(payload, dict):
        try:
            return int(payload.get("count", 0))
        except (TypeError, ValueError):
            return 0
    return 0


def _predict_canary_gate(
    *,
    minimum_balance_usd: float,
    connector_balance: float,
    direct_balance: float,
    runtime_audit: dict[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    runtime_state = _runtime_balance_state(runtime_audit, "Predict.fun")
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


def _predict_order_preview_readiness(
    *,
    requested: bool,
    private_key_configured: bool,
    metadata_found: bool,
    canary_gate_passed: bool,
) -> dict[str, Any]:
    blockers: list[str] = []
    if not private_key_configured:
        blockers.append("predict_fun_private_key_missing")
    if requested and not metadata_found:
        blockers.append("predict_fun_market_metadata_not_found")
    if not canary_gate_passed:
        blockers.append("predict_fun_balance_or_runtime_gate_failed")
    return {
        "requested": requested,
        "ready": not blockers,
        "blocking_reasons": blockers,
    }


def _predict_failure_report(
    *,
    app_config: Any,
    runtime_audit: dict[str, Any],
    error: str,
    blocking_reason: str,
) -> dict[str, Any]:
    return {
        "config_path": None,
        "wallet_address": None,
        "collateral_token_address": app_config.predict_fun.collateral_token_address,
        "balance_function": app_config.predict_fun.balance_function,
        "balance_raw": None,
        "decimals": None,
        "direct_balance_usd": None,
        "connector_visible_balance_usd": None,
        "balance_probe_error": error,
        "effective_balance": {
            "effective_balance_usd": None,
            "connector_visible_balance_usd": None,
            "balance_cache_usd": None,
            "optimistic_debits_usd": None,
            "capital_reservations_usd": None,
            "available_after_reservations_usd": None,
            "runtime_audit": runtime_audit,
        },
        "canary_gate": {
            "passed": False,
            "minimum_balance_usd": app_config.min_venue_balance_usd,
            "blocking_reasons": [blocking_reason],
        },
    }


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Report Predict.fun live balance visibility and optional signed-order preview"
    )
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--market-id")
    parser.add_argument("--token-id")
    parser.add_argument("--side", choices=("BUY", "SELL"))
    parser.add_argument("--price", type=float)
    parser.add_argument("--size", type=float)
    args = parser.parse_args()

    load_operator_env(args.config)
    app_config = load_config(args.config)
    client = PredictFunApiClient(app_config.predict_fun)
    try:
        runtime_audit = _predict_runtime_audit(await _load_runtime_audit(app_config))
        preview_requested = bool(
            args.market_id and args.token_id and args.side and args.price is not None and args.size is not None
        )
        if not app_config.predict_fun.private_key:
            report = _predict_failure_report(
                app_config=app_config,
                runtime_audit=runtime_audit,
                error="PREDICT_FUN_PRIVATE_KEY is not configured",
                blocking_reason="predict_fun_private_key_missing",
            )
            report["config_path"] = args.config
            report["order_preview_readiness"] = _predict_order_preview_readiness(
                requested=preview_requested,
                private_key_configured=False,
                metadata_found=not preview_requested,
                canary_gate_passed=False,
            )
            print(_report_json(report))
            return
        try:
            balance_details = await client.get_cash_balance_details()
            connector_balance = await client.get_cash_balance()
        except Exception as exc:
            report = _predict_failure_report(
                app_config=app_config,
                runtime_audit=runtime_audit,
                error=str(exc),
                blocking_reason="predict_fun_balance_probe_failed",
            )
            report["config_path"] = args.config
            report["order_preview_readiness"] = _predict_order_preview_readiness(
                requested=preview_requested,
                private_key_configured=True,
                metadata_found=not preview_requested,
                canary_gate_passed=False,
            )
            print(_report_json(report))
            return
        canary_gate = _predict_canary_gate(
            minimum_balance_usd=app_config.min_venue_balance_usd,
            connector_balance=connector_balance,
            direct_balance=float(balance_details["balance"]),
            runtime_audit=runtime_audit,
        )
        runtime_state = _runtime_balance_state(runtime_audit, "Predict.fun")
        report = {
            "config_path": args.config,
            "wallet_address": balance_details["wallet_address"],
            "collateral_token_address": balance_details["collateral_token_address"],
            "balance_function": balance_details["balance_function"],
            "balance_raw": balance_details["balance_raw"],
            "decimals": balance_details["decimals"],
            "direct_balance_usd": balance_details["balance"],
            "connector_visible_balance_usd": connector_balance,
            "direct_vs_connector_delta_usd": round(float(balance_details["balance"]) - connector_balance, 12),
            "effective_balance": {
                "effective_balance_usd": (
                    connector_balance
                    if runtime_state["effective_balance_usd"] is None
                    else runtime_state["effective_balance_usd"]
                ),
                "balance_cache_usd": runtime_state["balance_cache_usd"],
                "optimistic_debits_usd": runtime_state["optimistic_debits_usd"],
                "capital_reservations_usd": runtime_state["capital_reservations_usd"],
                "available_after_reservations_usd": runtime_state["available_after_reservations_usd"],
                "runtime_audit": runtime_audit,
            },
            "canary_gate": canary_gate,
        }
        metadata: dict[str, Any] | None = None
        if preview_requested:
            metadata_error: str | None = None
            try:
                metadata = await _predict_market_metadata(client, market_id=args.market_id, token_id=args.token_id)
            except Exception as exc:
                metadata = None
                metadata_error = str(exc)
            if metadata_error is not None:
                report["order_preview_metadata_error"] = metadata_error
            if metadata is not None:
                try:
                    fee_rate_raw = metadata.get("feeRateBps")
                    if fee_rate_raw in (None, ""):
                        raise RuntimeError("Predict.fun market feeRateBps metadata is unavailable")
                    fee_rate_bps = int(str(fee_rate_raw))
                    price_precision_raw = metadata.get("decimalPrecision")
                    if price_precision_raw in (None, ""):
                        raise RuntimeError("Predict.fun market decimalPrecision metadata is unavailable")
                    price_precision = int(str(price_precision_raw))
                    token_side = next(
                        (
                            side
                            for side in (BinarySide.YES, BinarySide.NO)
                            if _token_id_for_side(metadata, side) == args.token_id
                        ),
                        None,
                    )
                    if token_side is None:
                        raise RuntimeError("Predict.fun token is not present in the selected market metadata")
                    neg_risk = False
                    for key in ("isNegRisk", "negRisk", "neg_risk"):
                        if isinstance(metadata.get(key), bool):
                            neg_risk = bool(metadata[key])
                            break
                    client.register_market(
                        args.token_id,
                        args.market_id,
                        token_side,
                        fee_rate_bps=fee_rate_bps,
                        price_precision=price_precision,
                    )
                    book = await client.watch_order_book(args.token_id)
                    signed = client._build_signed_order_payload(  # noqa: SLF001
                        token_id=args.token_id,
                        contracts=args.size,
                        limit_price=args.price,
                        sdk_side_name=args.side,
                        neg_risk=neg_risk,
                        fee_rate_bps=fee_rate_bps,
                        book=book,
                    )
                    report["order_preview"] = {
                        "market_id": args.market_id,
                        "market_question": metadata.get("question"),
                        "token_id": args.token_id,
                        "requested_side": args.side,
                        "requested_price": args.price,
                        "requested_size": args.size,
                        "fee_rate_bps": fee_rate_bps,
                        "neg_risk": neg_risk,
                        **_redacted_signed_preview(signed),
                    }
                except Exception as exc:
                    report["order_preview_error"] = str(exc)
        report["order_preview_readiness"] = _predict_order_preview_readiness(
            requested=preview_requested,
            private_key_configured=bool(app_config.predict_fun.private_key),
            metadata_found=metadata is not None if args.market_id or args.token_id else True,
            canary_gate_passed=bool(canary_gate["passed"]),
        )

        print(_report_json(report))
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
