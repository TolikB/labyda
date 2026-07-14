from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import replace
from typing import Any

from arbitrage_engine.config import load_config, load_operator_env
from arbitrage_engine.main import _deduplicate_markets, _filter_markets_by_volume
from arbitrage_engine.market_discovery import GammaMarketResolver
from arbitrage_engine.matcher import normalize_text
from arbitrage_engine.models import MarketSpec, myriad_execution_side_for_route, myriad_execution_token_for_route
from arbitrage_engine.myriad_discovery import MyriadMarketResolver
from arbitrage_engine.sx_bet_discovery import SxBetMarketResolver


def _select_probe_markets(markets: list[MarketSpec], contains: str | None, limit: int) -> list[MarketSpec]:
    if contains:
        needle = normalize_text(contains)
        filtered = [
            market
            for market in markets
            if needle in normalize_text(market.symbol) or needle in normalize_text(market.target_label)
        ]
    else:
        filtered = list(markets)
    return filtered[:limit]


def _matched_market_rows(markets: list[MarketSpec], *, route: str, limit: int = 10) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for market in markets:
        if route == "polymarket":
            if not market.polymarket_token_id:
                continue
            row = {
                "symbol": market.symbol,
                "target_label": market.target_label,
                "polymarket_market_id": market.polymarket_market_id,
                "polymarket_token_id": market.polymarket_token_id,
                "polymarket_side": market.polymarket_side.value,
                "sx_market_hash": market.predict_fun_market_id,
                "sx_token_id": market.predict_fun_token_id,
                "sx_side": market.predict_fun_side.value,
                "polymarket_volume_usd": market.polymarket_volume_usd,
                "sx_volume_usd": market.predict_fun_volume_usd,
            }
        elif route == "myriad":
            if not market.myriad_market_id:
                continue
            myriad_execution_side = myriad_execution_side_for_route(market, "sx_myriad")
            row = {
                "symbol": market.symbol,
                "target_label": market.target_label,
                "myriad_market_id": market.myriad_market_id,
                "myriad_side": market.myriad_side.value if market.myriad_side is not None else None,
                "myriad_execution_side_for_sx_myriad": (
                    myriad_execution_side.value if myriad_execution_side is not None else None
                ),
                "myriad_execution_token_for_sx_myriad": myriad_execution_token_for_route(market, "sx_myriad"),
                "myriad_condition_id": market.myriad_condition_id,
                "sx_market_hash": market.predict_fun_market_id,
                "sx_token_id": market.predict_fun_token_id,
                "sx_side": market.predict_fun_side.value,
                "myriad_volume_usd": market.myriad_volume_usd,
                "sx_volume_usd": market.predict_fun_volume_usd,
            }
        else:
            continue
        rows.append(row)
        if len(rows) >= limit:
            break
    return rows


def _selected_market_rows(markets: list[MarketSpec], *, limit: int = 10) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for market in markets[:limit]:
        rows.append(
            {
                "symbol": market.symbol,
                "target_label": market.target_label,
                "sx_market_hash": market.predict_fun_market_id,
                "sx_token_id": market.predict_fun_token_id,
                "sx_side": market.predict_fun_side.value,
            }
        )
    return rows


def _sx_identity(market: MarketSpec) -> tuple[str | None, str]:
    return market.predict_fun_market_id, market.predict_fun_token_id


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Probe live SX Bet route matching on a constrained market subset"
    )
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--route", choices=("polymarket", "myriad"), default="polymarket")
    parser.add_argument("--contains", help="Substring filter applied to SX proposition titles")
    parser.add_argument("--limit", type=int, default=25, help="Maximum SX proposition markets to probe")
    parser.add_argument("--require-match", action="store_true", help="Exit non-zero when no matches are found")
    args = parser.parse_args()

    load_operator_env(args.config)
    app_config = load_config(args.config)
    sx_config = replace(
        app_config.sx_bet,
        enabled=True,
        api_key=app_config.sx_bet.api_key,
    )
    sx_resolver = SxBetMarketResolver(sx_config, scan_all=True, categories_to_scan=app_config.categories_to_scan)
    gamma: GammaMarketResolver | None = None
    myriad: MyriadMarketResolver | None = None
    try:
        sx_markets = await sx_resolver.resolve([])
        selected = _select_probe_markets(sx_markets, args.contains, args.limit)
        route_stats: dict[str, Any]
        if args.route == "polymarket":
            gamma = GammaMarketResolver(scan_all=True)
            await gamma.bootstrap([])
            resolved = await gamma.resolve(selected)
            route_stats = {
                "requested": gamma.last_resolution_stats.requested,
                "exact_id_matches": gamma.last_resolution_stats.exact_id_matches,
                "exact_title_matches": gamma.last_resolution_stats.exact_title_matches,
                "semantic_matches": gamma.last_resolution_stats.semantic_matches,
                "unresolved": gamma.last_resolution_stats.unresolved,
                "rejection_reasons": dict(gamma.last_resolution_stats.rejection_reasons),
            }
        else:
            myriad = MyriadMarketResolver(app_config.myriad_markets)
            resolved = await myriad.resolve(selected)
            matched_now = [market for market in resolved if market.myriad_market_id]
            route_stats = {
                "requested": len(selected),
                "matched": len(matched_now),
                "unresolved": max(0, len(selected) - len(matched_now)),
            }
        deduped = _deduplicate_markets(resolved)
        filtered = _filter_markets_by_volume(deduped, app_config)
        if args.route == "polymarket":
            matched = [market for market in filtered if market.polymarket_token_id]
        else:
            matched = [market for market in filtered if market.myriad_market_id]
        matched_keys = {_sx_identity(market) for market in matched}
        unmatched = [market for market in selected if _sx_identity(market) not in matched_keys]
        report = {
            "route": args.route,
            "contains": args.contains,
            "limit": args.limit,
            "sx_catalog_count": len(sx_markets),
            "selected_count": len(selected),
            "resolved_count": len(resolved),
            "deduped_count": len(deduped),
            "filtered_count": len(filtered),
            "matched_count": len(matched),
            "route_stats": route_stats,
            "selected_examples": _selected_market_rows(selected),
            "matched_examples": _matched_market_rows(matched, route=args.route),
            "unmatched_examples": _selected_market_rows(unmatched),
        }
        print(json.dumps(report, indent=2))
        if args.require_match and not matched:
            raise SystemExit(1)
    finally:
        await asyncio.gather(
            sx_resolver.close(),
            *(client.close() for client in (gamma, myriad) if client is not None),
            return_exceptions=True,
        )


if __name__ == "__main__":
    asyncio.run(main())
