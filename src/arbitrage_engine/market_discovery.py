from __future__ import annotations

import json
import logging
from dataclasses import replace
from datetime import datetime
from typing import Any

from .models import MarketSpec, PolymarketSide

LOGGER = logging.getLogger(__name__)


class GammaMarketResolver:
    def __init__(self, gamma_base_url: str = "https://gamma-api.polymarket.com") -> None:
        self._gamma_base_url = gamma_base_url

    async def resolve(self, markets: list[MarketSpec]) -> list[MarketSpec]:
        resolved: list[MarketSpec] = []
        for market in markets:
            if market.polymarket_token_id and market.polymarket_token_id != "replace-with-token-id":
                resolved.append(market)
                continue
            resolved.append(await self._resolve_one(market))
        return resolved

    async def _resolve_one(self, market: MarketSpec) -> MarketSpec:
        try:
            import aiohttp
        except ImportError as exc:
            raise RuntimeError("aiohttp is required for Polymarket market discovery") from exc

        query = f"{market.symbol} {market.target_label}".replace("-", " ")
        params = {
            "active": "true",
            "closed": "false",
            "limit": 50,
            "q": query,
        }
        url = f"{self._gamma_base_url}/markets"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=15) as response:
                response.raise_for_status()
                payload: list[dict[str, Any]] = await response.json()

        candidate = _best_candidate(payload, market)
        if candidate is None:
            raise RuntimeError(f"Could not discover Polymarket market for {market.symbol} {market.target_label}")

        token_ids = _parse_token_ids(candidate.get("clobTokenIds"))
        token_index = 0 if market.polymarket_side is PolymarketSide.YES else 1
        if len(token_ids) <= token_index:
            raise RuntimeError(f"Discovered market has no token id for {market.polymarket_side.value}: {candidate!r}")

        condition_id = candidate.get("conditionId") or candidate.get("condition_id")
        expires_at = _parse_optional_datetime(
            candidate.get("endDateIso")
            or candidate.get("endDate")
            or candidate.get("end_date_iso")
            or candidate.get("end_date")
        )
        LOGGER.info(
            "polymarket_market_discovered",
            extra={
                "_symbol": market.symbol,
                "_target_label": market.target_label,
                "_token_id": token_ids[token_index],
                "_condition_id": condition_id,
            },
        )
        return replace(
            market,
            polymarket_token_id=str(token_ids[token_index]),
            condition_id=str(condition_id) if condition_id else market.condition_id,
            expires_at=market.expires_at or expires_at,
        )


def _best_candidate(candidates: list[dict[str, Any]], market: MarketSpec) -> dict[str, Any] | None:
    symbol_terms = {part.lower() for part in market.symbol.replace("-", " ").split() if part}
    target_terms = {part.lower().replace("$", "").replace(",", "") for part in market.target_label.split() if part}

    best: tuple[int, dict[str, Any]] | None = None
    for candidate in candidates:
        text = " ".join(
            str(candidate.get(key, ""))
            for key in ("question", "slug", "description", "title")
        ).lower().replace("$", "").replace(",", "")
        score = sum(1 for term in symbol_terms | target_terms if term and term in text)
        if score <= 0:
            continue
        if best is None or score > best[0]:
            best = (score, candidate)
    return best[1] if best else None


def _parse_token_ids(raw: Any) -> list[str]:
    if isinstance(raw, str):
        parsed = json.loads(raw)
        return [str(item) for item in parsed]
    if isinstance(raw, list):
        return [str(item) for item in raw]
    return []


def _parse_optional_datetime(raw: Any) -> datetime | None:
    if not isinstance(raw, str) or not raw:
        return None
    return datetime.fromisoformat(raw.replace("Z", "+00:00"))
