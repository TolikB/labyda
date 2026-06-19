from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import replace
from datetime import datetime
from typing import Any

from .http import client_session
from .matcher import normalize_text
from .models import MarketSpec, PolymarketSide

LOGGER = logging.getLogger(__name__)


class GammaMarketResolver:
    def __init__(self, gamma_base_url: str = "https://gamma-api.polymarket.com", *, scan_all: bool = False) -> None:
        self._gamma_base_url = gamma_base_url
        self._scan_all = scan_all
        self._session: Any | None = None
        self._semaphore = asyncio.Semaphore(20)

    def _get_session(self) -> Any:
        if self._session is None or self._session.closed:
            self._session = client_session()
        return self._session

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    async def resolve(self, markets: list[MarketSpec]) -> list[MarketSpec]:
        if self._scan_all:
            results = await asyncio.gather(*(self._resolve_one(market) for market in markets), return_exceptions=True)
            scan_results: list[MarketSpec] = []
            for market, result in zip(markets, results):
                if isinstance(result, BaseException):
                    LOGGER.info(
                        "polymarket_scan_all_market_skipped",
                        extra={"_symbol": market.symbol, "_reason": str(result)},
                    )
                    continue
                scan_results.append(result)
            return scan_results
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

        query = market.target_label or market.symbol
        params: dict[str, str | int]
        if market.polymarket_market_id:
            params = {"id": market.polymarket_market_id}
        else:
            params = {"active": "true", "closed": "false", "limit": 100, "q": query.replace("-", " ")}
        url = f"{self._gamma_base_url}/markets"
        session = self._get_session()
        async with self._semaphore:
            async with session.get(url, params=params, timeout=15) as response:
                response.raise_for_status()
                payload: list[dict[str, Any]] = await response.json()

        candidate = _best_candidate(payload, market)
        if candidate is None:
            raise RuntimeError(f"Could not discover Polymarket market for {market.symbol} {market.target_label}")

        token_id = _token_id_for_side(candidate, market.polymarket_side)
        if token_id is None:
            raise RuntimeError(f"Discovered market has no unambiguous {market.polymarket_side.value} token: {candidate!r}")

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
                "_token_id": token_id,
                "_condition_id": condition_id,
            },
        )
        return replace(
            market,
            polymarket_token_id=token_id,
            polymarket_market_id=str(candidate.get("id") or market.polymarket_market_id or "") or None,
            polymarket_url=market.polymarket_url or _polymarket_public_url(candidate),
            condition_id=str(condition_id) if condition_id else market.condition_id,
            neg_risk=_optional_bool(candidate, ("negRisk", "neg_risk", "isNegRisk")),
            expires_at=market.expires_at or expires_at,
            polymarket_volume_usd=_market_volume(candidate),
        )


def _best_candidate(candidates: list[dict[str, Any]], market: MarketSpec) -> dict[str, Any] | None:
    if market.polymarket_market_id:
        return next(
            (candidate for candidate in candidates if str(candidate.get("id") or "") == market.polymarket_market_id),
            None,
        )
    expected_title = normalize_text(market.target_label or market.symbol)
    for candidate in candidates:
        candidate_title = normalize_text(str(candidate.get("question") or candidate.get("title") or ""))
        if not expected_title or candidate_title != expected_title:
            continue
        candidate_expiry = _parse_optional_datetime(candidate.get("endDate") or candidate.get("endDateIso"))
        if market.expires_at is not None and candidate_expiry is not None:
            delta = abs((market.expires_at - candidate_expiry).total_seconds())
            if delta > 1800:
                continue
        return candidate
    return None


def _token_id_for_side(candidate: dict[str, Any], side: PolymarketSide) -> str | None:
    token_ids = _parse_token_ids(candidate.get("clobTokenIds"))
    outcomes = _parse_string_list(candidate.get("outcomes"))
    if len(token_ids) != len(outcomes):
        return None
    matches = [index for index, outcome in enumerate(outcomes) if outcome.strip().upper() == side.value]
    if len(matches) != 1:
        return None
    return token_ids[matches[0]]


def _parse_string_list(raw: Any) -> list[str]:
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return []
        return [str(item) for item in parsed] if isinstance(parsed, list) else []
    if isinstance(raw, list):
        return [str(item) for item in raw]
    return []


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


def _optional_bool(payload: dict[str, Any], keys: tuple[str, ...]) -> bool | None:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, bool):
            return value
        if isinstance(value, str) and value.lower() in {"true", "false", "1", "0"}:
            return value.lower() in {"true", "1"}
    return None


def _market_volume(payload: dict[str, Any]) -> float | None:
    for key in ("volumeClob", "volumeNum", "volume", "volume24hr"):
        try:
            if payload.get(key) not in (None, ""):
                return float(payload[key])
        except (TypeError, ValueError):
            continue
    return None


def _polymarket_public_url(payload: dict[str, Any]) -> str | None:
    for key in ("url", "marketUrl", "market_url"):
        value = payload.get(key)
        if isinstance(value, str) and value.startswith(("https://", "http://")):
            return value
    event_slug = payload.get("eventSlug") or payload.get("event_slug")
    events = payload.get("events")
    if not event_slug and isinstance(events, list):
        for event in events:
            if isinstance(event, dict) and event.get("slug"):
                event_slug = event["slug"]
                break
    slug = event_slug or payload.get("slug")
    return f"https://polymarket.com/event/{slug}" if slug else None
