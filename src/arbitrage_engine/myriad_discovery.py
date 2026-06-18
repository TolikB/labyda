from __future__ import annotations

import logging
from dataclasses import replace
from datetime import datetime
from typing import Any, cast

from .config import MyriadMarketsConfig
from .http import client_session
from .matcher import MarketText, SemanticMarketMatcher
from .models import BinarySide, MarketSpec

LOGGER = logging.getLogger(__name__)


class MyriadMarketResolver:
    def __init__(self, config: MyriadMarketsConfig, *, scan_all: bool = False) -> None:
        self._config = config
        self._scan_all = scan_all

    async def resolve(self, markets: list[MarketSpec]) -> list[MarketSpec]:
        if not self._config.enabled:
            return markets
        if not self._scan_all and markets and all(
            market.myriad_market_id and not market.myriad_market_id.startswith("replace-with") for market in markets
        ):
            return markets
        try:
            payloads = await self._fetch_markets()
        except Exception:
            LOGGER.exception("myriad_discovery_failed")
            return markets
        raw_myriad_markets = [_market_text(item) for item in payloads]
        myriad_markets = cast(list[MarketText], [item for item in raw_myriad_markets if item is not None])
        if self._scan_all and not markets:
            return [_market_spec_from_text(item) for item in myriad_markets]
        matcher = SemanticMarketMatcher()

        resolved: list[MarketSpec] = []
        for market in markets:
            if market.myriad_market_id and not market.myriad_market_id.startswith("replace-with"):
                resolved.append(market)
                continue
            if market.expires_at is None:
                resolved.append(market)
                continue
            source = [
                MarketText(
                    platform="config",
                    market_id=market.symbol,
                    title=f"{market.symbol} {market.target_label}",
                    expires_at=market.expires_at,
                )
            ]
            matches = matcher.match(source, myriad_markets)
            if not matches:
                resolved.append(market)
                continue
            match = max(matches, key=lambda item: item.similarity)
            LOGGER.info(
                "myriad_market_discovered",
                extra={
                    "_symbol": market.symbol,
                    "_target_label": market.target_label,
                    "_myriad_market_id": match.right.market_id,
                    "_similarity": match.similarity,
                },
            )
            resolved.append(
                replace(
                    market,
                    myriad_market_id=match.right.market_id,
                    myriad_side=match.right_side,
                )
            )
        return resolved

    async def _fetch_markets(self) -> list[dict[str, Any]]:
        try:
            import aiohttp
        except ImportError as exc:
            raise RuntimeError("aiohttp is required for Myriad market discovery") from exc

        headers = {"Content-Type": "application/json"}
        if self._config.api_key:
            headers["x-api-key"] = self._config.api_key
        url = f"{self._config.api_url.rstrip('/')}/markets"
        params = _market_query_params(self._config.chain_id)
        markets: list[dict[str, Any]] = []
        async with client_session(headers) as session:
            page = 1
            while True:
                async with session.get(url, params={**params, "page": page}, timeout=15) as response:
                    response.raise_for_status()
                    payload = await response.json()
                markets.extend(_extract_market_list(payload))
                pagination = payload.get("pagination") if isinstance(payload, dict) else None
                if not isinstance(pagination, dict) or not bool(pagination.get("hasNext")):
                    break
                page += 1
        return markets


def _extract_market_list(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("markets", "data", "items", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
            if isinstance(value, dict):
                nested = _extract_market_list(value)
                if nested:
                    return nested
    return []


def _market_query_params(chain_id: int) -> dict[str, int | str]:
    return {"network_id": chain_id, "trading_model": "ob", "state": "open", "limit": 100}


def _market_text(payload: dict[str, Any]) -> MarketText | None:
    market_id = _first_str(payload, ("id", "marketId", "market_id"))
    title = _first_str(payload, ("question", "title", "name", "slug"))
    expires_at_raw = _first_str(payload, ("expiresAt", "expires_at", "resolvedAt", "resolved_at", "expiry_timestamp"))
    if not market_id or not title or not expires_at_raw:
        return None
    expires_at = _parse_datetime(expires_at_raw)
    if expires_at is None:
        return None
    labels = _outcome_labels(payload)
    if labels is None:
        return None
    yes_label, no_label = labels
    return MarketText(
        platform="myriad",
        market_id=market_id,
        title=title,
        expires_at=expires_at,
        yes_label=yes_label,
        no_label=no_label,
        external_market_id=_polymarket_external_market_id(payload),
    )


def _outcome_labels(payload: dict[str, Any]) -> tuple[str, str] | None:
    outcomes = payload.get("outcomes") or payload.get("tokens") or payload.get("assets")
    if not isinstance(outcomes, list) or len(outcomes) < 2:
        return None
    by_id: dict[int, str] = {}
    by_label: dict[str, str] = {}
    for item in outcomes:
        if isinstance(item, dict):
            label = str(
                item.get("title")
                or item.get("name")
                or item.get("label")
                or item.get("outcome")
                or item.get("side")
                or ""
            ).strip()
            raw_id = item.get("id") if item.get("id") is not None else item.get("outcomeId")
            if raw_id is not None:
                try:
                    by_id[int(raw_id)] = label
                except (TypeError, ValueError):
                    pass
        else:
            label = str(item).strip()
        if label.upper() in {BinarySide.YES.value, BinarySide.NO.value}:
            by_label[label.upper()] = label
    yes_label = by_id.get(0) or by_label.get(BinarySide.YES.value)
    no_label = by_id.get(1) or by_label.get(BinarySide.NO.value)
    if not yes_label or not no_label:
        return None
    if yes_label.upper() != BinarySide.YES.value or no_label.upper() != BinarySide.NO.value:
        return None
    return yes_label, no_label


def _polymarket_external_market_id(payload: dict[str, Any]) -> str | None:
    sources = payload.get("externalSources") or payload.get("external_sources")
    if not isinstance(sources, list):
        return None
    for source in sources:
        if not isinstance(source, dict):
            continue
        provider = str(source.get("providerName") or source.get("provider_name") or "").lower()
        market_id = source.get("externalMarketId") or source.get("external_market_id")
        if provider == "polymarket" and market_id not in (None, ""):
            return str(market_id)
    return None


def _first_str(payload: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = payload.get(key)
        if value not in (None, ""):
            return str(value)
    return None


def _parse_datetime(raw: str) -> datetime | None:
    try:
        if raw.isdigit():
            timestamp = int(raw)
            if timestamp > 10_000_000_000:
                timestamp //= 1000
            return datetime.fromtimestamp(timestamp)
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def _market_spec_from_text(market: MarketText) -> MarketSpec:
    return MarketSpec(
        symbol=market.title,
        target_label=market.title,
        polymarket_token_id="",
        polymarket_market_id=market.external_market_id,
        polymarket_side=BinarySide.YES,
        predict_fun_token_id="",
        predict_fun_side=BinarySide.NO,
        expires_at=market.expires_at,
        myriad_market_id=market.market_id,
        myriad_side=BinarySide.NO,
        rules_fingerprint=f"myriad:{market.market_id}",
    )
