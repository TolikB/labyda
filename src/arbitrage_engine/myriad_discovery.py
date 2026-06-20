from __future__ import annotations

import logging
from dataclasses import replace
from datetime import UTC, datetime
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
        self._session: Any | None = None

    def _get_session(self) -> Any:
        if self._session is None or self._session.closed:
            headers = {"Content-Type": "application/json"}
            if self._config.api_key:
                headers["x-api-key"] = self._config.api_key
            self._session = client_session(headers)
        return self._session

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    async def resolve(self, markets: list[MarketSpec]) -> list[MarketSpec]:
        if not self._config.enabled:
            return markets
        if (
            not self._scan_all
            and markets
            and all(
                market.myriad_market_id and not market.myriad_market_id.startswith("replace-with") for market in markets
            )
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
            exact_external = next(
                (
                    candidate
                    for candidate in myriad_markets
                    if market.polymarket_market_id and candidate.external_market_id == market.polymarket_market_id
                ),
                None,
            )
            if exact_external is not None:
                resolved.append(
                    replace(
                        market,
                        myriad_market_id=exact_external.market_id,
                        myriad_url=exact_external.public_url,
                        myriad_side=BinarySide.NO,
                        myriad_volume_usd=exact_external.volume_usd,
                        category=market.category or exact_external.category,
                        resolution_source=market.resolution_source or exact_external.resolution_source,
                        outcome_semantics=market.outcome_semantics or exact_external.outcome_semantics,
                        cutoff_at=market.cutoff_at or exact_external.expires_at,
                    )
                )
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
                    myriad_url=match.right.public_url,
                    myriad_side=match.right_side,
                    myriad_volume_usd=match.right.volume_usd,
                    category=market.category or match.right.category,
                    resolution_source=market.resolution_source or match.right.resolution_source,
                    outcome_semantics=market.outcome_semantics or match.right.outcome_semantics,
                    cutoff_at=market.cutoff_at or match.right.expires_at,
                )
            )
        return resolved

    async def _fetch_markets(self) -> list[dict[str, Any]]:
        try:
            import aiohttp

            _ = aiohttp
        except ImportError as exc:
            raise RuntimeError("aiohttp is required for Myriad market discovery") from exc

        url = f"{self._config.api_url.rstrip('/')}/markets"
        params = _market_query_params(self._config.chain_id)
        markets: list[dict[str, Any]] = []
        session = self._get_session()
        page = 1
        while True:
            async with session.get(url, params={**params, "page": page}, timeout=15) as response:
                response.raise_for_status()
                payload = await response.json()
            markets.extend(_extract_market_list(payload))
            if not _has_next_page(payload, page):
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


def _has_next_page(payload: Any, current_page: int) -> bool:
    if not isinstance(payload, dict):
        return False
    pagination = payload.get("pagination") or payload.get("pageInfo") or payload.get("page_info")
    if not isinstance(pagination, dict):
        return False
    for key in ("hasNext", "has_next", "hasNextPage", "has_next_page"):
        if key in pagination:
            return bool(pagination[key])
    next_page = pagination.get("nextPage") or pagination.get("next_page")
    if next_page not in (None, ""):
        return int(str(next_page)) > current_page
    total_pages = pagination.get("totalPages") or pagination.get("total_pages")
    if total_pages not in (None, ""):
        return current_page < int(str(total_pages))
    return False


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
        volume_usd=_market_volume(payload),
        public_url=_myriad_public_url(payload, market_id),
        category=_first_str(payload, ("category", "categorySlug", "category_slug", "group")),
        resolution_source=_first_str(payload, ("resolutionSource", "resolution_source", "oracle")),
        outcome_semantics=_first_str(payload, ("rules", "description", "resolutionRules")),
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
            return datetime.fromtimestamp(timestamp, tz=UTC)
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
        myriad_url=market.public_url,
        myriad_side=BinarySide.NO,
        rules_fingerprint=f"myriad:{market.market_id}",
        myriad_volume_usd=market.volume_usd,
        category=market.category,
        resolution_source=market.resolution_source,
        outcome_semantics=market.outcome_semantics,
        cutoff_at=market.expires_at,
    )


def _market_volume(payload: dict[str, Any]) -> float | None:
    for key in ("volumeNotional", "volume_notional", "volumeUsd", "volume_usd", "volume"):
        try:
            if payload.get(key) not in (None, ""):
                return float(payload[key])
        except (TypeError, ValueError):
            continue
    return None


def _myriad_public_url(payload: dict[str, Any], market_id: str) -> str:
    for key in ("url", "marketUrl", "market_url"):
        value = payload.get(key)
        if isinstance(value, str) and value.startswith(("https://", "http://")):
            return value
    return f"https://myriad.markets/markets/{market_id}"
