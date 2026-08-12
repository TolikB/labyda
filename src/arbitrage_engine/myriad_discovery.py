from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, cast

from .config import MyriadMarketsConfig
from .discovery_cpu import run_discovery_cpu
from .http import client_session
from .market_mapping import normalize_category
from .matcher import MarketText, SemanticMarketMatcher
from .models import BinarySide, MarketSpec

LOGGER = logging.getLogger(__name__)
_SPORTS_MATCH_EXPIRY_WINDOW_SECONDS = 7 * 24 * 60 * 60
_SX_MARKET_MIN_SIMILARITY = 0.78


class MyriadMarketResolver:
    def __init__(
        self,
        config: MyriadMarketsConfig,
        *,
        scan_all: bool = False,
        categories_to_scan: list[str] | None = None,
    ) -> None:
        self._config = config
        self._scan_all = scan_all
        self._categories_to_scan = {
            category for value in (categories_to_scan or []) if (category := normalize_category(value))
        }
        self._session: Any | None = None
        self._market_payload_cache: list[dict[str, Any]] | None = None
        self._last_catalog_raw_count = 0
        self._last_catalog_parsed_count = 0

    @property
    def last_catalog_counts(self) -> tuple[int, int]:
        return self._last_catalog_raw_count, self._last_catalog_parsed_count

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

    def invalidate_cache(self) -> None:
        self._market_payload_cache = None

    async def resolve(self, markets: list[MarketSpec]) -> list[MarketSpec]:
        if not self._config.enabled:
            return markets
        if (
            not self._scan_all
            and markets
            and all(
                market.myriad_market_id
                and not market.myriad_market_id.startswith("replace-with")
                and _has_complete_myriad_metadata(market)
                for market in markets
            )
        ):
            return markets
        try:
            payloads = await self._fetch_markets()
        except Exception as exc:
            LOGGER.exception("myriad_discovery_failed")
            if self._scan_all:
                raise RuntimeError(f"Myriad discovery failed: {exc}") from exc
            return markets
        myriad_markets = await run_discovery_cpu(_scan_all_market_texts, payloads, self._categories_to_scan)
        self._last_catalog_raw_count = len(payloads)
        self._last_catalog_parsed_count = len(myriad_markets)
        if self._scan_all and not markets:
            return [spec for item in myriad_markets for spec in _market_specs_from_text(item)]
        return await run_discovery_cpu(_resolve_market_specs, markets, myriad_markets)

    async def _fetch_markets(self) -> list[dict[str, Any]]:
        if self._market_payload_cache is not None:
            return self._market_payload_cache
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
        self._market_payload_cache = markets
        return markets


def _resolve_market_specs(markets: list[MarketSpec], myriad_markets: list[MarketText]) -> list[MarketSpec]:
    myriad_by_id = {candidate.market_id: candidate for candidate in myriad_markets}
    myriad_by_external_id = {
        candidate.external_market_id: candidate
        for candidate in myriad_markets
        if candidate.external_market_id is not None
    }
    resolved: list[MarketSpec] = []
    for market in markets:
        if market.myriad_market_id and not market.myriad_market_id.startswith("replace-with"):
            exact_market = myriad_by_id.get(market.myriad_market_id)
            resolved.append(_merge_existing_myriad_metadata(market, exact_market) if exact_market else market)
            continue
        if market.expires_at is None:
            resolved.append(market)
            continue
        exact_external = myriad_by_external_id.get(market.polymarket_market_id or "")
        if exact_external is not None:
            resolved.append(_merge_discovered_myriad_market(market, exact_external, side=BinarySide.NO))
            continue
        matcher = SemanticMarketMatcher(
            min_similarity=_min_similarity_for_market(market),
            expiry_window_seconds=_expiry_window_seconds_for_market(market),
        )
        source = [_source_market_text(market)]
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
        resolved.append(_merge_discovered_myriad_market(market, match.right, side=match.right_side))
    return resolved


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


def _has_complete_myriad_metadata(market: MarketSpec) -> bool:
    return bool(market.myriad_condition_id and market.myriad_collateral_token)


def _merge_discovered_myriad_market(
    market: MarketSpec,
    discovered: MarketText,
    *,
    side: BinarySide,
) -> MarketSpec:
    return replace(
        market,
        myriad_market_id=market.myriad_market_id or discovered.market_id,
        myriad_condition_id=market.myriad_condition_id or discovered.condition_id,
        myriad_collateral_token=market.myriad_collateral_token or discovered.collateral_token,
        myriad_url=market.myriad_url or discovered.public_url,
        myriad_side=side,
        myriad_volume_usd=market.myriad_volume_usd or discovered.volume_usd,
        category=market.category or discovered.category,
        resolution_source=market.resolution_source or discovered.resolution_source,
        outcome_semantics=market.outcome_semantics or discovered.outcome_semantics,
        cutoff_at=market.cutoff_at or discovered.expires_at,
    )


def _merge_existing_myriad_metadata(market: MarketSpec, discovered: MarketText) -> MarketSpec:
    return replace(
        market,
        myriad_condition_id=market.myriad_condition_id or discovered.condition_id,
        myriad_collateral_token=market.myriad_collateral_token or discovered.collateral_token,
        myriad_url=market.myriad_url or discovered.public_url,
        myriad_volume_usd=market.myriad_volume_usd or discovered.volume_usd,
        category=market.category or discovered.category,
        resolution_source=market.resolution_source or discovered.resolution_source,
        outcome_semantics=market.outcome_semantics or discovered.outcome_semantics,
        cutoff_at=market.cutoff_at or discovered.expires_at,
    )


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


def _source_market_text(market: MarketSpec) -> MarketText:
    if market.venue_b_label == "SX Bet" and market.symbol:
        title = market.symbol
    else:
        title = f"{market.symbol} {market.target_label}"
    expires_at = market.expires_at
    if expires_at is None:
        raise ValueError("market expiry is required for discovery matching")
    return MarketText(
        platform="config",
        market_id=market.symbol,
        title=title,
        expires_at=expires_at,
    )


def _expiry_window_seconds_for_market(market: MarketSpec) -> int:
    if normalize_category(market.category or "") == "sports" or market.venue_b_label == "SX Bet":
        return _SPORTS_MATCH_EXPIRY_WINDOW_SECONDS
    return 1_800


def _min_similarity_for_market(market: MarketSpec) -> float:
    if market.venue_b_label == "SX Bet":
        return _SX_MARKET_MIN_SIMILARITY
    return 0.85


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
        category=_market_category(payload),
        resolution_source=_first_str(payload, ("resolutionSource", "resolution_source", "oracle")),
        outcome_semantics=_first_str(payload, ("rules", "description", "resolutionRules")),
        condition_id=_market_condition_id(payload),
        collateral_token=_market_collateral_token(payload),
    )


def _outcome_labels(payload: dict[str, Any]) -> tuple[str, str] | None:
    outcomes = payload.get("outcomes") or payload.get("tokens") or payload.get("assets")
    if not isinstance(outcomes, list) or len(outcomes) != 2:
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
    explicit_yes = by_id.get(0)
    explicit_no = by_id.get(1)
    if explicit_yes and explicit_no:
        normalized = {explicit_yes.upper(), explicit_no.upper()}
        if normalized == {BinarySide.YES.value, BinarySide.NO.value}:
            if (
                explicit_yes.upper() != BinarySide.YES.value
                or explicit_no.upper() != BinarySide.NO.value
            ):
                return None
        elif explicit_yes.casefold() == explicit_no.casefold():
            return None
        return explicit_yes, explicit_no
    yes_label = by_label.get(BinarySide.YES.value)
    no_label = by_label.get(BinarySide.NO.value)
    if not yes_label or not no_label:
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


def _market_condition_id(payload: dict[str, Any]) -> str | None:
    direct = _first_str(payload, ("conditionId", "condition_id"))
    if direct:
        return direct
    condition = payload.get("condition")
    if isinstance(condition, Mapping):
        nested = condition.get("id") or condition.get("conditionId") or condition.get("condition_id")
        if nested not in (None, ""):
            return str(nested)
    return None


def _market_collateral_token(payload: dict[str, Any]) -> str | None:
    direct = _first_str(
        payload,
        ("collateralToken", "collateral_token", "collateralTokenAddress", "collateral_token_address"),
    )
    if direct:
        return direct
    token = payload.get("token")
    if isinstance(token, Mapping):
        nested = token.get("address") or token.get("tokenAddress") or token.get("token_address")
        if nested not in (None, ""):
            return str(nested)
    return None


def _parse_datetime(raw: str) -> datetime | None:
    try:
        if raw.isdigit():
            timestamp = int(raw)
            if timestamp > 10_000_000_000:
                timestamp //= 1000
            return datetime.fromtimestamp(timestamp, tz=UTC)
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    except ValueError:
        return None


def _market_spec_from_text(market: MarketText) -> MarketSpec:
    return _market_specs_from_text(market)[0]


def _market_specs_from_text(market: MarketText) -> list[MarketSpec]:
    standard_labels = (
        market.yes_label.upper() == BinarySide.YES.value
        and market.no_label.upper() == BinarySide.NO.value
    )
    common: dict[str, Any] = {
        "symbol": market.title,
        "polymarket_token_id": "",
        "polymarket_market_id": market.external_market_id,
        "predict_fun_token_id": "",
        "venue_b_label": "Myriad",
        "expires_at": market.expires_at,
        "myriad_market_id": market.market_id,
        "myriad_condition_id": market.condition_id,
        "myriad_collateral_token": market.collateral_token,
        "myriad_url": market.public_url,
        "myriad_volume_usd": market.volume_usd,
        "category": market.category,
        "resolution_source": market.resolution_source,
        "outcome_semantics": market.outcome_semantics,
        "cutoff_at": market.expires_at,
    }
    return [
        MarketSpec(
            target_label=market.title if standard_labels else market.yes_label,
            polymarket_side=BinarySide.YES,
            predict_fun_side=BinarySide.NO,
            myriad_side=BinarySide.NO,
            rules_fingerprint=f"myriad:{market.market_id}",
            **common,
        ),
        MarketSpec(
            target_label=market.title if standard_labels else market.no_label,
            polymarket_side=BinarySide.NO,
            predict_fun_side=BinarySide.YES,
            myriad_side=BinarySide.YES,
            rules_fingerprint=f"myriad:{market.market_id}:reverse",
            **common,
        ),
    ]


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


def _market_category(payload: Mapping[str, Any]) -> str | None:
    direct = (
        payload.get("category")
        or payload.get("categorySlug")
        or payload.get("category_slug")
        or payload.get("group")
    )
    if isinstance(direct, Mapping):
        direct = direct.get("title") or direct.get("name") or direct.get("slug")
    if isinstance(direct, str) and direct.strip():
        return direct.strip()
    topics = payload.get("topics")
    if isinstance(topics, Sequence) and not isinstance(topics, (str, bytes)):
        for topic in topics:
            if isinstance(topic, str) and topic.strip():
                return topic.strip()
    scoreboard = payload.get("scoreboard")
    if isinstance(scoreboard, Mapping) and scoreboard.get("type"):
        return "sports"
    if payload.get("moneyline") or payload.get("inPlay"):
        return "sports"
    tags = payload.get("tags")
    if isinstance(tags, Sequence) and not isinstance(tags, (str, bytes)):
        for tag in tags:
            if not isinstance(tag, Mapping):
                continue
            tag_type = str(tag.get("type") or "").strip().lower()
            if tag_type in {"league", "team", "round", "sport", "market-type"}:
                return "sports"
            for key in ("title", "name", "slug"):
                candidate = tag.get(key)
                if isinstance(candidate, str) and normalize_category(candidate) is not None:
                    return candidate.strip()
    return None


def _filter_scan_all_market_texts(markets: list[MarketText], allowed: set[str]) -> list[MarketText]:
    if not allowed:
        return markets
    return [market for market in markets if normalize_category(market.category) in allowed]


def _scan_all_market_texts(payloads: list[dict[str, Any]], allowed: set[str]) -> list[MarketText]:
    raw_myriad_markets = [_market_text(item) for item in payloads]
    myriad_markets = cast(list[MarketText], [item for item in raw_myriad_markets if item is not None])
    return _filter_scan_all_market_texts(myriad_markets, allowed)
