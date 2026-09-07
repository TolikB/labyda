"""Opinion.trade market discovery.

Opinion occupies the generic second-leg MarketSpec slot, exactly like SX Bet, so
a resolved spec carries ``venue_b_label="Opinion"`` and a
``"<marketId>:<tokenId>"`` execution token. Only binary markets are eligible;
categorical parents are skipped because the engine hedges two-outcome books.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Sequence
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

from .config import OpinionConfig
from .connectors.opinion import execution_token, unwrap_envelope
from .discovery_cpu import run_discovery_cpu
from .http import client_session
from .market_mapping import normalize_category
from .matcher import MarketText, SemanticMarketMatcher
from .models import BinarySide, MarketSpec, opposite_binary_side

LOGGER = logging.getLogger(__name__)

_CATALOG_FALLBACK_TTL_SECONDS = 15 * 60
_MAX_MARKET_PAGES = 500
_SPORTS_MATCH_EXPIRY_WINDOW_SECONDS = 7 * 24 * 60 * 60
_DEFAULT_MATCH_EXPIRY_WINDOW_SECONDS = 1_800
_SPORTS_MIN_SIMILARITY = 0.78
_DEFAULT_MIN_SIMILARITY = 0.85

# Opinion market status codes; only Activated markets are tradable.
_STATUS_ACTIVATED = 2
_MARKET_TYPE_BINARY = 0


class OpinionMarketResolver:
    """Resolve Opinion.trade counterparts for discovered markets."""

    def __init__(
        self,
        config: OpinionConfig,
        *,
        scan_all: bool = False,
        categories_to_scan: list[str] | None = None,
        monotonic: Callable[[], float] | None = None,
    ) -> None:
        self._config = config
        self._scan_all = scan_all
        self._categories_to_scan = {
            category for value in (categories_to_scan or []) if (category := normalize_category(value))
        }
        self._monotonic = monotonic or time.monotonic
        self._session: Any | None = None
        self._market_payload_cache: list[dict[str, Any]] | None = None
        self._last_good_market_texts: tuple[MarketText, ...] | None = None
        self._last_good_market_texts_at: float | None = None
        self._last_good_catalog_raw_count = 0
        self._last_catalog_raw_count = 0
        self._last_catalog_parsed_count = 0

    @property
    def last_catalog_counts(self) -> tuple[int, int]:
        return self._last_catalog_raw_count, self._last_catalog_parsed_count

    def _get_session(self) -> Any:
        if self._session is None or self._session.closed:
            headers = {"Accept": "application/json"}
            if self._config.api_key:
                headers["apikey"] = self._config.api_key
            self._session = client_session(headers)
        return self._session

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None
        self._market_payload_cache = None
        self._last_good_market_texts = None
        self._last_good_market_texts_at = None

    def invalidate_cache(self) -> None:
        self._market_payload_cache = None

    async def resolve(self, markets: list[MarketSpec]) -> list[MarketSpec]:
        if not self._config.enabled:
            return markets
        if (
            not self._scan_all
            and markets
            and all(
                market.venue_b_label == "Opinion"
                and market.predict_fun_token_id
                and not market.predict_fun_token_id.startswith("replace-with")
                for market in markets
            )
        ):
            return markets
        try:
            payloads = await self._fetch_markets()
        except Exception as exc:
            fallback = self._recent_last_good_catalog()
            if fallback is None:
                LOGGER.exception("opinion_discovery_failed")
                if self._scan_all:
                    raise RuntimeError(f"Opinion discovery failed: {exc}") from exc
                return markets
            LOGGER.warning(
                "opinion_discovery_using_recent_last_good_catalog",
                extra={"_catalog_market_count": len(fallback), "_error_type": type(exc).__name__},
            )
            opinion_markets = list(fallback)
            self._last_catalog_raw_count = self._last_good_catalog_raw_count
        else:
            opinion_markets = await run_discovery_cpu(
                _scan_all_market_texts, payloads, self._categories_to_scan
            )
            self._last_catalog_raw_count = len(payloads)
            self._last_good_catalog_raw_count = len(payloads)
            self._last_good_market_texts = tuple(opinion_markets)
            self._last_good_market_texts_at = self._monotonic()
        self._last_catalog_parsed_count = len(opinion_markets)
        if self._scan_all and not markets:
            return [spec for item in opinion_markets for spec in _market_specs_from_text(item)]
        return await run_discovery_cpu(
            _resolve_market_specs, markets, opinion_markets, not self._scan_all
        )

    async def _fetch_markets(self) -> list[dict[str, Any]]:
        if self._market_payload_cache is not None:
            return self._market_payload_cache
        try:
            import aiohttp

            _ = aiohttp
        except ImportError as exc:
            raise RuntimeError("aiohttp is required for Opinion market discovery") from exc

        url = f"{self._config.api_base_url.rstrip('/')}/market"
        limit = max(1, min(self._config.market_page_limit, 20))
        session = self._get_session()
        markets: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        for page in range(1, _MAX_MARKET_PAGES + 1):
            params = {
                "page": page,
                "limit": limit,
                "status": "activated",
                "marketType": _MARKET_TYPE_BINARY,
                "chainId": str(self._config.chain_id),
            }
            async with session.get(url, params=params, timeout=20) as response:
                response.raise_for_status()
                payload = await response.json()
            result = unwrap_envelope(payload, "/market")
            batch = result.get("list") if isinstance(result, dict) else None
            if not isinstance(batch, list) or not batch:
                break
            fresh = [item for item in batch if isinstance(item, dict)]
            markets.extend(fresh)
            for item in fresh:
                identity = item.get("marketId")
                if identity is not None:
                    seen_ids.add(str(identity))
            total = _optional_int(result.get("total")) if isinstance(result, dict) else None
            if total is not None and len(seen_ids) >= total:
                break
            if len(batch) < limit:
                break
        else:
            raise RuntimeError(f"Opinion market discovery exceeded {_MAX_MARKET_PAGES} pages")
        self._market_payload_cache = markets
        return markets

    def _recent_last_good_catalog(self) -> tuple[MarketText, ...] | None:
        if self._last_good_market_texts is None or self._last_good_market_texts_at is None:
            return None
        if self._monotonic() - self._last_good_market_texts_at > _CATALOG_FALLBACK_TTL_SECONDS:
            return None
        return self._last_good_market_texts


def _resolve_market_specs(
    markets: list[MarketSpec],
    opinion_markets: list[MarketText],
    log_discovered: bool,
) -> list[MarketSpec]:
    by_id = {candidate.market_id: candidate for candidate in opinion_markets}
    by_condition_id = {
        candidate.condition_id: candidate
        for candidate in opinion_markets
        if candidate.condition_id is not None
    }
    resolved: list[MarketSpec] = []
    for market in markets:
        if market.venue_b_label == "Opinion" and market.predict_fun_token_id:
            resolved.append(market)
            continue
        if market.expires_at is None:
            resolved.append(market)
            continue
        exact = by_id.get(market.predict_fun_market_id or "") or by_condition_id.get(
            market.condition_id or ""
        )
        if exact is not None:
            side = opposite_binary_side(market.polymarket_side)
            resolved.append(_apply_opinion_market(market, exact, side, "exact_id"))
            continue
        matcher = SemanticMarketMatcher(
            min_similarity=_min_similarity_for_market(market),
            expiry_window_seconds=_expiry_window_seconds_for_market(market),
        )
        matches = matcher.match([_source_market_text(market)], opinion_markets)
        if not matches:
            resolved.append(market)
            continue
        match = max(matches, key=lambda item: item.similarity)
        if log_discovered:
            LOGGER.info(
                "opinion_market_discovered",
                extra={
                    "_symbol": market.symbol,
                    "_target_label": market.target_label,
                    "_opinion_market_id": match.right.market_id,
                    "_similarity": match.similarity,
                },
            )
        resolved.append(_apply_opinion_market(market, match.right, match.right_side, "semantic_title"))
    return resolved


def _apply_opinion_market(
    market: MarketSpec,
    discovered: MarketText,
    side: BinarySide,
    mapping_strategy: str,
) -> MarketSpec:
    token_id = _opinion_token_id(discovered, side)
    if token_id is None:
        return market
    return replace(
        market,
        predict_fun_token_id=token_id,
        predict_fun_side=side,
        venue_b_label="Opinion",
        predict_fun_market_id=discovered.market_id,
        predict_fun_url=discovered.public_url,
        predict_fun_volume_usd=market.predict_fun_volume_usd or discovered.volume_usd,
        category=market.category or discovered.category,
        resolution_source=market.resolution_source or discovered.resolution_source,
        outcome_semantics=market.outcome_semantics or discovered.outcome_semantics,
        cutoff_at=_earliest_cutoff(market.cutoff_at, market.expires_at, discovered.expires_at),
        mapping_strategy=mapping_strategy,
    )


def _market_specs_from_text(market: MarketText) -> list[MarketSpec]:
    yes_token = _opinion_token_id(market, BinarySide.YES)
    no_token = _opinion_token_id(market, BinarySide.NO)
    if yes_token is None or no_token is None:
        return []
    specs: list[MarketSpec] = []
    for target_label, polymarket_side, hedge_side, hedge_token in (
        (market.yes_label, BinarySide.YES, BinarySide.NO, no_token),
        (market.no_label, BinarySide.NO, BinarySide.YES, yes_token),
    ):
        specs.append(
            MarketSpec(
                symbol=market.title,
                target_label=target_label,
                polymarket_token_id="",
                polymarket_side=polymarket_side,
                predict_fun_token_id=hedge_token,
                predict_fun_side=hedge_side,
                venue_b_label="Opinion",
                expires_at=market.expires_at,
                predict_fun_market_id=market.market_id,
                predict_fun_url=market.public_url,
                rules_fingerprint=f"opinion:{market.market_id}:{polymarket_side.value.lower()}",
                predict_fun_volume_usd=market.volume_usd,
                category=market.category,
                resolution_source=market.resolution_source,
                outcome_semantics=market.outcome_semantics,
                cutoff_at=market.expires_at,
            )
        )
    return specs


def _scan_all_market_texts(
    payloads: Sequence[dict[str, Any]],
    categories: set[str],
) -> list[MarketText]:
    texts: list[MarketText] = []
    for payload in payloads:
        text = _market_text(payload)
        if text is None:
            continue
        if categories and normalize_category(text.category) not in categories:
            continue
        texts.append(text)
    return texts


def _market_text(payload: dict[str, Any]) -> MarketText | None:
    market_id = _optional_str(payload.get("marketId"))
    title = _optional_str(payload.get("marketTitle"))
    cutoff_at = _parse_epoch(payload.get("cutoffAt"))
    if not market_id or not title or cutoff_at is None:
        return None
    if _optional_int(payload.get("marketType")) not in (None, _MARKET_TYPE_BINARY):
        return None
    if _optional_int(payload.get("status")) not in (None, _STATUS_ACTIVATED):
        return None
    yes_token = _optional_str(payload.get("yesTokenId"))
    no_token = _optional_str(payload.get("noTokenId"))
    if not yes_token or not no_token:
        return None
    return MarketText(
        platform="opinion",
        market_id=market_id,
        title=title,
        expires_at=cutoff_at,
        yes_label=_optional_str(payload.get("yesLabel")) or "YES",
        no_label=_optional_str(payload.get("noLabel")) or "NO",
        volume_usd=_optional_float(payload.get("volume")),
        public_url=_public_url(payload, market_id),
        category=_market_category(payload),
        resolution_source=_optional_str(payload.get("resolutionSource")),
        outcome_semantics=_optional_str(payload.get("rules") or payload.get("description")),
        condition_id=_optional_str(payload.get("conditionId")),
        # The outcome token ids ride in collateral_token so the CPU-bound
        # matcher stays on the shared MarketText contract.
        collateral_token=f"{yes_token}|{no_token}",
    )


def _opinion_token_id(market: MarketText, side: BinarySide) -> str | None:
    tokens = (market.collateral_token or "").split("|")
    if len(tokens) != 2 or not all(tokens):
        return None
    outcome_token = tokens[0] if side is BinarySide.YES else tokens[1]
    return execution_token(market.market_id, outcome_token)


def _market_category(payload: dict[str, Any]) -> str | None:
    labels = payload.get("labels")
    if isinstance(labels, list):
        for label in labels:
            normalized = normalize_category(_optional_str(label))
            if normalized:
                return normalized
    return normalize_category(_optional_str(payload.get("category")))


def _public_url(payload: dict[str, Any], market_id: str) -> str:
    slug = _optional_str(payload.get("slug"))
    return f"https://app.opinion.trade/market/{slug or market_id}"


def _source_market_text(market: MarketSpec) -> MarketText:
    expires_at = market.expires_at
    if expires_at is None:
        raise ValueError("market expiry is required for discovery matching")
    title = market.symbol if market.venue_b_label == "SX Bet" else f"{market.symbol} {market.target_label}"
    return MarketText(
        platform="config",
        market_id=market.symbol,
        title=title,
        expires_at=expires_at,
    )


def _expiry_window_seconds_for_market(market: MarketSpec) -> int:
    if normalize_category(market.category or "") == "sports" or market.venue_b_label == "SX Bet":
        return _SPORTS_MATCH_EXPIRY_WINDOW_SECONDS
    return _DEFAULT_MATCH_EXPIRY_WINDOW_SECONDS


def _min_similarity_for_market(market: MarketSpec) -> float:
    if market.venue_b_label == "SX Bet":
        return _SPORTS_MIN_SIMILARITY
    return _DEFAULT_MIN_SIMILARITY


def _earliest_cutoff(*values: datetime | None) -> datetime | None:
    normalized = [
        value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        for value in values
        if value is not None
    ]
    return min((value.astimezone(UTC) for value in normalized), default=None)


def _parse_epoch(value: Any) -> datetime | None:
    seconds = _optional_int(value)
    if seconds is None or seconds <= 0:
        return None
    if seconds > 10_000_000_000:
        seconds //= 1_000
    return datetime.fromtimestamp(seconds, tz=UTC)


def _optional_str(value: Any) -> str | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    return text or None


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
