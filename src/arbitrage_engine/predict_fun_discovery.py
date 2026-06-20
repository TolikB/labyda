from __future__ import annotations

import logging
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any

from .config import PredictFunConfig
from .http import client_session
from .matcher import normalize_text, text_similarity
from .models import BinarySide, MarketSpec

LOGGER = logging.getLogger(__name__)
PREDICT_MARKETS_PATH = "/v1/markets"
BENIGN_TITLE_VARIANTS = {
    "above",
    "below",
    "over",
    "under",
    "exceed",
    "exceeds",
    "exceeding",
    "greater",
    "less",
    "more",
    "than",
}


class PredictFunMarketResolver:
    def __init__(self, config: PredictFunConfig, *, scan_all: bool = False) -> None:
        self._config = config
        self._scan_all = scan_all
        self._session: Any | None = None
        self._market_payload_cache: list[dict[str, Any]] | None = None

    def _get_session(self) -> Any:
        if self._session is None or self._session.closed:
            headers: dict[str, str] = {}
            if self._config.api_key:
                headers["X-API-Key"] = self._config.api_key
                headers["Authorization"] = f"Bearer {self._config.api_key}"
            self._session = client_session(headers)
        return self._session

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    async def resolve(self, markets: list[MarketSpec]) -> list[MarketSpec]:
        if not self._config.api_base_url:
            return markets
        if not self._scan_all and markets and all(
            market.predict_fun_token_id and not market.predict_fun_token_id.startswith("replace-with")
            for market in markets
        ):
            return markets

        resolved: list[MarketSpec] = []
        try:
            market_payloads = await self._fetch_markets()
        except Exception as exc:
            LOGGER.exception("predict_fun_discovery_failed")
            raise RuntimeError(f"Predict.fun discovery failed: {exc}") from exc
        if self._scan_all and not markets:
            return [spec for payload in market_payloads if (spec := _market_spec_from_payload(payload)) is not None]
        for market in markets:
            if market.predict_fun_token_id and not market.predict_fun_token_id.startswith("replace-with"):
                resolved.append(market)
                continue
            candidate = _best_candidate(market_payloads, market)
            if candidate is None:
                resolved.append(market)
                continue
            token_id = _token_id_for_side(candidate, market.predict_fun_side)
            if token_id is None:
                resolved.append(market)
                continue
            market_id = _first_str(candidate, ("id", "marketId", "market_id", "conditionId", "condition_id"))
            LOGGER.info(
                "predict_fun_market_discovered",
                extra={
                    "_symbol": market.symbol,
                    "_target_label": market.target_label,
                    "_token_id": token_id,
                    "_market_id": market_id,
                },
            )
            resolved.append(
                replace(
                    market,
                    predict_fun_token_id=token_id,
                    predict_fun_market_id=market.predict_fun_market_id or market_id,
                    predict_fun_url=market.predict_fun_url or _predict_fun_public_url(candidate, market_id),
                    predict_fun_neg_risk=_optional_bool(candidate, ("isNegRisk", "negRisk", "neg_risk")),
                    predict_fun_fee_rate_bps=_optional_int(candidate, ("feeRateBps", "fee_rate_bps")),
                    predict_fun_volume_usd=_market_volume(candidate),
                    category=market.category or _market_category(candidate),
                    resolution_source=market.resolution_source or _first_str(
                        candidate, ("resolutionSource", "resolution_source", "oracle")
                    ),
                    outcome_semantics=market.outcome_semantics or _first_str(
                        candidate, ("rules", "description", "resolutionRules")
                    ),
                    cutoff_at=market.cutoff_at or market.expires_at,
                )
            )
        return resolved

    async def _fetch_markets(self) -> list[dict[str, Any]]:
        if self._market_payload_cache is not None:
            return self._market_payload_cache
        try:
            import aiohttp
        except ImportError as exc:
            raise RuntimeError("aiohttp is required for Predict.fun market discovery") from exc

        if self._config.api_base_url is None:
            return []
        base_url = self._config.api_base_url.rstrip("/")
        url = f"{base_url}{PREDICT_MARKETS_PATH}"
        session = self._get_session()
        markets: list[dict[str, Any]] = []
        after: str | None = None
        while True:
            params = {"status": "OPEN", "includeStats": "true", "first": 100}
            if after:
                params["after"] = after
            async with session.get(url, params=params, timeout=15) as response:
                if response.status in (401, 403):
                    raise RuntimeError(
                        f"Predict.fun markets API rejected authentication ({response.status}); "
                        "set a valid PREDICT_FUN_API_KEY"
                    )
                response.raise_for_status()
                payload = await response.json()
            markets.extend(_extract_market_list(payload))
            cursor = _next_cursor(payload, after)
            if cursor is None:
                break
            after = cursor
        if not markets:
            raise RuntimeError(f"Predict.fun markets API returned no market records from {url}")
        self._market_payload_cache = markets
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


def _next_cursor(payload: Any, current: str | None) -> str | None:
    if not isinstance(payload, dict):
        return None
    containers = [payload]
    for key in ("data", "pageInfo", "page_info", "pagination"):
        value = payload.get(key)
        if isinstance(value, dict):
            containers.append(value)
            for nested_key in ("pageInfo", "page_info", "pagination"):
                nested = value.get(nested_key)
                if isinstance(nested, dict):
                    containers.append(nested)
    for container in containers:
        has_next = container.get("hasNextPage", container.get("has_next_page", container.get("hasNext")))
        if has_next is False:
            continue
        for key in ("nextCursor", "next_cursor", "endCursor", "end_cursor", "after", "cursor"):
            value = container.get(key)
            if isinstance(value, dict):
                value = value.get("after") or value.get("next") or value.get("endCursor")
            if value not in (None, ""):
                cursor = str(value)
                if cursor != current:
                    return cursor
    return None


def _best_candidate(candidates: list[dict[str, Any]], market: MarketSpec) -> dict[str, Any] | None:
    if market.predict_fun_market_id:
        exact = next(
            (
                candidate
                for candidate in candidates
                if _first_str(candidate, ("id", "marketId", "market_id", "conditionId", "condition_id"))
                == market.predict_fun_market_id
            ),
            None,
        )
        if exact is not None:
            return exact
    symbol_text = normalize_text(market.symbol)
    target_text = normalize_text(market.target_label)
    expected_title = market.symbol if symbol_text == target_text else f"{market.symbol} {market.target_label}"
    matches: list[tuple[float, str, dict[str, Any]]] = []
    for candidate in candidates:
        candidate_title = _first_str(candidate, ("question", "title", "name")) or ""
        score = _strict_title_score(expected_title, candidate_title)
        if score < 0.85:
            continue
        candidate_expiry_raw = _first_str(candidate, ("expiresAt", "expires_at", "endDate", "end_date", "expiry"))
        candidate_expiry = _parse_datetime(candidate_expiry_raw) if candidate_expiry_raw else None
        if market.expires_at is not None:
            if candidate_expiry is None:
                continue
            left = market.expires_at if market.expires_at.tzinfo is not None else market.expires_at.replace(tzinfo=timezone.utc)
            right = candidate_expiry if candidate_expiry.tzinfo is not None else candidate_expiry.replace(tzinfo=timezone.utc)
            if abs((left - right).total_seconds()) > 1_800:
                continue
        candidate_id = _first_str(candidate, ("id", "marketId", "market_id", "conditionId", "condition_id")) or ""
        matches.append((score, candidate_id, candidate))
    if not matches:
        return None
    matches.sort(key=lambda item: item[0], reverse=True)
    if len(matches) > 1 and matches[0][1] != matches[1][1] and abs(matches[0][0] - matches[1][0]) <= 0.01:
        LOGGER.error(
            "predict_fun_ambiguous_title_match_rejected",
            extra={"_expected_title": expected_title, "_candidate_ids": [matches[0][1], matches[1][1]]},
        )
        return None
    return matches[0][2]


def _strict_title_score(expected_title: str, candidate_title: str) -> float:
    expected_normalized = normalize_text(expected_title)
    candidate_normalized = normalize_text(candidate_title)
    if not expected_normalized or not candidate_normalized:
        return 0.0
    if expected_normalized == candidate_normalized:
        return 1.0
    expected_tokens = set(expected_normalized.split())
    candidate_tokens = set(candidate_normalized.split())
    expected_core = expected_tokens - BENIGN_TITLE_VARIANTS
    candidate_core = candidate_tokens - BENIGN_TITLE_VARIANTS
    if expected_core != candidate_core:
        return 0.0
    return max(0.85, text_similarity(expected_title, candidate_title))


def _token_id_for_side(candidate: dict[str, Any], side: BinarySide) -> str | None:
    direct_keys = (
        f"{side.value.lower()}TokenId",
        f"{side.value.lower()}_token_id",
        f"{side.value.lower()}Token",
        f"{side.value.lower()}_token",
    )
    token_id = _first_str(candidate, direct_keys)
    if token_id:
        return token_id

    outcomes = _iter_outcomes(candidate)
    for outcome in outcomes:
        label = str(
            outcome.get("side") or outcome.get("name") or outcome.get("label") or outcome.get("outcome") or ""
        ).upper()
        if label == side.value:
            return _first_str(
                outcome,
                ("tokenId", "token_id", "onChainId", "on_chain_id", "id", "assetId", "asset_id"),
            )

    return None


def _iter_outcomes(candidate: dict[str, Any]) -> list[dict[str, Any]]:
    for key in ("outcomes", "tokens", "assets"):
        value = candidate.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def _first_str(payload: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = payload.get(key)
        if value not in (None, ""):
            return str(value)
    return None


def _optional_bool(payload: dict[str, Any], keys: tuple[str, ...]) -> bool | None:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.lower()
            if lowered in ("true", "1", "yes"):
                return True
            if lowered in ("false", "0", "no"):
                return False
    return None


def _optional_int(payload: dict[str, Any], keys: tuple[str, ...]) -> int | None:
    for key in keys:
        value = payload.get(key)
        if value not in (None, ""):
            try:
                return int(str(value))
            except (TypeError, ValueError):
                return None
    return None


def _market_spec_from_payload(payload: dict[str, Any]) -> MarketSpec | None:
    market_id = _first_str(payload, ("id", "marketId", "market_id", "conditionId", "condition_id"))
    title = _first_str(payload, ("question", "title", "name", "slug"))
    expires_raw = _first_str(payload, ("expiresAt", "expires_at", "endDate", "end_date", "expiry"))
    no_token_id = _token_id_for_side(payload, BinarySide.NO)
    if not market_id or not title or not expires_raw or not no_token_id:
        return None
    expires_at = _parse_datetime(expires_raw)
    if expires_at is None:
        return None
    return MarketSpec(
        symbol=title,
        target_label=title,
        polymarket_token_id="",
        polymarket_side=BinarySide.YES,
        predict_fun_token_id=no_token_id,
        predict_fun_side=BinarySide.NO,
        expires_at=expires_at,
        predict_fun_market_id=market_id,
        predict_fun_url=_predict_fun_public_url(payload, market_id),
        predict_fun_neg_risk=_optional_bool(payload, ("isNegRisk", "negRisk", "neg_risk")),
        predict_fun_fee_rate_bps=_optional_int(payload, ("feeRateBps", "fee_rate_bps")),
        rules_fingerprint=f"predict:{market_id}",
        predict_fun_volume_usd=_market_volume(payload),
        category=_market_category(payload),
        resolution_source=_first_str(payload, ("resolutionSource", "resolution_source", "oracle")),
        outcome_semantics=_first_str(payload, ("rules", "description", "resolutionRules")),
        cutoff_at=expires_at,
    )


def _parse_datetime(raw: str) -> datetime | None:
    try:
        if raw.isdigit():
            timestamp = int(raw)
            if timestamp > 10_000_000_000:
                timestamp //= 1000
            return datetime.fromtimestamp(timestamp, tz=timezone.utc)
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def _market_volume(payload: dict[str, Any]) -> float | None:
    for key in ("volumeUsd", "volume_usd", "volume24h", "volume"):
        try:
            if payload.get(key) not in (None, ""):
                return float(payload[key])
        except (TypeError, ValueError):
            continue
    return None


def _market_category(payload: dict[str, Any]) -> str | None:
    direct = _first_str(payload, ("category", "categorySlug", "category_slug", "group"))
    if direct:
        return direct
    tags = payload.get("tags")
    if isinstance(tags, list):
        for tag in tags:
            if isinstance(tag, dict):
                value = _first_str(tag, ("name", "label", "slug"))
            else:
                value = str(tag) if tag not in (None, "") else None
            if value:
                return value
    return None


def _predict_fun_public_url(payload: dict[str, Any], market_id: str | None) -> str | None:
    for key in ("url", "marketUrl", "market_url"):
        value = payload.get(key)
        if isinstance(value, str) and value.startswith(("https://", "http://")):
            return value
    return f"https://predict.fun/market/{market_id}" if market_id else None
