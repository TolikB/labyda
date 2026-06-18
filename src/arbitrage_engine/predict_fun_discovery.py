from __future__ import annotations

import logging
from dataclasses import replace
from datetime import datetime
from typing import Any

from .config import PredictFunConfig
from .http import client_session
from .models import BinarySide, MarketSpec

LOGGER = logging.getLogger(__name__)
PREDICT_MARKETS_PATH = "/v1/markets"


class PredictFunMarketResolver:
    def __init__(self, config: PredictFunConfig, *, scan_all: bool = False) -> None:
        self._config = config
        self._scan_all = scan_all

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
        if self._scan_all:
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
                    neg_risk=market.neg_risk if market.neg_risk is not None else _optional_bool(candidate, ("negRisk", "neg_risk", "isNegRisk")),
                )
            )
        return resolved

    async def _fetch_markets(self) -> list[dict[str, Any]]:
        try:
            import aiohttp
        except ImportError as exc:
            raise RuntimeError("aiohttp is required for Predict.fun market discovery") from exc

        headers: dict[str, str] = {}
        if self._config.api_key:
            headers["X-API-Key"] = self._config.api_key
            headers["Authorization"] = f"Bearer {self._config.api_key}"

        if self._config.api_base_url is None:
            return []
        base_url = self._config.api_base_url.rstrip("/")
        url = f"{base_url}{PREDICT_MARKETS_PATH}"
        async with client_session(headers) as session:
            async with session.get(url, params={"active": "true"}, timeout=15) as response:
                if response.status in (401, 403):
                    raise RuntimeError(
                        f"Predict.fun markets API rejected authentication ({response.status}); "
                        "set a valid PREDICT_FUN_API_KEY"
                    )
                response.raise_for_status()
                payload = await response.json()
        markets = _extract_market_list(payload)
        if not markets:
            raise RuntimeError(f"Predict.fun markets API returned no market records from {url}")
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


def _best_candidate(candidates: list[dict[str, Any]], market: MarketSpec) -> dict[str, Any] | None:
    symbol_terms = {part.lower() for part in market.symbol.replace("-", " ").split() if part}
    target_terms = {part.lower().replace("$", "").replace(",", "") for part in market.target_label.split() if part}
    terms = symbol_terms | target_terms

    best: tuple[int, dict[str, Any]] | None = None
    for candidate in candidates:
        text = _candidate_text(candidate)
        score = sum(1 for term in terms if term and term in text)
        if score <= 0:
            continue
        if best is None or score > best[0]:
            best = (score, candidate)
    return best[1] if best else None


def _candidate_text(candidate: dict[str, Any]) -> str:
    parts: list[str] = []
    for key in ("question", "title", "name", "slug", "description", "symbol"):
        value = candidate.get(key)
        if value is not None:
            parts.append(str(value))
    for outcome in _iter_outcomes(candidate):
        for key in ("name", "label", "outcome", "side"):
            value = outcome.get(key)
            if value is not None:
                parts.append(str(value))
    return " ".join(parts).lower().replace("$", "").replace(",", "")


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
            return _first_str(outcome, ("tokenId", "token_id", "id", "assetId", "asset_id"))

    indexed_tokens = _indexed_token_ids(candidate)
    if len(indexed_tokens) >= 2:
        return indexed_tokens[0 if side is BinarySide.YES else 1]
    return None


def _iter_outcomes(candidate: dict[str, Any]) -> list[dict[str, Any]]:
    for key in ("outcomes", "tokens", "assets"):
        value = candidate.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def _indexed_token_ids(candidate: dict[str, Any]) -> list[str]:
    for key in ("tokenIds", "token_ids", "clobTokenIds", "outcomeTokenIds"):
        value = candidate.get(key)
        if isinstance(value, list):
            return [str(item) for item in value]
        if isinstance(value, str) and value.startswith("["):
            import json

            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [str(item) for item in parsed]
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
        neg_risk=_optional_bool(payload, ("negRisk", "neg_risk", "isNegRisk")),
        rules_fingerprint=f"predict:{market_id}",
    )


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
