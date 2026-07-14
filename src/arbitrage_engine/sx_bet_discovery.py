from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

from .config import SxBetConfig
from .discovery_cpu import run_discovery_cpu
from .http import client_session
from .market_mapping import normalize_category
from .matcher import MarketText, SemanticMarketMatcher
from .models import BinarySide, MarketSpec

LOGGER = logging.getLogger(__name__)


class SxBetMarketResolver:
    def __init__(
        self,
        config: SxBetConfig,
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
            headers = {"Accept": "application/json"}
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
                market.venue_b_label == "SX Bet"
                and market.predict_fun_token_id
                and not market.predict_fun_token_id.startswith("replace-with")
                for market in markets
            )
        ):
            return markets
        try:
            payloads = await self._fetch_markets()
        except Exception as exc:
            LOGGER.exception("sx_bet_discovery_failed")
            if self._scan_all:
                raise RuntimeError(f"SX Bet discovery failed: {exc}") from exc
            return markets
        sx_markets = await run_discovery_cpu(_scan_all_market_texts, payloads, self._categories_to_scan)
        self._last_catalog_raw_count = len(payloads)
        self._last_catalog_parsed_count = len(sx_markets)
        if self._scan_all and not markets:
            return [spec for market in sx_markets for spec in _market_specs_from_text(market)]
        matcher = SemanticMarketMatcher()

        resolved: list[MarketSpec] = []
        for market in markets:
            if market.venue_b_label == "SX Bet" and market.predict_fun_token_id:
                resolved.append(market)
                continue
            if market.expires_at is None:
                resolved.append(market)
                continue
            exact_market = next(
                (
                    candidate
                    for candidate in sx_markets
                    if market.predict_fun_market_id and candidate.market_id == market.predict_fun_market_id
                ),
                None,
            )
            if exact_market is not None:
                side = market.predict_fun_side
                resolved.append(_apply_exact_market(market, exact_market, side))
                continue
            source = [
                MarketText(
                    platform="config",
                    market_id=market.symbol,
                    title=f"{market.symbol} {market.target_label}",
                    expires_at=market.expires_at,
                    yes_label=market.target_label,
                    no_label=market.symbol,
                )
            ]
            matches = matcher.match(source, sx_markets)
            if not matches:
                resolved.append(market)
                continue
            match = max(matches, key=lambda item: item.similarity)
            LOGGER.info(
                "sx_bet_market_discovered",
                extra={
                    "_symbol": market.symbol,
                    "_target_label": market.target_label,
                    "_sx_market_hash": match.right.market_id,
                    "_similarity": match.similarity,
                },
            )
            resolved.append(
                replace(
                    market,
                    predict_fun_token_id=_sx_token_id(match.right.market_id, match.right_side),
                    predict_fun_side=match.right_side,
                    venue_b_label="SX Bet",
                    predict_fun_market_id=match.right.market_id,
                    predict_fun_url=match.right.public_url,
                    predict_fun_fee_rate_bps=self._config.taker_fee_bps,
                    predict_fun_volume_usd=match.right.volume_usd,
                    category=market.category or match.right.category,
                    resolution_source=market.resolution_source or match.right.resolution_source,
                    outcome_semantics=market.outcome_semantics or match.right.outcome_semantics,
                    cutoff_at=market.cutoff_at or match.right.expires_at,
                )
            )
        return resolved

    async def _fetch_markets(self) -> list[dict[str, Any]]:
        if self._market_payload_cache is not None:
            return self._market_payload_cache
        try:
            import aiohttp

            _ = aiohttp
        except ImportError as exc:
            raise RuntimeError("aiohttp is required for SX Bet market discovery") from exc

        url = f"{self._config.api_base_url.rstrip('/')}/markets/active"
        markets: list[dict[str, Any]] = []
        session = self._get_session()
        pagination_key: str | None = None
        page = 1
        while True:
            payload = await _fetch_market_page(
                session,
                url,
                pagination_key=pagination_key,
                page=page,
            )
            page_markets = _extract_market_list(payload)
            if not page_markets:
                break
            markets.extend(page_markets)
            pagination_key = _next_pagination_key(payload)
            if pagination_key:
                continue
            if not _has_next_page(payload, page):
                break
            page += 1
        self._market_payload_cache = markets
        return markets


def _apply_exact_market(market: MarketSpec, exact_market: MarketText, side: BinarySide) -> MarketSpec:
    return replace(
        market,
        predict_fun_token_id=_sx_token_id(exact_market.market_id, side),
        predict_fun_side=side,
        venue_b_label="SX Bet",
        predict_fun_market_id=exact_market.market_id,
        predict_fun_url=exact_market.public_url,
        predict_fun_volume_usd=exact_market.volume_usd,
        category=market.category or exact_market.category,
        resolution_source=market.resolution_source or exact_market.resolution_source,
        outcome_semantics=market.outcome_semantics or exact_market.outcome_semantics,
        cutoff_at=market.cutoff_at or exact_market.expires_at,
    )


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


def _next_pagination_key(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    containers: list[Mapping[str, Any]] = [payload]
    data = payload.get("data")
    if isinstance(data, Mapping):
        containers.append(data)
    for container in containers:
        next_key = container.get("nextKey") or container.get("next_key") or container.get("paginationKey")
        if next_key not in (None, ""):
            return str(next_key)
    return None


async def _fetch_market_page(
    session: Any,
    url: str,
    *,
    pagination_key: str | None,
    page: int,
) -> dict[str, Any]:
    attempts: list[tuple[dict[str, Any], int]] = []
    if pagination_key:
        for page_size in (100, 50, 25):
            attempts.append(({"pageSize": page_size, "paginationKey": pagination_key}, 30))
            attempts.append(({"perPage": page_size, "paginationKey": pagination_key}, 30))
    else:
        for page_size in (100, 50, 25):
            attempts.append(({"pageSize": page_size}, 30))
            attempts.append(({"perPage": page_size}, 30))
        attempts.append(({"page": page, "perPage": 100}, 30))
        attempts.append(({"page": page, "perPage": 50}, 30))
    last_error: Exception | None = None
    for params, timeout_seconds in attempts:
        try:
            async with session.get(url, params=params, timeout=timeout_seconds) as response:
                response.raise_for_status()
                payload = await response.json()
            if isinstance(payload, dict):
                return payload
            return {"data": payload}
        except Exception as exc:
            last_error = exc
            continue
    if last_error is not None:
        raise last_error
    raise RuntimeError("SX Bet market discovery could not fetch a page")


def _has_next_page(payload: Any, current_page: int) -> bool:
    if not isinstance(payload, dict):
        return False
    pagination = payload.get("pagination") or payload.get("pageInfo") or payload.get("page_info") or payload.get(
        "data"
    )
    if not isinstance(pagination, dict):
        return False
    for key in ("hasNext", "has_next", "hasNextPage", "has_next_page"):
        if key in pagination:
            return bool(pagination[key])
    next_page = pagination.get("nextPage") or pagination.get("next_page")
    if next_page not in (None, ""):
        try:
            return int(str(next_page)) > current_page
        except (TypeError, ValueError):
            return False
    total_pages = pagination.get("totalPages") or pagination.get("total_pages")
    if total_pages not in (None, ""):
        try:
            return current_page < int(str(total_pages))
        except (TypeError, ValueError):
            return False
    return False


def _sx_market_text(payload: dict[str, Any]) -> MarketText | None:
    market_hash = _first_str(payload, ("marketHash", "market_hash", "id"))
    if not market_hash:
        return None
    title = _market_title(payload)
    expires_at_raw = _first_str(
        payload,
        ("startsAt", "startDate", "start_date", "eventStartTime", "event_start_time", "startsAtMs", "gameTime"),
    )
    if not title or not expires_at_raw:
        return None
    expires_at = _parse_datetime(expires_at_raw)
    if expires_at is None:
        return None
    outcome_one = _first_str(payload, ("outcomeOneName", "outcome_one_name", "homeTeamName")) or "Outcome One"
    outcome_two = _first_str(payload, ("outcomeTwoName", "outcome_two_name", "awayTeamName")) or "Outcome Two"
    return MarketText(
        platform="sx_bet",
        market_id=market_hash,
        title=title,
        expires_at=expires_at,
        yes_label=outcome_one,
        no_label=outcome_two,
        volume_usd=_market_volume(payload),
        public_url=_public_url(payload, market_hash),
        category=_market_category(payload) or "sports",
        resolution_source=_resolution_source(payload),
        outcome_semantics=_outcome_semantics(payload, outcome_one, outcome_two),
    )


def _market_specs_from_text(market: MarketText) -> list[MarketSpec]:
    proposition = _proposition_title(market)
    return [
        MarketSpec(
            symbol=proposition,
            target_label=market.yes_label,
            polymarket_token_id="",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id=_sx_token_id(market.market_id, BinarySide.NO),
            predict_fun_side=BinarySide.NO,
            venue_b_label="SX Bet",
            expires_at=market.expires_at,
            predict_fun_market_id=market.market_id,
            predict_fun_url=market.public_url,
            predict_fun_fee_rate_bps=0,
            rules_fingerprint=f"sx:{market.market_id}:yes",
            predict_fun_volume_usd=market.volume_usd,
            category=market.category,
            resolution_source=market.resolution_source,
            outcome_semantics=market.outcome_semantics,
            cutoff_at=market.expires_at,
        ),
        MarketSpec(
            symbol=proposition,
            target_label=market.no_label,
            polymarket_token_id="",
            polymarket_side=BinarySide.NO,
            predict_fun_token_id=_sx_token_id(market.market_id, BinarySide.YES),
            predict_fun_side=BinarySide.YES,
            venue_b_label="SX Bet",
            expires_at=market.expires_at,
            predict_fun_market_id=market.market_id,
            predict_fun_url=market.public_url,
            predict_fun_fee_rate_bps=0,
            rules_fingerprint=f"sx:{market.market_id}:no",
            predict_fun_volume_usd=market.volume_usd,
            category=market.category,
            resolution_source=market.resolution_source,
            outcome_semantics=market.outcome_semantics,
            cutoff_at=market.expires_at,
        ),
    ]


def _sx_token_id(market_hash: str, side: BinarySide) -> str:
    return f"{market_hash}:{side.value}"


def _proposition_title(market: MarketText) -> str:
    title = market.title.strip()
    yes_label = market.yes_label.strip()
    no_label = market.no_label.strip()
    title_parts = [part.strip() for part in title.split("|") if part.strip()]
    context = title_parts[0] if title_parts else title
    matchup = next((part for part in title_parts if " vs " in part.casefold()), "")

    if no_label.casefold() in {"the field", "field"}:
        competition = context.removeprefix("Outrights - ").strip()
        return f"Will {yes_label} win {_articleize(competition)}?"
    if yes_label.casefold().startswith("over ") and no_label.casefold().startswith("under "):
        threshold = yes_label[5:].strip()
        if matchup:
            return f"Will {matchup} total go over {threshold}?"
        return f"Will total go over {threshold}?"
    if matchup and yes_label.casefold() in matchup.casefold() and no_label.casefold() in matchup.casefold():
        return f"Will {yes_label} beat {no_label}?"
    spread = _spread_suffix(yes_label)
    if matchup and spread is not None:
        opponent = no_label.removesuffix(_spread_suffix(no_label) or "").strip()
        subject = yes_label.removesuffix(spread).strip()
        if subject and opponent:
            return f"Will {subject} cover {spread} vs {opponent}?"
    return f"{title} - {yes_label}"


def _articleize(value: str) -> str:
    if value.casefold().startswith("the "):
        return value
    return f"the {value}"


def _spread_suffix(value: str) -> str | None:
    compact = value.strip().split()
    if len(compact) < 2:
        return None
    suffix = compact[-1]
    if suffix.startswith(("+", "-")):
        return suffix
    return None


def _market_title(payload: Mapping[str, Any]) -> str | None:
    direct = _first_str(payload, ("title", "question", "name", "label"))
    if direct:
        return direct
    event = _first_str(payload, ("eventName", "event_name", "group1", "leagueLabel", "sportXeventId"))
    market_type = _first_str(payload, ("type", "marketType", "market_type"))
    if market_type is not None and market_type.isdigit():
        market_type = None
    line = _first_str(payload, ("line", "points", "total"))
    team_one = _first_str(payload, ("teamOneName", "homeTeamName", "outcomeOneName", "outcome_one_name"))
    team_two = _first_str(payload, ("teamTwoName", "awayTeamName", "outcomeTwoName", "outcome_two_name"))
    teams = " vs ".join([value for value in (team_one, team_two) if value])
    parts = [value for value in (event, teams, market_type, line) if value]
    return " | ".join(parts) if parts else None


def _first_str(payload: Mapping[str, Any], keys: tuple[str, ...]) -> str | None:
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
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    except ValueError:
        return None


def _market_volume(payload: Mapping[str, Any]) -> float | None:
    for key in ("volumeUsd", "volume_usd", "volume", "liquidityUsd", "liquidity_usd", "totalBetSizeUsd"):
        value = payload.get(key)
        if value in (None, ""):
            continue
        try:
            return float(str(value))
        except (TypeError, ValueError):
            continue
    return None


def _public_url(payload: Mapping[str, Any], market_hash: str) -> str | None:
    for key in ("url", "marketUrl", "market_url"):
        value = payload.get(key)
        if isinstance(value, str) and value.startswith(("https://", "http://")):
            return value
    return f"https://sx.bet/market/{market_hash}"


def _market_category(payload: Mapping[str, Any]) -> str | None:
    direct = _first_str(payload, ("sportLabel", "sport", "category", "leagueLabel", "league"))
    if direct:
        return "sports"
    tags = payload.get("tags")
    if isinstance(tags, Sequence) and not isinstance(tags, (str, bytes)):
        for tag in tags:
            if isinstance(tag, str) and tag.strip():
                return tag.strip()
    return None


def _resolution_source(payload: Mapping[str, Any]) -> str | None:
    direct = _first_str(payload, ("resultSource", "result_source", "resolutionSource", "resolution_source"))
    if direct:
        return direct
    league = _first_str(payload, ("leagueLabel", "league"))
    return f"Official {league} result" if league else "Official event result"


def _outcome_semantics(payload: Mapping[str, Any], outcome_one: str, outcome_two: str) -> str:
    market_type = _first_str(payload, ("type", "marketType", "market_type")) or "market"
    line = _first_str(payload, ("line", "points", "total"))
    line_suffix = f" line={line}" if line else ""
    return f"Outcome one={outcome_one}; outcome two={outcome_two}; type={market_type}{line_suffix}"


def _scan_all_market_texts(payloads: list[dict[str, Any]], allowed: set[str]) -> list[MarketText]:
    result: list[MarketText] = []
    for payload in payloads:
        market = _sx_market_text(payload)
        if market is None:
            continue
        if allowed and normalize_category(market.category) not in allowed:
            continue
        result.append(market)
    return result
