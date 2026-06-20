from __future__ import annotations

import asyncio
import contextlib
import email.utils
import json
import logging
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any

from .http import client_session
from .matcher import normalize_text
from .models import MarketSpec, PolymarketSide

LOGGER = logging.getLogger(__name__)

_GAMMA_ID_BATCH_SIZE = 50
_MAX_CLOB_PAGES = 200
_MAX_HTTP_ATTEMPTS = 3
_MAX_RETRY_AFTER_SECONDS = 30.0
_MIN_REQUEST_INTERVAL_SECONDS = 0.25

GammaPayload = Mapping[str, Any]


class GammaCacheUnavailable(RuntimeError):
    """Raised when Gamma discovery has no complete, usable local snapshot."""


@dataclass(frozen=True)
class _GammaSnapshot:
    markets: tuple[GammaPayload, ...]
    by_id: Mapping[str, GammaPayload]
    by_title: Mapping[str, tuple[GammaPayload, ...]]
    fetched_at: datetime | None
    generation: int
    usable: bool


def _empty_snapshot() -> _GammaSnapshot:
    return _GammaSnapshot((), MappingProxyType({}), MappingProxyType({}), None, 0, False)


class GammaMarketResolver:
    def __init__(
        self,
        gamma_base_url: str = "https://gamma-api.polymarket.com",
        *,
        scan_all: bool = False,
        refresh_interval_seconds: float = 300.0,
        max_stale_seconds: float = 900.0,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._gamma_base_url = gamma_base_url
        self._scan_all = scan_all
        self._refresh_interval_seconds = refresh_interval_seconds
        self._max_stale_seconds = max_stale_seconds
        self._now = now or (lambda: datetime.now(UTC))
        self._session: Any | None = None
        self._snapshot = _empty_snapshot()
        self._refresh_lock = asyncio.Lock()
        self._refresh_task: asyncio.Task[None] | None = None
        self._refresh_http_requests = 0
        self._refresh_429s = 0
        self._refresh_pages = 0
        self._refresh_records = 0
        self._last_http_request_at = 0.0
        self._seed_market_ids: tuple[str, ...] = ()

    def _get_session(self) -> Any:
        if self._session is None or self._session.closed:
            self._session = client_session()
        return self._session

    async def bootstrap(self, markets: Sequence[MarketSpec] = ()) -> None:
        self._seed_market_ids = tuple(
            dict.fromkeys(market.polymarket_market_id for market in markets if market.polymarket_market_id)
        )
        await self.refresh()
        if not self._snapshot.usable or not self._snapshot.markets:
            raise GammaCacheUnavailable("Gamma bootstrap produced no usable markets")

    async def refresh(self) -> None:
        async with self._refresh_lock:
            started = time.monotonic()
            self._refresh_http_requests = 0
            self._refresh_429s = 0
            self._refresh_pages = 0
            self._refresh_records = 0
            previous = self._snapshot
            try:
                payloads = await self._fetch_all_markets()
                snapshot = self._build_snapshot(payloads, generation=previous.generation + 1)
                if not snapshot.markets:
                    raise GammaCacheUnavailable("Gamma refresh contained no valid markets")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                stale_age_seconds = (
                    (self._now() - previous.fetched_at).total_seconds()
                    if previous.fetched_at is not None
                    else float("inf")
                )
                using_stale_snapshot = bool(previous.markets and stale_age_seconds <= self._max_stale_seconds)
                self._snapshot = replace(previous, usable=using_stale_snapshot)
                LOGGER.error(
                    "gamma_bulk_refresh_failed",
                    extra={
                        "_generation": previous.generation + 1,
                        "_pages": self._refresh_pages,
                        "_records": self._refresh_records,
                        "_duration_seconds": time.monotonic() - started,
                        "_http_request_count": self._refresh_http_requests,
                        "_http_429_count": self._refresh_429s,
                        "_using_stale_snapshot": using_stale_snapshot,
                        "_stale_age_seconds": stale_age_seconds,
                        "_error": str(exc),
                    },
                )
                raise GammaCacheUnavailable("Gamma cache refresh failed") from exc
            self._snapshot = snapshot
            LOGGER.info(
                "gamma_bulk_refresh_completed",
                extra={
                    "_generation": snapshot.generation,
                    "_pages": self._refresh_pages,
                    "_records": len(snapshot.markets),
                    "_duration_seconds": time.monotonic() - started,
                    "_http_request_count": self._refresh_http_requests,
                    "_http_429_count": self._refresh_429s,
                },
            )

    def start_background_refresh(self) -> None:
        if self._refresh_task is None or self._refresh_task.done():
            self._refresh_task = asyncio.create_task(self._background_refresh_loop(), name="gamma-cache-refresh")

    async def _background_refresh_loop(self) -> None:
        while True:
            await asyncio.sleep(self._refresh_interval_seconds)
            try:
                await self.refresh()
            except GammaCacheUnavailable:
                # refresh() already emitted structured failure details. Keep retrying on cadence.
                continue

    async def close(self) -> None:
        if self._refresh_task is not None:
            self._refresh_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._refresh_task
            self._refresh_task = None
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    async def _fetch_all_markets(self) -> list[dict[str, Any]]:
        clob_markets = await self._fetch_clob_markets()
        gamma_markets: list[dict[str, Any]] = []
        for index in range(0, len(self._seed_market_ids), _GAMMA_ID_BATCH_SIZE):
            gamma_markets.extend(await self._fetch_page(self._seed_market_ids[index : index + _GAMMA_ID_BATCH_SIZE]))

        gamma_by_condition = {
            str(item.get("conditionId") or item.get("condition_id") or ""): item for item in gamma_markets
        }
        result: list[dict[str, Any]] = []
        merged_conditions: set[str] = set()
        for clob_market in clob_markets:
            condition_id = str(clob_market.get("conditionId") or "")
            gamma = gamma_by_condition.get(condition_id)
            result.append({**clob_market, **gamma} if gamma is not None else clob_market)
            if gamma is not None:
                merged_conditions.add(condition_id)
        result.extend(
            item
            for item in gamma_markets
            if str(item.get("conditionId") or item.get("condition_id") or "") not in merged_conditions
        )
        self._refresh_records = len(result)
        return result

    async def _fetch_clob_markets(self) -> list[dict[str, Any]]:
        cursor = "MA=="
        seen_cursors: set[str] = set()
        result: list[dict[str, Any]] = []
        for _ in range(_MAX_CLOB_PAGES):
            page, next_cursor = await self._fetch_clob_page(cursor)
            self._refresh_pages += 1
            result.extend(_adapt_clob_candidate(item) for item in page)
            self._refresh_records += len(page)
            if next_cursor in (None, "", "LTE="):
                return result
            assert next_cursor is not None
            if next_cursor in seen_cursors:
                raise RuntimeError("Polymarket CLOB pagination repeated a cursor")
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        raise RuntimeError(f"Polymarket CLOB pagination exceeded {_MAX_CLOB_PAGES} pages")

    async def _fetch_clob_page(self, cursor: str) -> tuple[list[dict[str, Any]], str | None]:
        session = self._get_session()
        await self._pace_request()
        self._refresh_http_requests += 1
        async with session.get(
            "https://clob.polymarket.com/sampling-markets",
            params={"next_cursor": cursor},
            timeout=30,
        ) as response:
            response.raise_for_status()
            payload: Any = await response.json()
        if not isinstance(payload, dict):
            raise RuntimeError("Polymarket CLOB returned a malformed catalog response")
        data = payload.get("data")
        next_cursor = payload.get("next_cursor")
        if (
            not isinstance(data, list)
            or any(not isinstance(item, dict) for item in data)
            or next_cursor is not None
            and not isinstance(next_cursor, str)
        ):
            raise RuntimeError("Polymarket CLOB returned a malformed catalog page")
        return data, next_cursor

    async def _fetch_page(self, market_ids: Sequence[str]) -> list[dict[str, Any]]:
        try:
            import aiohttp

            _ = aiohttp
        except ImportError as exc:
            raise RuntimeError("aiohttp is required for Polymarket market discovery") from exc

        url = f"{self._gamma_base_url}/markets"
        params = [("id", market_id) for market_id in market_ids]
        for attempt in range(_MAX_HTTP_ATTEMPTS):
            session = self._get_session()
            await self._pace_request()
            self._refresh_http_requests += 1
            retry_after: float | None = None
            async with session.get(url, params=params, timeout=15) as response:
                if response.status == 429:
                    self._refresh_429s += 1
                    if attempt + 1 >= _MAX_HTTP_ATTEMPTS:
                        response.raise_for_status()
                    retry_after = _bounded_retry_after(response.headers.get("Retry-After"))
                else:
                    response.raise_for_status()
                    payload: Any = await response.json()
            if retry_after is not None:
                await asyncio.sleep(retry_after)
                continue
            if not isinstance(payload, list) or any(not isinstance(item, dict) for item in payload):
                raise RuntimeError("Gamma returned a malformed batch-ID page")
            return payload
        raise RuntimeError("Gamma batch-ID request retries exhausted")

    async def _pace_request(self) -> None:
        request_delay = _MIN_REQUEST_INTERVAL_SECONDS - (time.monotonic() - self._last_http_request_at)
        if request_delay > 0:
            await asyncio.sleep(request_delay)
        self._last_http_request_at = time.monotonic()

    def _build_snapshot(self, payloads: list[dict[str, Any]], *, generation: int) -> _GammaSnapshot:
        valid: list[GammaPayload] = []
        by_id: dict[str, GammaPayload] = {}
        by_title_lists: dict[str, list[GammaPayload]] = {}
        for raw in payloads:
            if not _is_valid_candidate(raw):
                continue
            candidate: GammaPayload = MappingProxyType(dict(raw))
            market_id = str(candidate["id"])
            if market_id in by_id:
                raise RuntimeError(f"Gamma returned duplicate market id {market_id}")
            title = normalize_text(_candidate_title(candidate))
            valid.append(candidate)
            by_id[market_id] = candidate
            by_title_lists.setdefault(title, []).append(candidate)
        by_title = {key: tuple(values) for key, values in by_title_lists.items()}
        return _GammaSnapshot(
            markets=tuple(valid),
            by_id=MappingProxyType(by_id),
            by_title=MappingProxyType(by_title),
            fetched_at=self._now(),
            generation=generation,
            usable=True,
        )

    async def resolve(self, markets: list[MarketSpec]) -> list[MarketSpec]:
        if any(_needs_resolution(market) for market in markets) and not self._snapshot.usable:
            raise GammaCacheUnavailable("Gamma cache is unavailable; call bootstrap() before resolve()")

        if self._scan_all:
            scan_results: list[MarketSpec] = []
            for market in markets:
                try:
                    scan_results.append(self._resolve_from_snapshot(market) if _needs_resolution(market) else market)
                except Exception as exc:
                    LOGGER.info(
                        "polymarket_scan_all_market_skipped",
                        extra={"_symbol": market.symbol, "_reason": str(exc)},
                    )
            return scan_results

        resolved: list[MarketSpec] = []
        for market in markets:
            if not _needs_resolution(market):
                resolved.append(market)
                continue
            resolved.append(self._resolve_from_snapshot(market))
        return resolved

    def _resolve_from_snapshot(self, market: MarketSpec) -> MarketSpec:
        snapshot = self._snapshot
        if not snapshot.usable:
            raise GammaCacheUnavailable("Gamma cache is unavailable")
        candidate = _best_candidate_from_snapshot(snapshot, market)
        if candidate is None:
            raise RuntimeError(f"Could not discover Polymarket market for {market.symbol} {market.target_label}")

        token_id = _token_id_for_side(candidate, market.polymarket_side)
        if token_id is None:
            raise RuntimeError(f"Discovered market has no unambiguous {market.polymarket_side.value} token")
        condition_id = candidate.get("conditionId") or candidate.get("condition_id")
        expires_at = _candidate_expiry(candidate)
        LOGGER.info(
            "polymarket_market_discovered",
            extra={
                "_symbol": market.symbol,
                "_target_label": market.target_label,
                "_token_id": token_id,
                "_condition_id": condition_id,
                "_gamma_generation": snapshot.generation,
            },
        )
        return replace(
            market,
            polymarket_token_id=token_id,
            polymarket_market_id=str(candidate["id"]),
            polymarket_url=market.polymarket_url or _polymarket_public_url(candidate),
            condition_id=str(condition_id),
            neg_risk=_optional_bool(candidate, ("negRisk", "neg_risk", "isNegRisk")),
            expires_at=market.expires_at or expires_at,
            polymarket_volume_usd=_market_volume(candidate),
            category=market.category or _market_category(candidate),
            resolution_source=market.resolution_source or _resolution_source(candidate),
            outcome_semantics=market.outcome_semantics or _outcome_semantics(candidate),
            cutoff_at=market.cutoff_at or expires_at,
        )


def _best_candidate_from_snapshot(snapshot: _GammaSnapshot, market: MarketSpec) -> GammaPayload | None:
    if market.polymarket_market_id:
        candidate = snapshot.by_id.get(market.polymarket_market_id)
        return candidate if candidate is not None and _expiry_matches(market, candidate) else None
    expected_title = normalize_text(market.target_label or market.symbol)
    candidates = snapshot.by_title.get(expected_title, ())
    if len(candidates) != 1:
        return None
    candidate = candidates[0]
    return candidate if _expiry_matches(market, candidate) else None


def _needs_resolution(market: MarketSpec) -> bool:
    return not market.polymarket_token_id or market.polymarket_token_id == "replace-with-token-id"


def _adapt_clob_candidate(payload: Mapping[str, Any]) -> dict[str, Any]:
    tokens = payload.get("tokens")
    token_rows = [item for item in tokens if isinstance(item, Mapping)] if isinstance(tokens, list) else []
    condition_id = str(payload.get("condition_id") or "")
    return {
        **payload,
        "id": condition_id,
        "conditionId": condition_id,
        "endDateIso": payload.get("end_date_iso"),
        "clobTokenIds": [str(item.get("token_id") or "") for item in token_rows],
        "outcomes": [str(item.get("outcome") or "") for item in token_rows],
        "acceptingOrders": payload.get("accepting_orders"),
        "enableOrderBook": payload.get("enable_order_book"),
        "negRisk": payload.get("neg_risk"),
        "slug": payload.get("market_slug"),
    }


def _best_candidate(candidates: list[dict[str, Any]], market: MarketSpec) -> dict[str, Any] | None:
    """Compatibility helper used by focused matching tests; production resolve uses the snapshot indexes."""
    valid = [candidate for candidate in candidates if _is_valid_candidate(candidate)]
    snapshot = GammaMarketResolver()._build_snapshot(valid, generation=1)
    selected = _best_candidate_from_snapshot(snapshot, market)
    return dict(selected) if selected is not None else None


def _is_valid_candidate(candidate: Mapping[str, Any]) -> bool:
    market_id = str(candidate.get("id") or "").strip()
    condition_id = str(candidate.get("conditionId") or candidate.get("condition_id") or "").strip()
    title = normalize_text(_candidate_title(candidate))
    expiry = _candidate_expiry(candidate)
    if not market_id or not condition_id or not title or expiry is None or expiry <= datetime.now(UTC):
        return False
    required_flags = (
        _optional_bool(candidate, ("active",)) is True,
        _optional_bool(candidate, ("closed",)) is False,
        _optional_bool(candidate, ("acceptingOrders", "accepting_orders")) is True,
        _optional_bool(candidate, ("enableOrderBook", "enable_order_book")) is True,
    )
    if not all(required_flags) or _optional_bool(candidate, ("archived",)) is True:
        return False
    token_ids = _parse_token_ids(candidate.get("clobTokenIds"))
    outcomes = _parse_string_list(candidate.get("outcomes"))
    return bool(token_ids) and len(token_ids) == len(outcomes) and all(token_ids) and all(outcomes)


def _candidate_title(candidate: Mapping[str, Any]) -> str:
    return str(candidate.get("question") or candidate.get("title") or candidate.get("name") or "")


def _candidate_expiry(candidate: Mapping[str, Any]) -> datetime | None:
    return _parse_optional_datetime(
        candidate.get("endDateIso")
        or candidate.get("endDate")
        or candidate.get("end_date_iso")
        or candidate.get("end_date")
    )


def _expiry_matches(market: MarketSpec, candidate: GammaPayload) -> bool:
    if market.expires_at is None:
        return True
    candidate_expiry = _candidate_expiry(candidate)
    if candidate_expiry is None:
        return False
    source_expiry = market.expires_at
    if source_expiry.tzinfo is None:
        source_expiry = source_expiry.replace(tzinfo=UTC)
    return abs((source_expiry.astimezone(UTC) - candidate_expiry).total_seconds()) <= 1800


def _token_id_for_side(candidate: Mapping[str, Any], side: PolymarketSide) -> str | None:
    token_ids = _parse_token_ids(candidate.get("clobTokenIds"))
    outcomes = _parse_string_list(candidate.get("outcomes"))
    if not token_ids or len(token_ids) != len(outcomes):
        return None
    matches = [index for index, outcome in enumerate(outcomes) if outcome.strip().upper() == side.value]
    if len(matches) != 1:
        return None
    return token_ids[matches[0]] or None


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
    return _parse_string_list(raw)


def _parse_optional_datetime(raw: Any) -> datetime | None:
    if not isinstance(raw, str) or not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _optional_bool(payload: Mapping[str, Any], keys: tuple[str, ...]) -> bool | None:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, bool):
            return value
        if isinstance(value, str) and value.lower() in {"true", "false", "1", "0"}:
            return value.lower() in {"true", "1"}
    return None


def _market_volume(payload: Mapping[str, Any]) -> float | None:
    for key in ("volumeClob", "volumeNum", "volume", "volume24hr"):
        try:
            if payload.get(key) not in (None, ""):
                return float(payload[key])
        except (TypeError, ValueError):
            continue
    return None


def _market_category(payload: Mapping[str, Any]) -> str | None:
    value = payload.get("category") or payload.get("group") or payload.get("marketType")
    if isinstance(value, Mapping):
        value = value.get("name") or value.get("slug")
    if isinstance(value, str) and value.strip():
        return value.strip()
    tags = payload.get("tags")
    if isinstance(tags, Sequence) and not isinstance(tags, (str, bytes)):
        for tag in tags:
            if isinstance(tag, Mapping):
                candidate = tag.get("label") or tag.get("name") or tag.get("slug")
            else:
                candidate = tag
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
    return None


def _resolution_source(payload: Mapping[str, Any]) -> str | None:
    value = payload.get("resolutionSource") or payload.get("resolution_source") or payload.get("oracle")
    return str(value).strip() if value not in (None, "") else None


def _outcome_semantics(payload: Mapping[str, Any]) -> str | None:
    value = payload.get("description") or payload.get("rules") or payload.get("resolutionRules")
    return str(value).strip() if value not in (None, "") else None


def _polymarket_public_url(payload: Mapping[str, Any]) -> str | None:
    for key in ("url", "marketUrl", "market_url"):
        value = payload.get(key)
        if isinstance(value, str) and value.startswith(("https://", "http://")):
            return value
    event_slug = payload.get("eventSlug") or payload.get("event_slug")
    events = payload.get("events")
    if not event_slug and isinstance(events, Sequence) and not isinstance(events, (str, bytes)):
        for event in events:
            if isinstance(event, Mapping) and event.get("slug"):
                event_slug = event["slug"]
                break
    slug = event_slug or payload.get("slug")
    return f"https://polymarket.com/event/{slug}" if slug else None


def _bounded_retry_after(raw: str | None) -> float:
    try:
        value = float(raw) if raw is not None else 1.0
    except ValueError:
        try:
            retry_at = email.utils.parsedate_to_datetime(raw or "")
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=UTC)
            value = (retry_at.astimezone(UTC) - datetime.now(UTC)).total_seconds()
        except (TypeError, ValueError):
            value = 1.0
    return min(max(value, 0.0), _MAX_RETRY_AFTER_SECONDS)
