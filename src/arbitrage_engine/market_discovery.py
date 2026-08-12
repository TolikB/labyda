from __future__ import annotations

import asyncio
import contextlib
import email.utils
import json
import logging
import time
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any

from .discovery_cpu import run_discovery_cpu
from .http import client_session
from .market_mapping import normalize_category
from .matcher import normalize_text, text_similarity
from .models import MarketSpec, PolymarketSide
from .sports_matching import sports_market_identity, structured_sports_match

LOGGER = logging.getLogger(__name__)

_GAMMA_ID_BATCH_SIZE = 50
_MAX_CLOB_PAGES = 199
_MAX_GAMMA_SPORTS_PAGES = 199
_MAX_HTTP_ATTEMPTS = 3
_MAX_RETRY_AFTER_SECONDS = 30.0
_MIN_REQUEST_INTERVAL_SECONDS = 0.25
_IMMUTABLE_MATCH_EXPIRY_WINDOW_SECONDS = 36 * 60 * 60
_SX_MARKET_MIN_SIMILARITY = 0.78
_POLYMARKET_HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
}
_SPORTS_MARKET_TYPES = ("moneyline", "spreads", "totals")

GammaPayload = Mapping[str, Any]


class GammaCacheUnavailable(RuntimeError):
    """Raised when Gamma discovery has no complete, usable local snapshot."""


@dataclass(frozen=True)
class _GammaSnapshot:
    markets: tuple[GammaPayload, ...]
    by_id: Mapping[str, GammaPayload]
    by_condition_id: Mapping[str, GammaPayload]
    by_title: Mapping[str, tuple[GammaPayload, ...]]
    by_title_term: Mapping[str, tuple[GammaPayload, ...]]
    fetched_at: datetime | None
    generation: int
    usable: bool


def _empty_snapshot() -> _GammaSnapshot:
    return _GammaSnapshot(
        (),
        MappingProxyType({}),
        MappingProxyType({}),
        MappingProxyType({}),
        MappingProxyType({}),
        None,
        0,
        False,
    )


@dataclass(frozen=True)
class GammaResolutionStats:
    requested: int = 0
    already_resolved: int = 0
    exact_id_matches: int = 0
    exact_title_matches: int = 0
    structured_sports_matches: int = 0
    semantic_matches: int = 0
    unresolved: int = 0
    rejection_reasons: tuple[tuple[str, int], ...] = ()


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
        self._seed_condition_ids: tuple[str, ...] = ()
        self._include_sports_catalog = False
        self._last_resolution_stats = GammaResolutionStats()

    @property
    def catalog_size(self) -> int:
        return len(self._snapshot.markets)

    @property
    def last_resolution_stats(self) -> GammaResolutionStats:
        return self._last_resolution_stats

    def _get_session(self) -> Any:
        if self._session is None or self._session.closed:
            self._session = client_session(dict(_POLYMARKET_HTTP_HEADERS))
        return self._session

    async def bootstrap(self, markets: Sequence[MarketSpec] = ()) -> None:
        self._include_sports_catalog = any(market.venue_b_label == "SX Bet" for market in markets)
        self._seed_market_ids = tuple(
            dict.fromkeys(
                market_id
                for market in markets
                if (market_id := _gamma_seed_market_id(market)) is not None
            )
        )
        self._seed_condition_ids = tuple(
            dict.fromkeys(
                condition_id
                for market in markets
                if (condition_id := _gamma_seed_condition_id(market)) is not None
            )
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
                snapshot = await run_discovery_cpu(self._build_snapshot, payloads, generation=previous.generation + 1)
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
        if self._include_sports_catalog:
            gamma_markets.extend(await self._fetch_sports_markets())
        for index in range(0, len(self._seed_market_ids), _GAMMA_ID_BATCH_SIZE):
            gamma_markets.extend(await self._fetch_page(self._seed_market_ids[index : index + _GAMMA_ID_BATCH_SIZE]))
        for index in range(0, len(self._seed_condition_ids), _GAMMA_ID_BATCH_SIZE):
            gamma_markets.extend(
                await self._fetch_condition_page(
                    self._seed_condition_ids[index : index + _GAMMA_ID_BATCH_SIZE]
                )
            )

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

    async def _fetch_sports_markets(self) -> list[dict[str, Any]]:
        cursor: str | None = None
        seen_cursors: set[str] = set()
        result: list[dict[str, Any]] = []
        for _ in range(_MAX_GAMMA_SPORTS_PAGES):
            page, next_cursor = await self._fetch_sports_page(cursor)
            self._refresh_pages += 1
            result.extend(page)
            self._refresh_records += len(page)
            if _sports_page_reaches_present(page, now=self._now()):
                return result
            if next_cursor in (None, ""):
                return result
            assert next_cursor is not None
            if next_cursor in seen_cursors:
                raise RuntimeError("Polymarket Gamma sports pagination repeated a cursor")
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        raise RuntimeError(f"Polymarket Gamma sports pagination exceeded {_MAX_GAMMA_SPORTS_PAGES} pages")

    async def _fetch_sports_page(self, cursor: str | None) -> tuple[list[dict[str, Any]], str | None]:
        params: list[tuple[str, str | int]] = [
            *(("sports_market_types", market_type) for market_type in _SPORTS_MARKET_TYPES),
            ("closed", "false"),
            ("active", "true"),
            ("order", "gameStartTime"),
            ("ascending", "false"),
            ("limit", 100),
        ]
        if cursor:
            params.append(("after_cursor", cursor))
        payload = await self._get_json_with_retries(
            f"{self._gamma_base_url}/markets/keyset",
            params=params,
            request_timeout=30,
        )
        if not isinstance(payload, dict):
            raise RuntimeError("Gamma returned a malformed sports catalog response")
        markets = payload.get("markets")
        next_cursor = payload.get("next_cursor")
        if (
            not isinstance(markets, list)
            or any(not isinstance(item, dict) for item in markets)
            or next_cursor is not None
            and not isinstance(next_cursor, str)
        ):
            raise RuntimeError("Gamma returned a malformed sports catalog page")
        return markets, next_cursor

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
        try:
            payload = await self._get_json_with_retries(
                "https://clob.polymarket.com/sampling-markets",
                params={"next_cursor": cursor},
                request_timeout=30,
            )
        except Exception as exc:
            if not _is_http_forbidden(exc):
                raise
            payload = await _load_json_via_urllib(
                "https://clob.polymarket.com/sampling-markets",
                params={"next_cursor": cursor},
                request_timeout=30,
                headers=_POLYMARKET_HTTP_HEADERS,
            )
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
        url = f"{self._gamma_base_url}/markets"
        # Gamma defaults to 20 records even when more repeated ID filters are
        # supplied. Keep the response bound aligned with the requested batch so
        # exact cross-venue IDs later in the batch are not silently omitted.
        params: list[tuple[str, str | int]] = [
            *(("id", market_id) for market_id in market_ids),
            ("limit", len(market_ids)),
        ]
        payload = await self._get_json_with_retries(url, params=params, request_timeout=15)
        if not isinstance(payload, list) or any(not isinstance(item, dict) for item in payload):
            raise RuntimeError("Gamma returned a malformed batch-ID page")
        return payload

    async def _fetch_condition_page(self, condition_ids: Sequence[str]) -> list[dict[str, Any]]:
        url = f"{self._gamma_base_url}/markets"
        params: list[tuple[str, str | int]] = [
            *(("condition_ids", condition_id) for condition_id in condition_ids),
            ("limit", len(condition_ids)),
        ]
        try:
            payload = await self._get_json_with_retries(url, params=params, request_timeout=15)
        except Exception as exc:
            if not _is_http_forbidden(exc):
                raise
            try:
                payload = await _load_json_via_urllib(
                    url,
                    params=params,
                    request_timeout=15,
                    headers=_POLYMARKET_HTTP_HEADERS,
                )
            except Exception as fallback_exc:
                if not _is_http_forbidden(fallback_exc):
                    raise
                if len(condition_ids) == 1:
                    LOGGER.warning("gamma_condition_enrichment_forbidden_skipped")
                    return []
                midpoint = len(condition_ids) // 2
                first = await self._fetch_condition_page(condition_ids[:midpoint])
                second = await self._fetch_condition_page(condition_ids[midpoint:])
                return [*first, *second]
        if not isinstance(payload, list) or any(not isinstance(item, dict) for item in payload):
            raise RuntimeError("Gamma returned a malformed batch-condition page")
        return payload

    async def _get_json_with_retries(self, url: str, *, params: Any, request_timeout: float) -> Any:
        for attempt in range(_MAX_HTTP_ATTEMPTS):
            session = self._get_session()
            await self._pace_request()
            self._refresh_http_requests += 1
            retry_after: float | None = None
            async with session.get(url, params=params, timeout=request_timeout) as response:
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
            return payload
        raise RuntimeError(f"HTTP retries exhausted for {url}")

    async def _pace_request(self) -> None:
        request_delay = _MIN_REQUEST_INTERVAL_SECONDS - (time.monotonic() - self._last_http_request_at)
        if request_delay > 0:
            await asyncio.sleep(request_delay)
        self._last_http_request_at = time.monotonic()

    def _build_snapshot(self, payloads: list[dict[str, Any]], *, generation: int) -> _GammaSnapshot:
        deduped_by_id: dict[str, GammaPayload] = {}
        ordered_market_ids: list[str] = []
        best_market_id_by_condition: dict[str, str] = {}
        best_market_id_by_id: dict[str, str] = {}
        valid: list[GammaPayload] = []
        by_id: dict[str, GammaPayload] = {}
        by_condition_id: dict[str, GammaPayload] = {}
        by_title_lists: dict[str, list[GammaPayload]] = {}
        by_title_term_lists: dict[str, list[GammaPayload]] = {}
        for raw in payloads:
            if not _is_valid_candidate(raw):
                continue
            candidate: GammaPayload = MappingProxyType(dict(raw))
            market_id = str(candidate["id"])
            condition_id = str(candidate.get("conditionId") or candidate.get("condition_id") or "")
            existing = deduped_by_id.get(market_id)
            if existing is None:
                deduped_by_id[market_id] = candidate
                ordered_market_ids.append(market_id)
            else:
                deduped_by_id[market_id] = _prefer_duplicate_candidate(existing, candidate)

        for market_id in ordered_market_ids:
            candidate = deduped_by_id[market_id]
            condition_id = str(candidate.get("conditionId") or candidate.get("condition_id") or "")
            existing_market_id = best_market_id_by_condition.get(condition_id)
            if existing_market_id is None:
                best_market_id_by_condition[condition_id] = market_id
                best_market_id_by_id[market_id] = market_id
                continue
            preferred = _prefer_duplicate_candidate(deduped_by_id[existing_market_id], candidate)
            preferred_market_id = str(preferred["id"])
            best_market_id_by_condition[condition_id] = preferred_market_id
            best_market_id_by_id[existing_market_id] = preferred_market_id
            best_market_id_by_id[market_id] = preferred_market_id

        for market_id in ordered_market_ids:
            if best_market_id_by_id.get(market_id, market_id) != market_id:
                continue
            candidate = deduped_by_id[market_id]
            condition_id = str(candidate.get("conditionId") or candidate.get("condition_id") or "")
            title = normalize_text(_candidate_title(candidate))
            valid.append(candidate)
            by_id[market_id] = candidate
            by_condition_id[condition_id] = candidate
            by_title_lists.setdefault(title, []).append(candidate)
            for term in _candidate_title_terms(candidate):
                by_title_term_lists.setdefault(term, []).append(candidate)
        for alias_id, preferred_market_id in best_market_id_by_id.items():
            alias_candidate = by_id.get(preferred_market_id)
            if alias_candidate is not None:
                by_id[alias_id] = alias_candidate
        by_title = {key: tuple(values) for key, values in by_title_lists.items()}
        by_title_term = {key: tuple(values) for key, values in by_title_term_lists.items()}
        return _GammaSnapshot(
            markets=tuple(valid),
            by_id=MappingProxyType(by_id),
            by_condition_id=MappingProxyType(by_condition_id),
            by_title=MappingProxyType(by_title),
            by_title_term=MappingProxyType(by_title_term),
            fetched_at=self._now(),
            generation=generation,
            usable=True,
        )

    async def resolve(self, markets: list[MarketSpec]) -> list[MarketSpec]:
        if any(_needs_resolution(market) for market in markets) and not self._snapshot.usable:
            raise GammaCacheUnavailable("Gamma cache is unavailable; call bootstrap() before resolve()")

        if self._scan_all:
            scan_results, resolution_stats = await run_discovery_cpu(self._resolve_scan_all, list(markets))
            self._last_resolution_stats = resolution_stats
            LOGGER.info(
                "polymarket_scan_all_resolution_summary",
                extra={
                    "_requested": resolution_stats.requested,
                    "_already_resolved": resolution_stats.already_resolved,
                    "_exact_id_matches": resolution_stats.exact_id_matches,
                    "_exact_title_matches": resolution_stats.exact_title_matches,
                    "_structured_sports_matches": resolution_stats.structured_sports_matches,
                    "_semantic_matches": resolution_stats.semantic_matches,
                    "_unresolved": resolution_stats.unresolved,
                    "_rejection_reasons": dict(resolution_stats.rejection_reasons),
                },
            )
            return scan_results

        stats = {
            "requested": len(markets),
            "already_resolved": 0,
            "exact_id_matches": 0,
            "exact_title_matches": 0,
            "structured_sports_matches": 0,
            "semantic_matches": 0,
            "unresolved": 0,
        }
        resolved: list[MarketSpec] = []
        for market in markets:
            if not _needs_resolution(market):
                resolved.append(market)
                continue
            item, strategy = self._resolve_from_snapshot_with_strategy(market)
            stats[f"{strategy}_matches"] += 1
            resolved.append(item)
        self._last_resolution_stats = GammaResolutionStats(
            requested=stats["requested"],
            already_resolved=stats["already_resolved"],
            exact_id_matches=stats["exact_id_matches"],
            exact_title_matches=stats["exact_title_matches"],
            structured_sports_matches=stats["structured_sports_matches"],
            semantic_matches=stats["semantic_matches"],
            unresolved=stats["unresolved"],
        )
        return resolved

    def _resolve_scan_all(self, markets: list[MarketSpec]) -> tuple[list[MarketSpec], GammaResolutionStats]:
        stats = {
            "requested": len(markets),
            "already_resolved": 0,
            "exact_id_matches": 0,
            "exact_title_matches": 0,
            "structured_sports_matches": 0,
            "semantic_matches": 0,
            "unresolved": 0,
        }
        rejection_reasons: dict[str, int] = {}
        scan_results: list[MarketSpec] = []
        for market in markets:
            if not _needs_resolution(market):
                stats["already_resolved"] += 1
                scan_results.append(market)
                continue
            try:
                resolved_item, strategy = self._resolve_from_snapshot_with_strategy(market)
                scan_results.append(resolved_item)
                stats[f"{strategy}_matches"] += 1
            except Exception as exc:
                stats["unresolved"] += 1
                reason = _resolution_rejection_reason(exc)
                rejection_reasons[reason] = rejection_reasons.get(reason, 0) + 1
        return scan_results, GammaResolutionStats(
            requested=stats["requested"],
            already_resolved=stats["already_resolved"],
            exact_id_matches=stats["exact_id_matches"],
            exact_title_matches=stats["exact_title_matches"],
            structured_sports_matches=stats["structured_sports_matches"],
            semantic_matches=stats["semantic_matches"],
            unresolved=stats["unresolved"],
            rejection_reasons=tuple(sorted(rejection_reasons.items())),
        )

    def _resolve_from_snapshot(self, market: MarketSpec) -> MarketSpec:
        resolved, _ = self._resolve_from_snapshot_with_strategy(market)
        return resolved

    def _resolve_from_snapshot_with_strategy(self, market: MarketSpec) -> tuple[MarketSpec, str]:
        snapshot = self._snapshot
        if not snapshot.usable:
            raise GammaCacheUnavailable("Gamma cache is unavailable")
        candidate, strategy = _best_candidate_from_snapshot_with_strategy(snapshot, market)
        if candidate is None:
            raise RuntimeError(f"Could not discover Polymarket market for {market.symbol} {market.target_label}")

        token_id = _token_id_for_market(candidate, market)
        if token_id is None:
            raise RuntimeError(f"Discovered market has no unambiguous {market.polymarket_side.value} token")
        condition_id = candidate.get("conditionId") or candidate.get("condition_id")
        expires_at = _candidate_expiry(candidate)
        if not self._scan_all:
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
        return (
            replace(
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
                mapping_strategy=strategy,
            ),
            strategy,
        )


def _best_candidate_from_snapshot(snapshot: _GammaSnapshot, market: MarketSpec) -> GammaPayload | None:
    candidate, _ = _best_candidate_from_snapshot_with_strategy(snapshot, market)
    return candidate


def _best_candidate_from_snapshot_with_strategy(
    snapshot: _GammaSnapshot, market: MarketSpec
) -> tuple[GammaPayload | None, str]:
    if market.polymarket_market_id:
        candidate = snapshot.by_id.get(market.polymarket_market_id) or snapshot.by_condition_id.get(
            market.polymarket_market_id
        )
        if candidate is not None:
            return (
                (candidate, "exact_id")
                if _expiry_matches(
                    market,
                    candidate,
                    window_seconds=max(
                        _IMMUTABLE_MATCH_EXPIRY_WINDOW_SECONDS,
                        _expiry_window_seconds_for_market(market),
                    ),
                )
                else (None, "unresolved")
            )
    expected_title = normalize_text(_matching_title(market))
    candidates = snapshot.by_title.get(expected_title, ())
    exact = [
        candidate
        for candidate in candidates
        if _expiry_matches(market, candidate, window_seconds=_expiry_window_seconds_for_market(market))
    ]
    if len(exact) == 1:
        return exact[0], "exact_title"
    if _requires_structured_sports_match(market):
        structured = _best_structured_sports_candidate(snapshot, market)
        return (structured, "structured_sports") if structured is not None else (None, "unresolved")
    if not _allow_semantic_scan(market):
        return None, "unresolved"
    semantic = _best_semantic_candidate(snapshot, market)
    return (semantic, "semantic") if semantic is not None else (None, "unresolved")


def _best_semantic_candidate(snapshot: _GammaSnapshot, market: MarketSpec) -> GammaPayload | None:
    expected_title = _matching_title(market)
    expiry_window_seconds = _expiry_window_seconds_for_market(market)
    min_similarity = _min_title_similarity_for_market(market)
    expected_subject = _sx_market_subject(market)
    require_metadata_similarity = _require_metadata_similarity(market)
    candidates_to_scan = _semantic_candidate_pool(snapshot, market, expected_subject)
    matches: list[tuple[float, str, GammaPayload]] = []
    for candidate in candidates_to_scan:
        if not _expiry_matches(market, candidate, window_seconds=expiry_window_seconds):
            continue
        if _token_id_for_market(candidate, market) is None:
            continue
        if expected_subject is not None and not _candidate_contains_subject(candidate, expected_subject):
            continue
        title_score = text_similarity(expected_title, _candidate_title(candidate))
        if title_score < min_similarity:
            continue
        rules_score = _optional_semantic_similarity(market.outcome_semantics, _outcome_semantics(candidate))
        if require_metadata_similarity and rules_score is not None and rules_score < 0.55:
            continue
        source_score = _optional_semantic_similarity(market.resolution_source, _resolution_source(candidate))
        if require_metadata_similarity and source_score is not None and source_score < 0.55:
            continue
        score = title_score + (rules_score or 0.0) * 0.05 + (source_score or 0.0) * 0.05
        matches.append((score, str(candidate["id"]), candidate))
    if not matches:
        return None
    matches.sort(key=lambda item: (item[0], item[1]), reverse=True)
    if len(matches) > 1 and matches[0][1] != matches[1][1] and matches[0][0] - matches[1][0] < 0.02:
        return None
    return matches[0][2]


def _requires_structured_sports_match(market: MarketSpec) -> bool:
    return market.venue_b_label == "SX Bet"


def _best_structured_sports_candidate(snapshot: _GammaSnapshot, market: MarketSpec) -> GammaPayload | None:
    source_identity = sports_market_identity(
        market.symbol,
        yes_label=market.target_label,
        outcome_semantics=market.outcome_semantics,
    )
    if source_identity is None:
        return None
    matches: list[GammaPayload] = []
    for candidate in _structured_sports_candidate_pool(snapshot, source_identity.participants):
        if _token_id_for_market(candidate, market) is None:
            continue
        outcome_labels = _structured_outcome_labels(candidate, expected_subject=market.target_label)
        candidate_identity = sports_market_identity(
            _candidate_title(candidate),
            yes_label=outcome_labels[0] if outcome_labels is not None else None,
            no_label=outcome_labels[1] if outcome_labels is not None else None,
            outcome_semantics=_outcome_semantics(candidate),
        )
        if structured_sports_match(
            source_identity,
            candidate_identity,
            left_cutoff=market.expires_at,
            right_cutoff=_candidate_expiry(candidate),
        ):
            matches.append(candidate)
    unique = {str(candidate["id"]): candidate for candidate in matches}
    return next(iter(unique.values())) if len(unique) == 1 else None


def _structured_outcome_labels(
    candidate: Mapping[str, Any],
    *,
    expected_subject: str,
) -> tuple[str, str] | None:
    outcomes = _parse_string_list(candidate.get("outcomes"))
    if len(outcomes) != 2 or any(not outcome.strip() for outcome in outcomes):
        return None
    expected = normalize_text(expected_subject)
    if expected and normalize_text(outcomes[1]) == expected and normalize_text(outcomes[0]) != expected:
        return outcomes[1], outcomes[0]
    return outcomes[0], outcomes[1]


def _structured_sports_candidate_pool(
    snapshot: _GammaSnapshot,
    participants: tuple[str, ...],
) -> Sequence[GammaPayload]:
    terms = {term for participant in participants for term in participant.split() if len(term) >= 3}
    if not terms:
        return ()
    populated = [group for term in terms if (group := snapshot.by_title_term.get(term, ()))]
    if not populated:
        return ()
    smallest = min(populated, key=len)
    return tuple(candidate for candidate in smallest if terms.issubset(_candidate_title_terms(candidate)))


def _semantic_candidate_pool(
    snapshot: _GammaSnapshot,
    market: MarketSpec,
    expected_subject: str | None,
) -> Sequence[GammaPayload]:
    if market.venue_b_label != "SX Bet" or not expected_subject:
        return snapshot.markets
    term_groups = [snapshot.by_title_term.get(term, ()) for term in expected_subject.split() if term]
    if not term_groups:
        return snapshot.markets
    smallest = min(term_groups, key=len)
    if not smallest:
        return ()
    if len(term_groups) == 1:
        return smallest
    required_terms = {term for term in expected_subject.split() if term}
    return tuple(
        candidate
        for candidate in smallest
        if required_terms.issubset(_candidate_title_terms(candidate))
    )


def _matching_title(market: MarketSpec) -> str:
    if market.venue_b_label in {"Myriad", "SX Bet"} and market.symbol:
        return market.symbol
    return market.target_label or market.symbol


def _allow_semantic_scan(market: MarketSpec) -> bool:
    if market.venue_b_label == "Predict.fun":
        return False
    return True


def _expiry_window_seconds_for_market(market: MarketSpec) -> int:
    if normalize_category(market.category or "") == "sports" or market.venue_b_label == "SX Bet":
        return 7 * 24 * 60 * 60
    return 1_800


def _min_title_similarity_for_market(market: MarketSpec) -> float:
    if market.venue_b_label == "SX Bet":
        return _SX_MARKET_MIN_SIMILARITY
    return 0.90


def _require_metadata_similarity(market: MarketSpec) -> bool:
    return market.venue_b_label != "SX Bet"


def _sx_market_subject(market: MarketSpec) -> str | None:
    if market.venue_b_label != "SX Bet":
        return None
    target = normalize_text(market.target_label)
    if target and target not in {"field", "field the", "the field"}:
        return target
    title = normalize_text(market.symbol)
    for marker in (
        " win ",
        " beat ",
        " cover ",
        " reach ",
        " eliminated ",
        " record ",
        " total go ",
        " qualify ",
    ):
        if marker in f" {title} ":
            prefix = title.split(marker, 1)[0]
            return prefix.removeprefix("will ").strip() or None
    return None


def _candidate_contains_subject(candidate: Mapping[str, Any], expected_subject: str) -> bool:
    candidate_title = f" {normalize_text(_candidate_title(candidate))} "
    return f" {expected_subject} " in candidate_title


def _optional_semantic_similarity(left: str | None, right: str | None) -> float | None:
    if not left or not right:
        return None
    return text_similarity(left, right)


def _resolution_rejection_reason(error: Exception) -> str:
    message = str(error).lower()
    if "no unambiguous" in message:
        return "ambiguous_outcomes"
    if "could not discover" in message:
        return "no_safe_match"
    if isinstance(error, GammaCacheUnavailable):
        return "catalog_unavailable"
    return type(error).__name__


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


def _prefer_duplicate_candidate(left: GammaPayload, right: GammaPayload) -> GammaPayload:
    left_score = _candidate_dedup_score(left)
    right_score = _candidate_dedup_score(right)
    if right_score > left_score:
        return right
    if right_score < left_score:
        return left
    right_json = json.dumps(right, sort_keys=True, default=str)
    left_json = json.dumps(left, sort_keys=True, default=str)
    return right if right_json > left_json else left


def _candidate_dedup_score(candidate: Mapping[str, Any]) -> tuple[float, ...]:
    public_url = _polymarket_public_url(candidate)
    description = _outcome_semantics(candidate)
    resolution_source = _resolution_source(candidate)
    volume = _market_volume(candidate) or -1.0
    return (
        float(len(_parse_token_ids(candidate.get("clobTokenIds")))),
        float(len(_parse_string_list(candidate.get("outcomes")))),
        1.0 if public_url else 0.0,
        1.0 if description else 0.0,
        1.0 if resolution_source else 0.0,
        volume,
        float(len(_candidate_title(candidate))),
    )


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


def _candidate_title_terms(candidate: Mapping[str, Any]) -> frozenset[str]:
    return frozenset(term for term in normalize_text(_candidate_title(candidate)).split() if len(term) >= 3)


def _candidate_expiry(candidate: Mapping[str, Any]) -> datetime | None:
    sports_start = _parse_optional_datetime(candidate.get("gameStartTime") or candidate.get("game_start_time"))
    if sports_start is not None:
        return sports_start
    events = candidate.get("events")
    is_sports_market = bool(candidate.get("sportsMarketType") or candidate.get("sports_market_type"))
    if is_sports_market and isinstance(events, list):
        for event in events:
            if not isinstance(event, Mapping):
                continue
            sports_start = _parse_optional_datetime(
                event.get("gameStartTime") or event.get("game_start_time") or event.get("startTime")
            )
            if sports_start is not None:
                return sports_start
    return _parse_optional_datetime(
        candidate.get("endDate")
        or candidate.get("endDateIso")
        or candidate.get("end_date_iso")
        or candidate.get("end_date")
    )


def _sports_page_reaches_present(page: Sequence[Mapping[str, Any]], *, now: datetime) -> bool:
    if not page:
        return True
    reference = now if now.tzinfo is not None else now.replace(tzinfo=UTC)
    starts = [
        _parse_optional_datetime(item.get("gameStartTime") or item.get("game_start_time"))
        for item in page
    ]
    return all(start is not None for start in starts) and any(
        start <= reference.astimezone(UTC) for start in starts if start is not None
    )


def _expiry_matches(market: MarketSpec, candidate: GammaPayload, *, window_seconds: int = 1_800) -> bool:
    if market.expires_at is None:
        return True
    candidate_expiry = _candidate_expiry(candidate)
    if candidate_expiry is None:
        return False
    source_expiry = market.expires_at
    if source_expiry.tzinfo is None:
        source_expiry = source_expiry.replace(tzinfo=UTC)
    return abs((source_expiry.astimezone(UTC) - candidate_expiry).total_seconds()) <= window_seconds


def _token_id_for_side(candidate: Mapping[str, Any], side: PolymarketSide) -> str | None:
    token_ids = _parse_token_ids(candidate.get("clobTokenIds"))
    outcomes = _parse_string_list(candidate.get("outcomes"))
    if not token_ids or len(token_ids) != len(outcomes):
        return None
    matches = [index for index, outcome in enumerate(outcomes) if outcome.strip().upper() == side.value]
    if len(matches) != 1:
        return None
    return token_ids[matches[0]] or None


def _token_id_for_market(candidate: Mapping[str, Any], market: MarketSpec) -> str | None:
    side_token = _token_id_for_side(candidate, market.polymarket_side)
    if side_token is not None:
        return side_token
    token_ids = _parse_token_ids(candidate.get("clobTokenIds"))
    outcomes = _parse_string_list(candidate.get("outcomes"))
    if not token_ids or len(token_ids) != len(outcomes):
        return None
    expected_label = normalize_text(market.target_label)
    if not expected_label:
        return None
    label_matches = [
        index for index, outcome in enumerate(outcomes) if normalize_text(outcome) == expected_label
    ]
    if not label_matches and market.venue_b_label == "Myriad" and market.polymarket_market_id:
        expected_words = expected_label.split()
        label_matches = [
            index
            for index, outcome in enumerate(outcomes)
            if _contains_contiguous_words(normalize_text(outcome).split(), expected_words)
        ]
    if len(label_matches) != 1:
        return None
    return token_ids[label_matches[0]] or None


def _contains_contiguous_words(words: list[str], expected: list[str]) -> bool:
    if not expected or len(expected) > len(words):
        return False
    width = len(expected)
    return any(words[index : index + width] == expected for index in range(len(words) - width + 1))


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


def _gamma_seed_market_id(market: MarketSpec) -> str | None:
    raw = (market.polymarket_market_id or "").strip()
    if not raw or raw.startswith("0x"):
        return None
    return raw if raw.isdigit() else None


def _gamma_seed_condition_id(market: MarketSpec) -> str | None:
    for value in (market.condition_id, market.polymarket_market_id):
        raw = (value or "").strip()
        is_hex = all(character in "0123456789abcdefABCDEF" for character in raw[2:])
        if len(raw) == 66 and raw.startswith("0x") and is_hex:
            return raw
    return None


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


async def _load_json_via_urllib(
    url: str,
    *,
    params: Mapping[str, Any] | Sequence[tuple[str, Any]] | None,
    request_timeout: float,
    headers: Mapping[str, str] | None,
) -> Any:
    normalized_params = list(params.items()) if isinstance(params, Mapping) else list(params or ())
    return await asyncio.to_thread(
        _load_json_via_urllib_sync,
        url,
        normalized_params,
        request_timeout,
        dict(headers or {}),
    )


def _load_json_via_urllib_sync(
    url: str,
    params: Sequence[tuple[str, Any]],
    request_timeout: float,
    headers: dict[str, str],
) -> Any:
    query = urllib.parse.urlencode([(str(key), str(value)) for key, value in params])
    request_url = f"{url}?{query}" if query else url
    request = urllib.request.Request(request_url, headers=headers)
    with urllib.request.urlopen(request, timeout=request_timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def _is_http_forbidden(exc: Exception) -> bool:
    status = getattr(exc, "status", None)
    return status == 403 or "403" in str(exc)


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
