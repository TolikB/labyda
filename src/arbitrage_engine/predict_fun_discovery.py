from __future__ import annotations

import logging
import re
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any

from .config import PredictFunConfig
from .discovery_cpu import run_discovery_cpu
from .http import client_session
from .market_mapping import normalize_category
from .matcher import normalize_text, text_similarity
from .models import BinarySide, MappingStatus, MarketSpec

LOGGER = logging.getLogger(__name__)
PREDICT_MARKETS_PATH = "/v1/markets"
_MAX_UINT256 = (1 << 256) - 1
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


@dataclass(frozen=True)
class _ParsedBinaryOutcome:
    label: str
    normalized_label: str
    token_id: str
    side: BinarySide
    index_set: int | None


class PredictFunMarketResolver:
    def __init__(
        self,
        config: PredictFunConfig,
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
            headers: dict[str, str] = {}
            if self._config.api_key:
                headers["X-API-Key"] = self._config.api_key
            self._session = client_session(headers)
        return self._session

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    def invalidate_cache(self) -> None:
        self._market_payload_cache = None

    async def resolve(self, markets: list[MarketSpec]) -> list[MarketSpec]:
        if not self._config.api_base_url:
            return markets

        try:
            market_payloads = await self._fetch_markets()
        except Exception as exc:
            LOGGER.exception("predict_fun_discovery_failed")
            raise RuntimeError(f"Predict.fun discovery failed: {exc}") from exc
        self._last_catalog_raw_count = len(market_payloads)
        poisoned_market_ids = await run_discovery_cpu(_raw_catalog_poisoned_market_ids, market_payloads)
        market_payloads = await run_discovery_cpu(_filter_scan_all_payloads, market_payloads, self._categories_to_scan)
        if self._scan_all and not markets:
            parsed = await run_discovery_cpu(_scan_all_market_specs, market_payloads, poisoned_market_ids)
            self._last_catalog_parsed_count = len(parsed)
            return parsed
        return await run_discovery_cpu(_resolve_market_specs, market_payloads, markets, poisoned_market_ids)

    async def _fetch_markets(self) -> list[dict[str, Any]]:
        if self._market_payload_cache is not None:
            return self._market_payload_cache
        try:
            import aiohttp

            _ = aiohttp
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


def _resolve_market_specs(
    market_payloads: list[dict[str, Any]],
    markets: list[MarketSpec],
    poisoned_market_ids: set[str] | None = None,
) -> list[MarketSpec]:
    candidates_by_id = _collision_safe_catalog_candidates(market_payloads, poisoned_market_ids)
    candidates_by_title_key: dict[frozenset[str], list[dict[str, Any]]] = {}
    for candidate in candidates_by_id.values():
        candidate_title = _first_str(candidate, ("question", "title", "name")) or ""
        title_key = _strict_title_key(candidate_title)
        if title_key:
            candidates_by_title_key.setdefault(title_key, []).append(candidate)

    resolved: list[MarketSpec] = []
    for market in markets:
        if market.venue_b_label != "Predict.fun":
            resolved.append(market)
            continue
        selected_candidate = candidates_by_id.get(market.predict_fun_market_id or "")
        # A persisted venue ID is part of the approved mapping identity. Never
        # migrate it to a same-title market without a fresh mapping review.
        if selected_candidate is None and not market.predict_fun_market_id:
            symbol_text = normalize_text(market.symbol)
            target_text = normalize_text(market.target_label)
            expected_title = market.symbol if symbol_text == target_text else f"{market.symbol} {market.target_label}"
            selected_candidate = _best_candidate(
                candidates_by_title_key.get(_strict_title_key(expected_title), []), market
            )
        if selected_candidate is None:
            resolved.append(_clear_predict_execution_metadata(market))
            continue
        token_id = _token_id_for_market(selected_candidate, market)
        if token_id is None:
            resolved.append(_clear_predict_execution_metadata(market))
            continue
        market_id = _predict_market_id(selected_candidate)
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
                predict_fun_market_id=market_id or market.predict_fun_market_id,
                predict_fun_url=_predict_fun_public_url(selected_candidate, market_id) or market.predict_fun_url,
                predict_fun_neg_risk=_optional_bool(selected_candidate, ("isNegRisk", "negRisk", "neg_risk")),
                predict_fun_fee_rate_bps=_optional_int(selected_candidate, ("feeRateBps", "fee_rate_bps")),
                predict_fun_price_precision=_optional_int(
                    selected_candidate,
                    ("decimalPrecision", "decimal_precision"),
                ),
                predict_fun_volume_usd=_market_volume(selected_candidate),
                category=market.category or _market_category(selected_candidate),
                resolution_source=market.resolution_source or _resolution_source(selected_candidate),
                outcome_semantics=market.outcome_semantics
                or _first_str(selected_candidate, ("rules", "description", "resolutionRules")),
                cutoff_at=market.cutoff_at or market.expires_at,
            )
        )
    return resolved


def _collision_safe_catalog_candidates(
    market_payloads: list[dict[str, Any]],
    poisoned_market_ids: set[str] | None = None,
) -> dict[str, dict[str, Any]]:
    poisoned = poisoned_market_ids or set()
    grouped_by_id: dict[str, list[dict[str, Any]]] = {}
    for payload in market_payloads:
        market_id = _predict_market_id(payload)
        if market_id is not None:
            grouped_by_id.setdefault(market_id, []).append(payload)

    candidates = {
        market_id: payloads[0]
        for market_id, payloads in grouped_by_id.items()
        if len(payloads) == 1
    }
    token_market_ids: dict[str, set[str]] = {}
    invalid_market_ids: set[str] = set()
    for market_id, payload in candidates.items():
        specs = _market_specs_from_payload(payload)
        if not specs:
            invalid_market_ids.add(market_id)
            continue
        token_ids = {spec.predict_fun_token_id.strip().casefold() for spec in specs if spec.predict_fun_token_id}
        if not token_ids:
            invalid_market_ids.add(market_id)
            continue
        for token_id in token_ids:
            token_market_ids.setdefault(token_id, set()).add(market_id)
    for market_ids in token_market_ids.values():
        if len(market_ids) > 1:
            invalid_market_ids.update(market_ids)
    return {
        market_id: payload
        for market_id, payload in candidates.items()
        if market_id not in invalid_market_ids and market_id not in poisoned
    }


def _raw_catalog_poisoned_market_ids(market_payloads: list[dict[str, Any]]) -> set[str]:
    claimed_by_market_id: dict[str, set[int]] = {}
    claimed_market_ids_by_row: dict[int, set[str]] = {}
    token_row_indexes: dict[str, set[int]] = {}
    poisoned: set[str] = set()
    for index, payload in enumerate(market_payloads):
        claimed_market_ids = _claimed_predict_market_ids(payload)
        claimed_market_ids_by_row[index] = claimed_market_ids
        for market_id in claimed_market_ids:
            claimed_by_market_id.setdefault(market_id, set()).add(index)
        for token_id in _claimed_predict_token_ids(payload):
            token_row_indexes.setdefault(token_id, set()).add(index)
        canonical_market_id = _predict_market_id(payload)
        if canonical_market_id is None:
            poisoned.update(claimed_market_ids)
            continue
    for market_id, row_indexes in claimed_by_market_id.items():
        if len(row_indexes) > 1:
            poisoned.add(market_id)
    for row_indexes in token_row_indexes.values():
        if len(row_indexes) > 1:
            for row_index in row_indexes:
                poisoned.update(claimed_market_ids_by_row[row_index])
    return poisoned


def _clear_predict_execution_metadata(market: MarketSpec) -> MarketSpec:
    """Keep mapping identity for diagnostics but block execution on stale catalog data."""
    return replace(
        market,
        predict_fun_token_id="",
        predict_fun_fee_rate_bps=None,
        predict_fun_price_precision=None,
        mapping_status=(
            MappingStatus.STALE if market.mapping_status is MappingStatus.VERIFIED else market.mapping_status
        ),
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
            left = market.expires_at if market.expires_at.tzinfo is not None else market.expires_at.replace(tzinfo=UTC)
            right = candidate_expiry if candidate_expiry.tzinfo is not None else candidate_expiry.replace(tzinfo=UTC)
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
    expected_core = _strict_title_key(expected_title)
    candidate_core = _strict_title_key(candidate_title)
    if expected_core != candidate_core:
        return 0.0
    return max(0.85, text_similarity(expected_title, candidate_title))


def _strict_title_key(title: str) -> frozenset[str]:
    return frozenset(set(normalize_text(title).split()) - BENIGN_TITLE_VARIANTS)


def _token_id_for_side(candidate: dict[str, Any], side: BinarySide) -> str | None:
    token_id = _direct_token_id_for_side(candidate, side)
    if token_id:
        return token_id

    return _labeled_outcome_token_id_for_side(candidate, side)


def _direct_token_id_for_side(candidate: dict[str, Any], side: BinarySide) -> str | None:
    token_ids = _direct_token_ids_for_side(candidate, side)
    if not token_ids or len({token_id.strip().casefold() for token_id in token_ids}) != 1:
        return None
    return token_ids[0]


def _direct_token_ids_for_side(candidate: dict[str, Any], side: BinarySide) -> tuple[str, ...] | None:
    direct_keys = (
        f"{side.value.lower()}TokenId",
        f"{side.value.lower()}_token_id",
        f"{side.value.lower()}Token",
        f"{side.value.lower()}_token",
    )
    token_ids: list[str] = []
    for key in direct_keys:
        if key not in candidate:
            continue
        token_id = _canonical_token_id(candidate[key])
        if token_id is None:
            return None
        token_ids.append(token_id)
    return tuple(token_ids)


def _labeled_outcome_token_id_for_side(candidate: dict[str, Any], side: BinarySide) -> str | None:
    outcomes = _iter_outcomes(candidate)
    for outcome in outcomes:
        label = str(
            outcome.get("side") or outcome.get("name") or outcome.get("label") or outcome.get("outcome") or ""
        ).upper()
        if label == side.value:
            return _consistent_outcome_token_id(outcome)

    return None


def _iter_outcomes(candidate: dict[str, Any]) -> list[dict[str, Any]]:
    raw_outcomes = _raw_outcomes(candidate)
    if raw_outcomes is None:
        return []
    return [item for item in raw_outcomes if isinstance(item, dict)]


def _raw_outcomes(candidate: dict[str, Any]) -> list[Any] | None:
    container_keys = [key for key in ("outcomes", "tokens", "assets") if key in candidate]
    if len(container_keys) != 1:
        return None
    value = candidate[container_keys[0]]
    return value if isinstance(value, list) else None


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
    specs = _market_specs_from_payload(payload)
    return specs[0] if specs else None


def _market_specs_from_payload(payload: dict[str, Any]) -> list[MarketSpec]:
    market_id = _predict_market_id(payload)
    title = _first_str(payload, ("question", "title", "name", "slug"))
    expires_raw = _first_str(payload, ("expiresAt", "expires_at", "endDate", "end_date", "expiry"))
    if not market_id or not title:
        return []
    expires_at = _parse_datetime(expires_raw) if expires_raw else None
    polymarket_condition_id = _polymarket_condition_id(payload)
    common: dict[str, Any] = {
        "symbol": title,
        "polymarket_token_id": "",
        "expires_at": expires_at,
        "predict_fun_market_id": market_id,
        "predict_fun_url": _predict_fun_public_url(payload, market_id),
        "predict_fun_neg_risk": _optional_bool(payload, ("isNegRisk", "negRisk", "neg_risk")),
        "predict_fun_fee_rate_bps": _optional_int(payload, ("feeRateBps", "fee_rate_bps")),
        "predict_fun_price_precision": _optional_int(
            payload,
            ("decimalPrecision", "decimal_precision"),
        ),
        "predict_fun_volume_usd": _market_volume(payload),
        "category": _market_category(payload),
        "resolution_source": _resolution_source(payload),
        "outcome_semantics": _first_str(payload, ("rules", "description", "resolutionRules")),
        "cutoff_at": expires_at,
        "polymarket_market_id": polymarket_condition_id,
        "condition_id": polymarket_condition_id,
    }

    if not _has_unambiguous_binary_outcomes(payload):
        return []
    parsed_outcomes = _parse_binary_outcomes(payload)
    named_outcomes = parsed_outcomes is not None and any(
        outcome.normalized_label not in {"yes", "no"} for outcome in parsed_outcomes
    )
    no_token_id = _token_id_for_side(payload, BinarySide.NO) if not named_outcomes else None
    yes_token_id = _token_id_for_side(payload, BinarySide.YES) if not named_outcomes else None
    if no_token_id:
        orientations = [
            MarketSpec(
                target_label=title,
                polymarket_side=BinarySide.YES,
                predict_fun_token_id=no_token_id,
                predict_fun_side=BinarySide.NO,
                rules_fingerprint=f"predict:{market_id}",
                **common,
            )
        ]
        if yes_token_id:
            orientations.append(
                MarketSpec(
                    target_label=title,
                    polymarket_side=BinarySide.NO,
                    predict_fun_token_id=yes_token_id,
                    predict_fun_side=BinarySide.YES,
                    rules_fingerprint=f"predict:{market_id}:reverse",
                    **common,
                )
            )
        return orientations

    outcomes = _tokenized_outcomes(payload)
    outcomes_by_index_set = {outcome["index_set"]: outcome for outcome in outcomes}
    if len(outcomes) != 2 or set(outcomes_by_index_set) != {1, 2}:
        return []
    yes_outcome = outcomes_by_index_set[1]
    no_outcome = outcomes_by_index_set[2]
    result: list[MarketSpec] = []
    for target_outcome, polymarket_side, hedge_outcome, predict_fun_side in (
        (yes_outcome, BinarySide.YES, no_outcome, BinarySide.NO),
        (no_outcome, BinarySide.NO, yes_outcome, BinarySide.YES),
    ):
        result.append(
            MarketSpec(
                target_label=target_outcome["label"],
                polymarket_side=polymarket_side,
                predict_fun_token_id=hedge_outcome["token_id"],
                predict_fun_side=predict_fun_side,
                rules_fingerprint=f"predict:{market_id}:{target_outcome['label']}",
                **common,
            )
        )
    return result


def _tokenized_outcomes(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if not _has_unambiguous_binary_outcomes(payload):
        return []
    parsed_outcomes = _parse_binary_outcomes(payload)
    if parsed_outcomes is None or any(outcome.index_set is None for outcome in parsed_outcomes):
        return []
    return [
        {
            "label": outcome.label,
            "token_id": outcome.token_id,
            "index_set": outcome.index_set,
        }
        for outcome in parsed_outcomes
    ]


def _has_unambiguous_binary_outcomes(payload: dict[str, Any]) -> bool:
    for side in (BinarySide.YES, BinarySide.NO):
        direct_token_ids = _direct_token_ids_for_side(payload, side)
        if direct_token_ids is None:
            return False
        if len({token_id.strip().casefold() for token_id in direct_token_ids}) > 1:
            return False
    direct_yes_token_id = _direct_token_id_for_side(payload, BinarySide.YES)
    direct_no_token_id = _direct_token_id_for_side(payload, BinarySide.NO)
    if (
        direct_yes_token_id
        and direct_no_token_id
        and direct_yes_token_id.strip().casefold() == direct_no_token_id.strip().casefold()
    ):
        return False
    outcome_container_keys = [key for key in ("outcomes", "tokens", "assets") if key in payload]
    if not outcome_container_keys:
        return direct_yes_token_id is not None and direct_no_token_id is not None
    if len(outcome_container_keys) != 1 or not isinstance(payload[outcome_container_keys[0]], list):
        return False
    raw_outcomes = _raw_outcomes(payload)
    assert raw_outcomes is not None
    parsed_outcomes = _parse_binary_outcomes(payload)
    if parsed_outcomes is None:
        return False
    tokens_by_side = {outcome.side: outcome.token_id.strip().casefold() for outcome in parsed_outcomes}
    for side in (BinarySide.YES, BinarySide.NO):
        direct_token_id = _direct_token_id_for_side(payload, side)
        outcome_token_id = tokens_by_side[side]
        if (
            direct_token_id
            and outcome_token_id
            and direct_token_id.strip().casefold() != outcome_token_id
        ):
            return False
    return True


def _parse_binary_outcomes(payload: dict[str, Any]) -> tuple[_ParsedBinaryOutcome, _ParsedBinaryOutcome] | None:
    raw_outcomes = _raw_outcomes(payload)
    if raw_outcomes is None or len(raw_outcomes) != 2 or any(not isinstance(item, dict) for item in raw_outcomes):
        return None
    parsed: list[_ParsedBinaryOutcome] = []
    for raw_outcome in raw_outcomes:
        assert isinstance(raw_outcome, dict)
        label = _consistent_outcome_label(raw_outcome)
        token_id = _consistent_outcome_token_id(raw_outcome)
        index_set_valid, index_set = _consistent_outcome_index_set(raw_outcome)
        if label is None or token_id is None or not index_set_valid:
            return None

        semantic_sides: set[BinarySide] = set()
        if "side" in raw_outcome:
            side_value = raw_outcome["side"]
            if not isinstance(side_value, str) or not side_value.strip():
                return None
            try:
                semantic_sides.add(BinarySide(side_value.strip().upper()))
            except ValueError:
                return None
        normalized_label = normalize_text(label)
        if normalized_label in {"yes", "no"}:
            semantic_sides.add(BinarySide(normalized_label.upper()))
        if index_set is not None:
            semantic_sides.add(BinarySide.YES if index_set == 1 else BinarySide.NO)
        if len(semantic_sides) != 1:
            return None
        parsed.append(
            _ParsedBinaryOutcome(
                label=label,
                normalized_label=normalized_label,
                token_id=token_id,
                side=next(iter(semantic_sides)),
                index_set=index_set,
            )
        )

    if {outcome.side for outcome in parsed} != {BinarySide.YES, BinarySide.NO}:
        return None
    if len({outcome.normalized_label for outcome in parsed}) != 2:
        return None
    if len({outcome.token_id.strip().casefold() for outcome in parsed}) != 2:
        return None
    index_sets = [outcome.index_set for outcome in parsed]
    if any(index_set is not None for index_set in index_sets):
        if any(index_set is None for index_set in index_sets) or set(index_sets) != {1, 2}:
            return None
    return parsed[0], parsed[1]


def _consistent_outcome_label(outcome: dict[str, Any]) -> str | None:
    labels: list[str] = []
    for key in ("name", "label", "outcome"):
        if key not in outcome:
            continue
        value = outcome[key]
        if not isinstance(value, str) or not value.strip():
            return None
        labels.append(value.strip())
    if not labels:
        side = outcome.get("side")
        return side.strip() if isinstance(side, str) and side.strip() else None
    normalized_labels = {normalize_text(label) for label in labels}
    if len(normalized_labels) != 1 or not next(iter(normalized_labels)):
        return None
    return labels[0]


def _consistent_outcome_token_id(outcome: dict[str, Any]) -> str | None:
    keys = ("tokenId", "token_id", "onChainId", "on_chain_id", "assetId", "asset_id")
    token_ids: list[str] = []
    for key in keys:
        if key not in outcome:
            continue
        token_id = _canonical_token_id(outcome[key])
        if token_id is None:
            return None
        token_ids.append(token_id)
    if not token_ids or len({token_id.casefold() for token_id in token_ids}) != 1:
        return None
    return token_ids[0]


def _predict_market_id(payload: dict[str, Any]) -> str | None:
    market_ids: list[str] = []
    for key in ("id", "marketId", "market_id"):
        if key not in payload:
            continue
        market_id = _canonical_market_id(payload[key])
        if market_id is None:
            return None
        market_ids.append(market_id)
    if not market_ids or len(set(market_ids)) != 1:
        return None
    return market_ids[0]


def _claimed_predict_market_ids(payload: dict[str, Any]) -> set[str]:
    claimed: set[str] = set()
    for key in ("id", "marketId", "market_id"):
        if key not in payload:
            continue
        market_id = _canonical_market_id(payload[key])
        if market_id is not None:
            claimed.add(market_id)
    return claimed


def _claimed_predict_token_ids(payload: dict[str, Any]) -> set[str]:
    claimed: set[str] = set()
    direct_keys = (
        "yesTokenId",
        "yes_token_id",
        "yesToken",
        "yes_token",
        "noTokenId",
        "no_token_id",
        "noToken",
        "no_token",
    )
    for key in direct_keys:
        if key in payload and (token_id := _canonical_token_id(payload[key])) is not None:
            claimed.add(token_id)
    outcome_token_keys = ("tokenId", "token_id", "onChainId", "on_chain_id", "assetId", "asset_id")
    for container_key in ("outcomes", "tokens", "assets"):
        container = payload.get(container_key)
        if not isinstance(container, list):
            continue
        for outcome in container:
            if not isinstance(outcome, dict):
                continue
            for key in outcome_token_keys:
                if key in outcome and (token_id := _canonical_token_id(outcome[key])) is not None:
                    claimed.add(token_id)
    return claimed


def _canonical_market_id(value: Any) -> str | None:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        return None
    text = str(value).strip()
    if not text:
        return None
    if re.fullmatch(r"[+-]?\d+", text):
        number = int(text)
        return str(number) if number >= 0 else None
    return text


def _canonical_token_id(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if re.fullmatch(r"[+-]?\d+", text):
        number = int(text)
        return str(number) if 0 <= number <= _MAX_UINT256 else None
    return text


def _consistent_outcome_index_set(outcome: dict[str, Any]) -> tuple[bool, int | None]:
    raw_values = [outcome[key] for key in ("indexSet", "index_set") if key in outcome]
    if not raw_values:
        return True, None
    parsed_values: set[int] = set()
    for value in raw_values:
        try:
            parsed_values.add(int(str(value)))
        except (TypeError, ValueError):
            return False, None
    if len(parsed_values) != 1:
        return False, None
    index_set = next(iter(parsed_values))
    return (index_set in (1, 2)), index_set


def _polymarket_condition_id(payload: dict[str, Any]) -> str | None:
    direct = _first_str(payload, ("polymarketConditionId", "polymarket_condition_id"))
    if direct:
        return direct
    raw = payload.get("polymarketConditionIds")
    if isinstance(raw, list):
        for item in raw:
            if item not in (None, ""):
                return str(item)
    return None


def _resolution_source(payload: dict[str, Any]) -> str | None:
    explicit = _first_str(payload, ("resolutionSource", "resolution_source", "oracle"))
    if explicit:
        return explicit
    resolver = _first_str(payload, ("resolverAddress", "resolver_address"))
    oracle_question = _first_str(payload, ("oracleQuestionId", "oracle_question_id"))
    components = []
    if resolver:
        components.append(f"resolver:{resolver}")
    if oracle_question:
        components.append(f"oracle_question:{oracle_question}")
    return ";".join(components) or None


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


def _market_volume(payload: dict[str, Any]) -> float | None:
    for key in ("volumeUsd", "volume_usd", "volume24h", "volume"):
        try:
            if payload.get(key) not in (None, ""):
                return float(payload[key])
        except (TypeError, ValueError):
            continue
    stats = payload.get("stats")
    if isinstance(stats, dict):
        for key in ("totalLiquidityUsd", "volumeTotalUsd", "volume24hUsd", "liquidity3cAskUsd"):
            try:
                if stats.get(key) not in (None, ""):
                    return float(stats[key])
            except (TypeError, ValueError):
                continue
    return None


def _token_id_for_market(candidate: dict[str, Any], market: MarketSpec) -> str | None:
    if not _has_unambiguous_binary_outcomes(candidate):
        return None
    parsed_outcomes = _parse_binary_outcomes(candidate)
    if parsed_outcomes is not None and any(
        outcome.normalized_label not in {"yes", "no"} for outcome in parsed_outcomes
    ):
        if any(outcome.index_set is None for outcome in parsed_outcomes):
            return None
        outcomes_by_side = {outcome.side: outcome for outcome in parsed_outcomes}
        execution_outcome = outcomes_by_side[market.predict_fun_side]
        target_side = BinarySide.NO if market.predict_fun_side is BinarySide.YES else BinarySide.YES
        target_outcome = outcomes_by_side[target_side]
        if target_outcome.normalized_label != normalize_text(market.target_label):
            return None
        return execution_outcome.token_id
    token_id = _token_id_for_side(candidate, market.predict_fun_side)
    if token_id:
        opposite_token_id = _token_id_for_side(
            candidate,
            BinarySide.NO if market.predict_fun_side is BinarySide.YES else BinarySide.YES,
        )
        if opposite_token_id and token_id.casefold() == opposite_token_id.casefold():
            return None
        return token_id
    return None


def _market_category(payload: dict[str, Any]) -> str | None:
    direct = _first_str(payload, ("category", "group"))
    if direct:
        return direct

    variant_data = payload.get("variantData") or payload.get("variant_data")
    if isinstance(variant_data, dict):
        variant_type = _first_str(variant_data, ("type",))
        if variant_type:
            normalized_variant = variant_type.strip().upper()
            if normalized_variant.startswith("CRYPTO_"):
                return "crypto"
            if normalized_variant.startswith("SPORTS_"):
                return "sports"

    market_type = _first_str(payload, ("marketType", "market_type"))
    if market_type and market_type.strip().upper().startswith("SPORTS_"):
        return "sports"
    if isinstance(payload.get("team"), dict):
        return "sports"

    category_slug = _first_str(payload, ("categorySlug", "category_slug"))
    classified_slug = _recognized_category_slug(category_slug)
    if classified_slug is not None:
        return classified_slug

    topics = payload.get("topics")
    if isinstance(topics, list):
        for topic in topics:
            if isinstance(topic, str) and topic.strip():
                return topic.strip()
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


def _recognized_category_slug(value: str | None) -> str | None:
    if not value:
        return None
    words = set(re.findall(r"[a-z0-9]+", value.lower()))
    if words & {"crypto", "cryptocurrency", "bitcoin", "btc", "ethereum", "eth", "solana", "xrp"}:
        return "crypto"
    if words & {"esport", "esports", "football", "soccer", "sport", "sports"}:
        return "sports"
    return None


def _predict_fun_public_url(payload: dict[str, Any], market_id: str | None) -> str | None:
    for key in ("url", "marketUrl", "market_url"):
        value = payload.get(key)
        if isinstance(value, str) and value.startswith(("https://", "http://")):
            return value
    return f"https://predict.fun/market/{market_id}" if market_id else None


def _filter_scan_all_payloads(payloads: list[dict[str, Any]], allowed: set[str]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for payload in payloads:
        if not _is_execution_open(payload):
            continue
        category = normalize_category(_market_category(payload))
        if not allowed or category is None or category in allowed:
            result.append(payload)
    return result


def _is_execution_open(payload: dict[str, Any]) -> bool:
    trading_status = _first_str(payload, ("tradingStatus", "trading_status"))
    if trading_status is not None:
        return trading_status.upper() == "OPEN"
    status = _first_str(payload, ("status", "marketStatus", "market_status"))
    if status is None:
        # The API request itself is status=OPEN. Older payloads omit the field.
        return True
    return status.upper() not in {
        "CANCELED",
        "CANCELLED",
        "CLOSED",
        "EXPIRED",
        "HALTED",
        "PAUSED",
        "RESOLVED",
        "SETTLED",
    }


def _scan_all_market_specs(
    payloads: list[dict[str, Any]],
    poisoned_market_ids: set[str] | None = None,
) -> list[MarketSpec]:
    result: list[MarketSpec] = []
    for payload in _collision_safe_catalog_candidates(payloads, poisoned_market_ids).values():
        result.extend(_market_specs_from_payload(payload))
    return result
