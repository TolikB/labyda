from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import datetime, timezone
from typing import Iterable

from .models import ExecutionMode, MappingStatus, MarketSpec

_CATEGORY_ALIASES = {
    "sport": "sports",
    "sports": "sports",
    "esport": "esports",
    "e-sports": "esports",
    "esports": "esports",
    "crypto": "finance",
    "cryptocurrency": "finance",
    "economics": "finance",
    "finance": "finance",
}


def normalize_category(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = " ".join(value.strip().lower().replace("_", "-").split())
    return _CATEGORY_ALIASES.get(normalized, normalized or None)


def filter_markets_for_categories(
    markets: Iterable[MarketSpec],
    categories: Iterable[str],
    execution_mode: ExecutionMode,
) -> list[MarketSpec]:
    allowed = {category for value in categories if (category := normalize_category(value))}
    result: list[MarketSpec] = []
    for market in markets:
        category = normalize_category(market.category)
        if category is None:
            if not execution_mode.submits_orders:
                result.append(market)
            continue
        if not allowed or category in allowed:
            result.append(replace(market, category=category))
    return result


def route_key(left_venue: str, right_venue: str) -> str:
    names = {
        "Polymarket": "polymarket",
        "Predict.fun": "predict",
        "Myriad": "myriad",
    }
    left = names.get(left_venue, left_venue.strip().lower().replace(".", "_"))
    right = names.get(right_venue, right_venue.strip().lower().replace(".", "_"))
    return f"{left}_{right}"


def is_live_mapping_eligible(
    market: MarketSpec,
    execution_mode: ExecutionMode,
    route: str | None = None,
) -> bool:
    if not execution_mode.submits_orders:
        return True
    return (
        market.mapping_status is MappingStatus.VERIFIED
        and bool(market.rules_fingerprint)
        and bool(market.resolution_source)
        and bool(market.outcome_semantics)
        and normalize_category(market.category) is not None
        and (route is None or route in market.verified_routes)
    )


def rules_fingerprint(
    *,
    title: str,
    resolution_source: str,
    cutoff_at: datetime,
    outcome_semantics: str,
    timezone_name: str = "UTC",
) -> str:
    cutoff = cutoff_at
    if cutoff.tzinfo is None:
        cutoff = cutoff.replace(tzinfo=timezone.utc)
    canonical = {
        "title": " ".join(title.lower().split()),
        "resolution_source": " ".join(resolution_source.lower().split()),
        "cutoff_at": cutoff.astimezone(timezone.utc).isoformat(),
        "outcome_semantics": " ".join(outcome_semantics.lower().split()),
        "timezone": timezone_name,
    }
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
