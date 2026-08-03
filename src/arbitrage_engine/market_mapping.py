from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime, timedelta

from .models import ExecutionMode, MappingStatus, MarketSpec

_CATEGORY_ALIASES = {
    "sport": "sports",
    "sports": "sports",
    "esport": "sports",
    "e-sports": "sports",
    "esports": "sports",
    "football": "sports",
    "soccer": "sports",
    "crypto": "finance",
    "cryptocurrency": "finance",
    "blockchain": "finance",
    "web3": "finance",
    "digital-assets": "finance",
    "digital assets": "finance",
    "economics": "finance",
    "finance": "finance",
}

_CRYPTO_CATEGORY_NAMES = {
    "crypto",
    "cryptocurrency",
    "blockchain",
    "web3",
    "digital-assets",
    "digital assets",
}
_CRYPTO_TITLE_TERMS = {
    "airdrop",
    "bitcoin",
    "btc",
    "crypto",
    "doge",
    "dogecoin",
    "ethereum",
    "fdv",
    "hyperliquid",
    "solana",
    "token",
    "xrp",
}


def normalize_category(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = " ".join(value.strip().lower().replace("_", "-").split())
    return _CATEGORY_ALIASES.get(normalized, normalized or None)


def normalize_launch_category(value: str | None) -> str | None:
    """Normalize configured category names without collapsing crypto into finance."""

    if value is None:
        return None
    raw = " ".join(value.strip().lower().replace("_", "-").split())
    if raw in _CRYPTO_CATEGORY_NAMES:
        return "crypto"
    return normalize_category(raw)


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
            result.append(market)
    return result


def filter_markets_for_launch_horizon(
    markets: Iterable[MarketSpec],
    categories: Iterable[str],
    *,
    sports_horizon_hours: float,
    crypto_horizon_hours: float,
    category_horizon_hours: Mapping[str, float] | None = None,
    now: datetime | None = None,
) -> list[MarketSpec]:
    requested = {" ".join(value.strip().lower().replace("_", "-").split()) for value in categories}
    crypto_only = bool(requested & {"crypto", "cryptocurrency"}) and not bool(
        requested & {"finance", "economics"}
    )
    reference = now or datetime.now(UTC)
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=UTC)
    reference = reference.astimezone(UTC)
    horizons = {
        "sports": sports_horizon_hours,
        "crypto": crypto_horizon_hours,
    }
    for raw_category, hours in (category_horizon_hours or {}).items():
        category = normalize_launch_category(raw_category)
        if category is not None:
            horizons[category] = hours

    result: list[MarketSpec] = []
    for market in markets:
        category = normalize_category(market.category)
        launch_label = launch_category(market)
        if category == "finance" and launch_label != "crypto" and crypto_only:
            continue
        horizon_hours = horizons.get(launch_label)
        horizon = timedelta(hours=horizon_hours) if horizon_hours is not None else None
        if horizon is None:
            result.append(market)
            continue
        cutoff = market.cutoff_at or market.expires_at
        if cutoff is None:
            continue
        if cutoff.tzinfo is None:
            cutoff = cutoff.replace(tzinfo=UTC)
        remaining = cutoff.astimezone(UTC) - reference
        if timedelta(0) < remaining <= horizon:
            result.append(market)
    return result


def _is_crypto_market(market: MarketSpec) -> bool:
    raw_category = " ".join((market.category or "").strip().lower().replace("_", "-").split())
    if raw_category in _CRYPTO_CATEGORY_NAMES:
        return True
    words = set(re.findall(r"[a-z0-9]+", f"{market.symbol} {market.target_label}".lower()))
    return bool(words & _CRYPTO_TITLE_TERMS)


def launch_category(market: MarketSpec) -> str:
    """Return the production universe label used by route coverage reports."""

    category = normalize_category(market.category)
    if category == "finance" and _is_crypto_market(market):
        return "crypto"
    return category or "unknown"


def route_key(left_venue: str, right_venue: str) -> str:
    names = {
        "Polymarket": "polymarket",
        "Predict.fun": "predict",
        "SX Bet": "sx",
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
        cutoff = cutoff.replace(tzinfo=UTC)
    canonical = {
        "title": " ".join(title.lower().split()),
        "resolution_source": " ".join(resolution_source.lower().split()),
        "cutoff_at": cutoff.astimezone(UTC).isoformat(),
        "outcome_semantics": " ".join(outcome_semantics.lower().split()),
        "timezone": timezone_name,
    }
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
