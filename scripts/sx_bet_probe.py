from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from decimal import ROUND_FLOOR, Decimal, InvalidOperation
from typing import Any

ODDS_PRECISION = Decimal("1e20")
USDC_DECIMALS = Decimal("1e6")


@dataclass(frozen=True)
class TakerLevel:
    side: str
    order_hash: str
    maker_betting_outcome_one: bool
    maker_implied: float
    taker_implied: float
    maker_remaining_usdc: float
    taker_available_usdc: float


def _decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _env_first(*keys: str) -> str | None:
    for key in keys:
        value = os.getenv(key)
        if value not in (None, ""):
            return value
    return None


def _http_json(url: str, *, headers: dict[str, str] | None = None) -> Any:
    merged_headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (compatible; sx-bet-probe/1.0; +https://docs.sx.bet/)",
    }
    if headers:
        merged_headers.update(headers)
    request = urllib.request.Request(url, headers=merged_headers)
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{url} returned HTTP {exc.code}: {body}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"{url} failed: {exc}") from exc


def _to_usdc(raw_units: Decimal) -> float:
    return float((raw_units / USDC_DECIMALS).quantize(Decimal("0.000001")))


def _maker_remaining(order: dict[str, Any]) -> Decimal:
    total_bet_size = _decimal(order.get("totalBetSize", "0"))
    fill_amount = _decimal(order.get("fillAmount", "0"))
    pending_fill_amount = _decimal(order.get("pendingFillAmount", "0"))
    return max(Decimal(0), total_bet_size - fill_amount - pending_fill_amount)


def _taker_available(order: dict[str, Any]) -> Decimal:
    maker_remaining = _maker_remaining(order)
    if maker_remaining <= 0:
        return Decimal(0)
    maker_odds = _decimal(order.get("percentageOdds", "0"))
    if maker_odds <= 0:
        return Decimal(0)
    taker_remaining = (maker_remaining * ODDS_PRECISION / maker_odds) - maker_remaining
    return taker_remaining.quantize(Decimal("1"), rounding=ROUND_FLOOR)


def _order_to_taker_level(order: dict[str, Any]) -> TakerLevel:
    maker_implied = _decimal(order["percentageOdds"]) / ODDS_PRECISION
    taker_implied = Decimal(1) - maker_implied
    maker_betting_outcome_one = bool(order["isMakerBettingOutcomeOne"])
    taker_side = "OUTCOME_TWO" if maker_betting_outcome_one else "OUTCOME_ONE"
    return TakerLevel(
        side=taker_side,
        order_hash=str(order["orderHash"]),
        maker_betting_outcome_one=maker_betting_outcome_one,
        maker_implied=float(maker_implied),
        taker_implied=float(taker_implied),
        maker_remaining_usdc=_to_usdc(_maker_remaining(order)),
        taker_available_usdc=_to_usdc(_taker_available(order)),
    )


def _sort_book(levels: list[TakerLevel]) -> list[TakerLevel]:
    return sorted(levels, key=lambda level: (-level.taker_implied, -level.taker_available_usdc, level.order_hash))


def _fetch_best_levels(api_base_url: str, market_hash: str) -> dict[str, list[dict[str, Any]]]:
    payload = _http_json(
        f"{api_base_url.rstrip('/')}/orders?marketHashes={urllib.parse.quote(market_hash)}"
    )
    raw_orders = payload.get("data") or []
    if not isinstance(raw_orders, list):
        raise RuntimeError("SX Bet orders payload did not contain a list in data")
    levels = [_order_to_taker_level(order) for order in raw_orders if isinstance(order, dict)]
    outcome_one = _sort_book([level for level in levels if level.side == "OUTCOME_ONE"])
    outcome_two = _sort_book([level for level in levels if level.side == "OUTCOME_TWO"])
    return {
        "outcome_one": [asdict(level) for level in outcome_one[:10]],
        "outcome_two": [asdict(level) for level in outcome_two[:10]],
    }


def _active_markets(api_base_url: str) -> list[dict[str, Any]]:
    payload = _http_json(f"{api_base_url.rstrip('/')}/markets/active?perPage=25")
    data = payload.get("data") or {}
    markets = data.get("markets") if isinstance(data, dict) else None
    if not isinstance(markets, list) or not markets:
        raise RuntimeError("SX Bet active markets payload did not contain any markets")
    return [market for market in markets if isinstance(market, dict)]


def _choose_market(api_base_url: str, explicit_market_hash: str | None) -> dict[str, Any]:
    if explicit_market_hash:
        payload = _http_json(
            f"{api_base_url.rstrip('/')}/markets/find?marketHashes={urllib.parse.quote(explicit_market_hash)}"
        )
        data = payload.get("data") or []
        if isinstance(data, list) and data:
            return data[0]
        raise RuntimeError(f"market {explicit_market_hash} not found on SX Bet")
    for market in _active_markets(api_base_url):
        if market.get("marketHash"):
            return market
    raise RuntimeError("SX Bet active markets payload did not contain a usable market")


def _fetch_realtime_token(api_base_url: str, api_key: str) -> dict[str, Any]:
    payload = _http_json(
        f"{api_base_url.rstrip('/')}/user/realtime-token/api-key",
        headers={"x-api-key": api_key},
    )
    token = payload.get("token")
    return {"token_present": bool(token), "token_prefix": str(token)[:16] if token else ""}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Probe the live SX Bet API contract and derive taker-side orderbook data."
    )
    parser.add_argument(
        "--api-base-url",
        default=_env_first("SX_BET_API_BASE_URL", "SX_API_BASE_URL") or "https://api.sx.bet",
    )
    parser.add_argument("--market-hash", default=_env_first("SX_BET_MARKET_HASH", "SX_MARKET_HASH"))
    parser.add_argument("--api-key", default=_env_first("SX_BET_API_KEY", "SX_API_KEY"))
    args = parser.parse_args()

    try:
        metadata = _http_json(f"{args.api_base_url.rstrip('/')}/metadata")
        market = _choose_market(args.api_base_url, args.market_hash)
        market_hash = str(market["marketHash"])
        best_levels = _fetch_best_levels(args.api_base_url, market_hash)
        if not args.market_hash and not any(best_levels.values()):
            for candidate in _active_markets(args.api_base_url):
                candidate_hash = str(candidate.get("marketHash") or "")
                if not candidate_hash or candidate_hash == market_hash:
                    continue
                candidate_levels = _fetch_best_levels(args.api_base_url, candidate_hash)
                if any(candidate_levels.values()):
                    market = candidate
                    market_hash = candidate_hash
                    best_levels = candidate_levels
                    break
        output: dict[str, Any] = {
            "api_base_url": args.api_base_url,
            "metadata": metadata.get("data", metadata),
            "market": market,
            "taker_book": best_levels,
        }
        if args.api_key:
            output["realtime_token"] = _fetch_realtime_token(args.api_base_url, args.api_key)
        print(json.dumps(output, indent=2, sort_keys=True))
        return 0
    except (KeyError, InvalidOperation, RuntimeError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
