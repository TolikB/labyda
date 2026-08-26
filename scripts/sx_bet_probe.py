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
_MAX_MARKET_PAGES = 500
_OFFICIAL_AUTHENTICATED_API_ORIGINS = {
    "https://api.sx.bet",
    "https://api.toronto.sx.bet",
}


class _RejectRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> urllib.request.Request | None:
        del req, fp, code, msg, headers, newurl
        return None


_AUTHENTICATED_OPENER = urllib.request.build_opener(_RejectRedirects())


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
    open_request = (
        _AUTHENTICATED_OPENER.open
        if any(key.lower() == "x-sx-api-key" for key in merged_headers)
        else urllib.request.urlopen
    )
    try:
        with open_request(request, timeout=20) as response:
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
    return sorted(levels, key=lambda level: (level.taker_implied, -level.taker_available_usdc, level.order_hash))


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


def _fetch_v3_best_levels(api_base_url: str, market_hash: str) -> dict[str, list[dict[str, Any]]]:
    payload = _http_json(
        f"{api_base_url.rstrip('/')}/orderbook-v3/snapshot?marketHash={urllib.parse.quote(market_hash)}"
    )
    data = payload.get("data") or {}
    if not isinstance(data, dict) or not isinstance(data.get("outcomeOne"), list) or not isinstance(
        data.get("outcomeTwo"),
        list,
    ):
        raise RuntimeError("SX Bet V3 snapshot is missing aggregated outcome levels")

    def taker_levels(maker_levels: list[Any], side: str) -> list[dict[str, Any]]:
        levels: list[dict[str, Any]] = []
        for index, raw in enumerate(maker_levels):
            if not isinstance(raw, dict):
                continue
            maker_probability = _decimal(raw.get("percentageOdds", "0")) / ODDS_PRECISION
            maker_stake = _decimal(raw.get("size", "0"))
            if maker_probability <= 0 or maker_probability >= 1 or maker_stake <= 0:
                continue
            taker_probability = Decimal(1) - maker_probability
            taker_stake = (maker_stake / maker_probability) - maker_stake
            levels.append(
                asdict(
                    TakerLevel(
                        side=side,
                        order_hash=f"v3:{data.get('version')}:{side}:{index}",
                        maker_betting_outcome_one=side == "OUTCOME_TWO",
                        maker_implied=float(maker_probability),
                        taker_implied=float(taker_probability),
                        maker_remaining_usdc=_to_usdc(maker_stake),
                        taker_available_usdc=_to_usdc(taker_stake),
                    )
                )
            )
        levels.sort(key=lambda level: (level["taker_implied"], -level["taker_available_usdc"]))
        return levels

    # To take outcome one, consume makers betting outcome two, and vice versa.
    return {
        "outcome_one": taker_levels(data["outcomeTwo"], "OUTCOME_ONE"),
        "outcome_two": taker_levels(data["outcomeOne"], "OUTCOME_TWO"),
    }


def _active_markets(api_base_url: str) -> list[dict[str, Any]]:
    endpoint = f"{api_base_url.rstrip('/')}/markets/active"
    markets: list[dict[str, Any]] = []
    pagination_key: str | None = None
    seen_pagination_keys: set[str] = set()
    for _page in range(_MAX_MARKET_PAGES):
        params = {"pageSize": "100"}
        if pagination_key:
            params["paginationKey"] = pagination_key
        payload = _http_json(f"{endpoint}?{urllib.parse.urlencode(params)}")
        data = payload.get("data") or {}
        page_markets = data.get("markets") if isinstance(data, dict) else None
        if not isinstance(page_markets, list):
            raise RuntimeError("SX Bet active markets payload did not contain a markets list")
        markets.extend(market for market in page_markets if isinstance(market, dict))
        pagination_key = str(data.get("nextKey") or "") or None
        if not pagination_key:
            break
        if pagination_key in seen_pagination_keys:
            raise RuntimeError("SX Bet active markets pagination repeated a cursor")
        seen_pagination_keys.add(pagination_key)
    else:
        raise RuntimeError(f"SX Bet active markets exceeded {_MAX_MARKET_PAGES} pages")
    if not markets:
        raise RuntimeError("SX Bet active markets payload did not contain any markets")
    return markets


def _choose_market(api_base_url: str, explicit_market_hash: str | None) -> dict[str, Any]:
    if explicit_market_hash:
        payload = _http_json(
            f"{api_base_url.rstrip('/')}/markets/find?marketHashes={urllib.parse.quote(explicit_market_hash)}"
        )
        data = payload.get("data") or []
        if isinstance(data, list) and data and isinstance(data[0], dict):
            return data[0]
        raise RuntimeError(f"market {explicit_market_hash} not found on SX Bet")
    for market in _active_markets(api_base_url):
        if market.get("marketHash"):
            return market
    raise RuntimeError("SX Bet active markets payload did not contain a usable market")


def _fetch_realtime_token(api_base_url: str, api_key: str, api_version: str) -> dict[str, Any]:
    normalized_origin = api_base_url.rstrip("/")
    if normalized_origin not in _OFFICIAL_AUTHENTICATED_API_ORIGINS:
        raise ValueError("SX Bet API keys may only be sent to an official SX Bet API host")
    token_path = "/user/realtime-token-v3/api-key" if api_version == "v3" else "/user/realtime-token/api-key"
    api_key_header = "x-sx-api-key" if api_version == "v3" else "x-api-key"
    payload = _http_json(
        f"{normalized_origin}{token_path}",
        headers={api_key_header: api_key},
    )
    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    token = data.get("token") if isinstance(data, dict) else None
    return {"token_present": bool(token)}


def _fetch_v3_account_contracts(api_base_url: str, api_key: str) -> dict[str, Any]:
    normalized_origin = api_base_url.rstrip("/")
    if normalized_origin not in _OFFICIAL_AUTHENTICATED_API_ORIGINS:
        raise ValueError("SX Bet API keys may only be sent to an official SX Bet API host")
    headers = {"x-sx-api-key": api_key}

    def fetch(path: str, **query: str | int) -> Any:
        suffix = f"?{urllib.parse.urlencode(query)}" if query else ""
        return _http_json(f"{normalized_origin}{path}{suffix}", headers=headers)

    realtime = _fetch_realtime_token(normalized_origin, api_key, "v3")
    proxy_payload = fetch("/user/proxy")
    balance_payload = fetch("/user/balance-v3")
    fee_payload = fetch("/user/fees-v3")
    orders_payload = fetch("/orders-v3", perPage=1)
    fills_payload = fetch("/fills-v3", perPage=1)
    positions_payload = fetch("/positions-v3", status="MATCHED,LOCKED", perPage=1)

    proxy = _response_dict(proxy_payload)
    balances = _response_records(balance_payload, "balances")
    fees = _response_dict(fee_payload)
    return {
        "realtime_token": realtime,
        "proxy": {
            "response_valid": bool(proxy),
            "deployed": bool(proxy.get("deployed")),
            "proxy_address_present": bool(proxy.get("obv3ProxyWalletAddress") or proxy.get("proxyWalletAddress")),
        },
        "balance": {
            "records": len(balances),
            "available_amount_present": any("availableAmount" in row for row in balances),
        },
        "fees": {
            "taker_payout_fee_present": "takerPayoutFee" in fees,
            "refund_fee_present": "refundFee" in fees,
        },
        "orders": {"records": len(_response_records(orders_payload, "orders"))},
        "fills": {"records": len(_response_records(fills_payload, "fills"))},
        "positions": {"records": len(_response_records(positions_payload, "positions"))},
    }


def _response_dict(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    data = payload.get("data", payload)
    return data if isinstance(data, dict) else {}


def _response_records(payload: Any, key: str) -> list[dict[str, Any]]:
    data = _response_dict(payload)
    rows = data.get(key, [])
    if not isinstance(rows, list):
        raise RuntimeError(f"SX Bet V3 authenticated response is missing {key}")
    return [row for row in rows if isinstance(row, dict)]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Probe the live SX Bet API contract and derive taker-side orderbook data."
    )
    parser.add_argument(
        "--api-base-url",
        default=_env_first("SX_BET_API_BASE_URL", "SX_API_BASE_URL") or "https://api.sx.bet",
    )
    parser.add_argument(
        "--api-version",
        choices=("v2", "v3"),
        default=(_env_first("SX_BET_API_VERSION") or "v2").lower(),
    )
    parser.add_argument("--market-hash", default=_env_first("SX_BET_MARKET_HASH", "SX_MARKET_HASH"))
    args = parser.parse_args()
    api_key = _env_first("SX_BET_API_KEY", "SX_API_KEY")

    try:
        metadata_path = "/metadata/obv3" if args.api_version == "v3" else "/metadata"
        metadata = _http_json(f"{args.api_base_url.rstrip('/')}{metadata_path}")
        market = _choose_market(args.api_base_url, args.market_hash)
        market_hash = str(market["marketHash"])
        fetch_levels = _fetch_v3_best_levels if args.api_version == "v3" else _fetch_best_levels
        best_levels = fetch_levels(args.api_base_url, market_hash)
        if not args.market_hash and not any(best_levels.values()):
            for candidate in _active_markets(args.api_base_url):
                candidate_hash = str(candidate.get("marketHash") or "")
                if not candidate_hash or candidate_hash == market_hash:
                    continue
                candidate_levels = fetch_levels(args.api_base_url, candidate_hash)
                if any(candidate_levels.values()):
                    market = candidate
                    market_hash = candidate_hash
                    best_levels = candidate_levels
                    break
        output: dict[str, Any] = {
            "api_base_url": args.api_base_url,
            "api_version": args.api_version,
            "metadata": metadata.get("data", metadata),
            "market": market,
            "taker_book": best_levels,
        }
        if api_key:
            if args.api_version == "v3":
                output["authenticated_contracts"] = _fetch_v3_account_contracts(
                    args.api_base_url,
                    api_key,
                )
            else:
                output["realtime_token"] = _fetch_realtime_token(
                    args.api_base_url,
                    api_key,
                    args.api_version,
                )
        print(json.dumps(output, indent=2, sort_keys=True))
        return 0
    except (KeyError, InvalidOperation, RuntimeError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
