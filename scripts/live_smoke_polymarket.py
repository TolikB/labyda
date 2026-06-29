from __future__ import annotations

import argparse
import asyncio
import os
from decimal import Decimal, ROUND_DOWN

import requests
from dotenv import load_dotenv

from arbitrage_engine.config import load_config
from arbitrage_engine.connectors.polymarket import PolymarketClobClient
from arbitrage_engine.models import BinarySide, ExecutionStatus


def _extract_order_id(payload: object) -> str:
    if not isinstance(payload, dict):
        raise RuntimeError(f"Unsupported Polymarket order payload: {payload!r}")
    for key in ("orderID", "order_id", "id", "hash"):
        value = payload.get(key)
        if value:
            return str(value)
    raise RuntimeError(f"Polymarket order response did not include an order id: {payload!r}")


def _quantize_down(value: float, tick_size: Decimal) -> float:
    return float((Decimal(str(value)) / tick_size).to_integral_value(rounding=ROUND_DOWN) * tick_size)


def _token_side(market_payload: dict[str, object], token_id: str) -> BinarySide:
    for token in market_payload.get("tokens", []):
        if not isinstance(token, dict):
            continue
        if str(token.get("token_id")) != token_id:
            continue
        outcome = str(token.get("outcome") or "").strip().lower()
        if outcome == "yes":
            return BinarySide.YES
        if outcome == "no":
            return BinarySide.NO
    raise RuntimeError(f"Could not infer binary side for Polymarket token {token_id}")


async def run() -> None:
    parser = argparse.ArgumentParser(description="Safe Polymarket mainnet create/status/cancel smoke test")
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--token-id", required=True)
    parser.add_argument("--max-notional-usd", type=float, default=1.0)
    parser.add_argument("--confirm-live-smoke", action="store_true")
    args = parser.parse_args()

    load_dotenv()
    if not args.confirm_live_smoke or os.getenv("LIVE_SMOKE_CONFIRM") != "YES":
        raise SystemExit("Refusing live order: pass --confirm-live-smoke and set LIVE_SMOKE_CONFIRM=YES")

    config = load_config(args.config)
    if not config.polymarket.private_key:
        raise SystemExit("POLYMARKET_PRIVATE_KEY is required")
    if not 0 < args.max_notional_usd <= 1.0:
        raise SystemExit("--max-notional-usd must be greater than 0 and no more than $1")

    token_response = requests.get(
        f"{config.polymarket.api_base_url.rstrip('/')}/markets-by-token/{args.token_id}",
        timeout=15,
    )
    token_response.raise_for_status()
    token_payload = token_response.json()
    condition_id = str(token_payload["condition_id"])

    market_response = requests.get(
        f"{config.polymarket.api_base_url.rstrip('/')}/markets/{condition_id}",
        timeout=15,
    )
    market_response.raise_for_status()
    market_payload = market_response.json()
    if not market_payload.get("active") or not market_payload.get("accepting_orders"):
        raise RuntimeError("Polymarket market is not active and accepting orders")

    tick_size = Decimal(str(market_payload["minimum_tick_size"]))
    minimum_order_size = float(market_payload.get("minimum_order_size") or 1.0)
    neg_risk = bool(market_payload.get("neg_risk"))
    side = _token_side(market_payload, args.token_id)

    client = PolymarketClobClient(config.polymarket)
    order_id: str | None = None
    try:
        book = await client.watch_order_book(args.token_id)
        if book.best_bid is None or book.best_ask is None:
            raise RuntimeError("Polymarket book is not two-sided; refusing smoke order")
        if book.best_ask.price <= float(tick_size):
            raise RuntimeError("Book has no safe passive price below the best ask; refusing smoke order")

        passive_target = min(book.best_ask.price - float(tick_size), max(book.best_bid.price, float(tick_size)))
        passive_price = _quantize_down(passive_target, tick_size)
        if passive_price < float(tick_size) or passive_price >= book.best_ask.price:
            raise RuntimeError("Calculated smoke price could cross the spread; refusing order")

        contracts = args.max_notional_usd / passive_price
        if contracts < minimum_order_size:
            raise RuntimeError(
                f"Requested notional is too small for Polymarket minimum order size: "
                f"contracts={contracts:.6f} minimum={minimum_order_size:.6f}"
            )

        sdk = client._get_sdk_client()
        from py_clob_client_v2 import OrderArgs, OrderType, PartialCreateOrderOptions
        from py_clob_client_v2.order_builder.constants import BUY

        response = client._sdk_call(
            lambda current: current.create_and_post_order(
                OrderArgs(token_id=args.token_id, price=passive_price, size=contracts, side=BUY),
                options=PartialCreateOrderOptions(tick_size=str(tick_size), neg_risk=neg_risk),
                order_type=OrderType.GTC,
            )
        )
        order_id = _extract_order_id(response)
        client._order_amounts[order_id] = contracts
        client._order_prices[order_id] = passive_price

        print(
            f"created order={order_id} token={args.token_id} condition_id={condition_id} "
            f"price={passive_price:.4f} contracts={contracts:.4f} notional={args.max_notional_usd:.2f}"
        )
        initial = await client.wait_filled(order_id, 1_500)
        if initial.amount_filled > 0:
            raise RuntimeError(
                f"Expected passive Polymarket order to remain unfilled, got "
                f"status={initial.status.value} filled={initial.amount_filled:.8f}"
            )
        print(f"status before cancel={initial.status.value} filled={initial.amount_filled:.8f}")

        await client.cancel_order(order_id)
        cancelled = await client.wait_filled(order_id, 1_500)
        if cancelled.status not in {ExecutionStatus.CANCELLED, ExecutionStatus.EXPIRED} or cancelled.amount_filled:
            raise RuntimeError(
                f"Expected zero-fill CANCELLED/EXPIRED, got {cancelled.status.value}; "
                f"filled={cancelled.amount_filled:.8f}"
            )
        order_id = None
        print(f"status after cancel={cancelled.status.value} filled={cancelled.amount_filled:.8f}")
    finally:
        if order_id is not None:
            try:
                await client.cancel_order(order_id)
            except Exception as exc:
                print(f"EMERGENCY: cleanup cancel failed for {order_id}: {exc}")
        await client.close()


if __name__ == "__main__":
    asyncio.run(run())
