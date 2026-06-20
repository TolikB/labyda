from __future__ import annotations

import argparse
import asyncio
import os

from dotenv import load_dotenv

from arbitrage_engine.config import load_config
from arbitrage_engine.connectors.myriad import MyriadClient, _outcome_id
from arbitrage_engine.models import BinarySide, ExecutionStatus


async def run() -> None:
    parser = argparse.ArgumentParser(description="Safe Myriad mainnet create/status/cancel smoke test")
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--market-id", type=int, required=True)
    parser.add_argument("--side", choices=("YES", "NO"), default="YES")
    parser.add_argument("--max-notional-usd", type=float, default=1.0)
    parser.add_argument("--confirm-live-smoke", action="store_true")
    args = parser.parse_args()

    load_dotenv()
    if not args.confirm_live_smoke or os.getenv("LIVE_SMOKE_CONFIRM") != "YES":
        raise SystemExit("Refusing live order: pass --confirm-live-smoke and set LIVE_SMOKE_CONFIRM=YES")

    config = load_config(args.config)
    if not config.myriad_markets.enabled:
        raise SystemExit("myriad_markets.enabled must be true")
    if not config.myriad_markets.private_key:
        raise SystemExit("MYRIAD_PRIVATE_KEY is required")
    if not 0 < args.max_notional_usd <= 1.0:
        raise SystemExit("--max-notional-usd must be greater than 0 and no more than $1")

    client = MyriadClient(config.myriad_markets)
    order_id: str | None = None
    try:
        side = BinarySide(args.side)
        token_id = f"{args.market_id}:{side.value}"
        book = await client.watch_order_book(token_id)
        if book.best_ask.price <= 0.02:
            raise RuntimeError("Book has no safe passive price below the best ask; refusing smoke order")
        passive_price = max(0.01, min(0.10, book.best_bid.price - 0.05, book.best_ask.price - 0.01))
        if passive_price >= book.best_ask.price:
            raise RuntimeError("Calculated smoke price could cross the spread; refusing order")
        contracts = args.max_notional_usd / passive_price
        signed = await client.sign_order(args.market_id, _outcome_id(side), 0, contracts, passive_price)

        order_id = await client.place_order(signed, time_in_force="GTC")
        print(
            f"created order={order_id} price={passive_price:.4f} contracts={contracts:.4f} "
            f"notional={args.max_notional_usd:.2f}"
        )
        initial = await client.wait_filled(order_id, 1_000)
        if initial.status is not ExecutionStatus.OPEN:
            raise RuntimeError(
                f"Expected passive order to remain OPEN, got {initial.status.value}; "
                f"filled={initial.amount_filled:.8f}"
            )
        print("status before cancel=OPEN")

        await client.cancel_order(order_id)
        cancelled = await client.wait_filled(order_id, 1_000)
        if cancelled.status is not ExecutionStatus.CANCELLED or cancelled.amount_filled:
            raise RuntimeError(
                f"Expected zero-fill CANCELLED, got {cancelled.status.value}; "
                f"filled={cancelled.amount_filled:.8f}"
            )
        order_id = None
        print("status after cancel=CANCELLED filled=0")
    finally:
        if order_id is not None:
            try:
                await client.cancel_order(order_id)
            except Exception as exc:
                print(f"EMERGENCY: cleanup cancel failed for {order_id}: {exc}")
        await client.close()


if __name__ == "__main__":
    asyncio.run(run())
