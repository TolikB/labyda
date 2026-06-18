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

    client = MyriadClient(config.myriad_markets)
    side = BinarySide(args.side)
    token_id = f"{args.market_id}:{side.value}"
    book = await client.watch_order_book(token_id)
    passive_price = max(0.01, min(0.10, book.best_bid.price - 0.05))
    contracts = 1.0 / passive_price
    signed = await client.sign_order(args.market_id, _outcome_id(side), 0, contracts, passive_price)

    order_id = await client.place_order(signed, time_in_force="GTC")
    print(f"created order={order_id} price={passive_price:.4f} contracts={contracts:.4f}")
    initial = await client.wait_filled(order_id, 1_000)
    if initial.status not in {ExecutionStatus.OPEN, ExecutionStatus.PARTIAL}:
        raise RuntimeError(f"Expected passive order to remain open, got {initial.status.value}")
    print(f"status before cancel={initial.status.value}")

    await client.cancel_order(order_id)
    cancelled = await client.wait_filled(order_id, 1_000)
    if cancelled.status is not ExecutionStatus.CANCELLED:
        raise RuntimeError(f"Expected CANCELLED, got {cancelled.status.value}")
    print("status after cancel=CANCELLED")


if __name__ == "__main__":
    asyncio.run(run())
