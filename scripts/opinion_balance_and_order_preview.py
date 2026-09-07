from __future__ import annotations

import argparse
import asyncio
import json
from decimal import Decimal
from typing import Any

from arbitrage_engine.config import load_config, load_operator_env
from arbitrage_engine.connectors.opinion import (
    OpinionClient,
    build_order_payload,
    execution_token,
)
from arbitrage_engine.models import BinarySide

_PREVIEW_ONLY_NOTICE = (
    "This operator script never submits an Opinion order. Use the runtime canary "
    "path for funded execution."
)


def _redacted_order_payload(client: OpinionClient, payload: dict[str, Any]) -> dict[str, Any]:
    """Report that a signable payload exists without echoing signing material."""
    return {
        "side": payload["side"],
        "order_type": payload["orderType"],
        "price": payload["price"],
        "shares": payload["shares"],
        "maker_amount_in_quote_token": payload["makerAmountInQuoteToken"],
        "signing_key_present": bool(client._config.private_key),  # noqa: SLF001
        "api_key_present": bool(client._config.api_key),  # noqa: SLF001
    }


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Preview Opinion.trade balances, order book depth, and local order construction"
    )
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--market-id", type=int)
    parser.add_argument("--token-id", help="Opinion outcome token id (yesTokenId or noTokenId)")
    parser.add_argument("--outcome-side", choices=("YES", "NO"))
    parser.add_argument("--order-side", choices=("BUY", "SELL"))
    parser.add_argument("--price", type=float)
    parser.add_argument("--size", type=float)
    args = parser.parse_args()

    load_operator_env(args.config)
    app_config = load_config(args.config)
    client = OpinionClient(app_config.opinion)
    try:
        payload: dict[str, Any] = {
            "venue": client.venue_name,
            "chain_id": app_config.opinion.chain_id,
            "api_base_url": app_config.opinion.api_base_url,
            "account_fingerprint": client.reconciliation_account_fingerprint(),
            "supports_full_reconciliation": client.supports_full_reconciliation(),
            "notice": _PREVIEW_ONLY_NOTICE,
        }

        if client.supports_full_reconciliation():
            try:
                payload["cash_balance_usd"] = await client.get_cash_balance()
            except Exception as exc:  # noqa: BLE001 - operator report, not a control path
                payload["cash_balance_error"] = str(exc)

        if args.market_id is None:
            print(json.dumps(payload, indent=2))
            return

        if not args.token_id:
            raise SystemExit("--market-id requires --token-id")

        token = execution_token(args.market_id, args.token_id)
        metadata = await client.get_market_metadata(args.market_id)
        book = await client.watch_order_book(token)
        constraints = await client.get_market_constraints(token)
        payload["market"] = {
            "market_id": args.market_id,
            "execution_token": token,
            "condition_id": metadata.condition_id if metadata is not None else None,
            "yes_token_id": metadata.yes_token_id if metadata is not None else None,
            "no_token_id": metadata.no_token_id if metadata is not None else None,
            "best_bid": book.bids[0].price if book.bids else None,
            "best_ask": book.asks[0].price if book.asks else None,
            "bid_levels": len(book.bids),
            "ask_levels": len(book.asks),
            "book_status": book.status.value,
            "minimum_notional_usd": str(constraints.minimum_notional) if constraints else None,
            "tick_size": str(constraints.tick_size) if constraints else None,
        }

        if args.order_side is None or args.price is None or args.size is None:
            print(json.dumps(payload, indent=2))
            return

        side = BinarySide(args.outcome_side) if args.outcome_side else BinarySide.YES
        preview = await client.preview_buy(
            token,
            side,
            Decimal(str(args.size)),
            Decimal(str(args.price)),
        )
        order_payload = build_order_payload(
            market_id=args.market_id,
            outcome_token=args.token_id,
            action=args.order_side,
            contracts=Decimal(str(args.size)),
            limit_price=Decimal(str(args.price)),
            price_precision=app_config.opinion.price_precision,
        )
        payload["order_preview"] = {
            "outcome_side": side.value,
            "requested_contracts": str(preview.requested_contracts),
            "average_price": str(preview.average_price),
            "notional_usd": str(preview.notional_usd),
            "available_depth_usd": str(preview.available_depth_usd),
            "price_impact_pct": str(preview.price_impact_pct),
            "expected_fee_usd": str(preview.expected_fee_usd),
            "fee_model": preview.fee_quote.model if preview.fee_quote else None,
            "executable": preview.executable,
            "blockers": list(preview.blockers),
            "payload": _redacted_order_payload(client, order_payload),
            "submitted": False,
        }
        print(json.dumps(payload, indent=2))
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
