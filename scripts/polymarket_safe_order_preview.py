from __future__ import annotations

import argparse
import json
import os

from arbitrage_engine.config import load_config, load_operator_env
from arbitrage_engine.connectors.polymarket import PolymarketClobClient


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Preview or submit a SAFE-mode Polymarket order from the funded wallet"
    )
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--condition-id", required=True)
    parser.add_argument("--token-id", required=True)
    parser.add_argument("--side", choices=("BUY", "SELL"), required=True)
    parser.add_argument("--price", type=float, required=True)
    parser.add_argument("--size", type=float, required=True)
    parser.add_argument("--tick-size")
    parser.add_argument("--neg-risk", choices=("true", "false"))
    parser.add_argument("--order-type", choices=("GTC", "FOK", "FAK"), default="FOK")
    parser.add_argument("--post-only", action="store_true")
    parser.add_argument("--confirm-post-order", action="store_true")
    args = parser.parse_args()

    load_operator_env(args.config)
    app_config = load_config(args.config)
    client = PolymarketClobClient(app_config.polymarket)
    sdk = client._get_sdk_client()
    market = sdk.get_market(args.condition_id)
    tick_size = args.tick_size or str(market["minimum_tick_size"])
    neg_risk = (args.neg_risk == "true") if args.neg_risk is not None else bool(market["neg_risk"])

    try:
        from py_clob_client_v2 import OrderArgs, OrderType, PartialCreateOrderOptions
        from py_clob_client_v2.order_builder.constants import BUY, SELL  # type: ignore[import-untyped]
    except ImportError as exc:
        raise SystemExit("py-clob-client-v2 is required for Polymarket order previews") from exc

    side = BUY if args.side == "BUY" else SELL
    order = sdk.create_order(
        OrderArgs(token_id=args.token_id, price=args.price, size=args.size, side=side),
        options=PartialCreateOrderOptions(tick_size=tick_size, neg_risk=neg_risk),
    )

    preview = {
        "condition_id": args.condition_id,
        "token_id": args.token_id,
        "market_question": market.get("question"),
        "minimum_tick_size": market.get("minimum_tick_size"),
        "neg_risk": neg_risk,
        "requested_side": args.side,
        "requested_price": args.price,
        "requested_size": args.size,
        "signature_type": int(order.signatureType),
        "maker": order.maker,
        "signer": order.signer,
        "maker_amount": order.makerAmount,
        "taker_amount": order.takerAmount,
        "timestamp": order.timestamp,
        "order_type": args.order_type,
        "post_only": args.post_only,
        "funder": app_config.polymarket.funder,
    }

    if not args.confirm_post_order or os.getenv("POLYMARKET_ORDER_SUBMIT_CONFIRM") != "YES":
        print(
            json.dumps(
                {
                    **preview,
                    "submitted": False,
                    "reason": "preview only",
                    "confirm_hint": (
                        "Set POLYMARKET_ORDER_SUBMIT_CONFIRM=YES and pass --confirm-post-order "
                        "to submit this order."
                    ),
                },
                indent=2,
            )
        )
        return

    order_type = getattr(OrderType, args.order_type)
    response = sdk.post_order(order, order_type=order_type, post_only=args.post_only)
    print(json.dumps({**preview, "submitted": True, "response": response}, indent=2))


if __name__ == "__main__":
    main()
