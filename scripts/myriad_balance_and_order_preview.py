from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from typing import Any

from eth_account import Account

from arbitrage_engine.config import load_config, load_operator_env
from arbitrage_engine.connectors.myriad import ERC20_BALANCE_ABI, MyriadClient, _outcome_id
from arbitrage_engine.models import BinarySide


def _redacted_signed_order(order: dict[str, Any], signature: str) -> dict[str, Any]:
    canonical_payload = json.dumps(
        {"order": order, "signature": signature},
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return {
        "signed_preview_created": True,
        "signed_order_sha256": hashlib.sha256(canonical_payload).hexdigest(),
        "signature_present": bool(signature),
    }


async def _token_balances(client: MyriadClient) -> dict[str, dict[str, Any]]:
    web3_client = client._get_web3_client()
    account = web3_client.account
    if account is None:
        raise RuntimeError("MYRIAD_PRIVATE_KEY is required")
    balances: dict[str, dict[str, Any]] = {}
    for symbol, token_address in client._config.collateral_tokens.items():
        token = web3_client.contract(token_address, ERC20_BALANCE_ABI)
        raw_balance = int(await token.functions.balanceOf(account.address).call())
        decimals = int(await token.functions.decimals().call())
        balances[symbol] = {
            "token_address": token_address,
            "decimals": decimals,
            "balance_raw": str(raw_balance),
            "balance": raw_balance / float(10**decimals),
        }
    return balances


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Preview Myriad balances, orderbook, and redacted signed-order validation"
    )
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--market-id", type=int)
    parser.add_argument("--side", choices=("YES", "NO"))
    parser.add_argument("--order-side", choices=("BUY", "SELL"))
    parser.add_argument("--price", type=float)
    parser.add_argument("--size", type=float)
    parser.add_argument("--time-in-force", choices=("GTC", "GTD", "FOK", "FAK", "PO"), default="FAK")
    parser.add_argument("--confirm-place-order", action="store_true")
    args = parser.parse_args()

    load_operator_env(args.config)
    app_config = load_config(args.config)
    client = MyriadClient(app_config.myriad_markets)
    try:
        if not app_config.myriad_markets.private_key:
            raise SystemExit("MYRIAD_PRIVATE_KEY is required")

        account = Account.from_key(app_config.myriad_markets.private_key)
        balances = await _token_balances(client)
        configured_symbol = app_config.myriad_markets.collateral_symbol
        configured_balance = balances.get(configured_symbol)
        payload: dict[str, Any] = {
            "trader_address": account.address,
            "configured_collateral_symbol": configured_symbol,
            "configured_collateral_balance": configured_balance,
            "all_collateral_balances": balances,
        }

        if args.market_id is None:
            print(json.dumps(payload, indent=2))
            return

        if not args.side or not args.order_side or args.price is None or args.size is None:
            raise SystemExit("--market-id requires --side, --order-side, --price, and --size")

        side = BinarySide(args.side)
        orderbook = await client.get_orderbook(args.market_id, _outcome_id(side))
        signed = await client.sign_order(
            market_id=args.market_id,
            outcome_id=_outcome_id(side),
            side=0 if args.order_side == "BUY" else 1,
            contracts=args.size,
            price=args.price,
        )
        payload["order_preview"] = {
            "market_id": args.market_id,
            "outcome_side": args.side,
            "order_side": args.order_side,
            "price": args.price,
            "size": args.size,
            "time_in_force": args.time_in_force,
            "orderbook": orderbook,
            **_redacted_signed_order(signed.order, signed.signature),
        }

        if not args.confirm_place_order or os.getenv("MYRIAD_ORDER_SUBMIT_CONFIRM") != "YES":
            payload["order_preview"]["submitted"] = False
            payload["order_preview"]["reason"] = "preview only"
            payload["order_preview"]["confirm_hint"] = (
                "Set MYRIAD_ORDER_SUBMIT_CONFIRM=YES and pass --confirm-place-order to submit this order."
            )
            print(json.dumps(payload, indent=2))
            return

        order_id = await client.place_order(signed, time_in_force=args.time_in_force)
        payload["order_preview"]["submitted"] = True
        payload["order_preview"]["order_id"] = order_id
        print(json.dumps(payload, indent=2))
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
