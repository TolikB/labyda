from __future__ import annotations

import argparse
import json
import os
import time

import requests
from dotenv import load_dotenv
from eth_account import Account

from polymarket_deposit_wallet_probe import (
    DEFAULT_POLYGON_RPC_URL,
    DEPOSIT_WALLET_FACTORY_ADDRESS,
    RELAYER_URL,
    _derive_expected_deposit_wallet,
)
from web3 import Web3


def _get_transaction(transaction_id: str, headers: dict[str, str]) -> dict[str, object]:
    response = requests.get(
        f"{RELAYER_URL}/transaction",
        params={"id": transaction_id},
        headers=headers,
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError(f"Unexpected relayer transaction payload: {payload!r}")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Submit a Polymarket deposit-wallet WALLET-CREATE transaction")
    parser.add_argument("--owner-address")
    parser.add_argument("--private-key")
    parser.add_argument("--rpc-url", default=DEFAULT_POLYGON_RPC_URL)
    parser.add_argument("--relayer-api-key", required=True)
    parser.add_argument("--relayer-api-address", required=True)
    parser.add_argument("--poll-seconds", type=int, default=90)
    parser.add_argument("--confirm-wallet-create", action="store_true")
    args = parser.parse_args()

    load_dotenv()
    private_key = args.private_key or os.getenv("POLYMARKET_PRIVATE_KEY")
    if not private_key:
        raise SystemExit("POLYMARKET_PRIVATE_KEY or --private-key is required")
    owner_address = args.owner_address or Account.from_key(private_key).address
    web3 = Web3(Web3.HTTPProvider(args.rpc_url, request_kwargs={"timeout": 20}))
    expected = _derive_expected_deposit_wallet(web3, owner_address)
    expected_wallet = str(expected["expected_wallet"])

    deployed_response = requests.get(
        f"{RELAYER_URL}/deployed",
        params={"address": expected_wallet, "type": "WALLET"},
        timeout=20,
    )
    deployed_response.raise_for_status()
    deployed_payload = deployed_response.json()
    if not isinstance(deployed_payload, dict):
        raise SystemExit(f"Unexpected deployed payload: {deployed_payload!r}")
    already_deployed = bool(deployed_payload.get("deployed"))

    headers = {
        "RELAYER_API_KEY": args.relayer_api_key,
        "RELAYER_API_KEY_ADDRESS": args.relayer_api_address,
    }
    preview = {
        "owner_address": owner_address,
        "expected_deposit_wallet": expected_wallet,
        "deposit_wallet_mode": expected.get("mode"),
        "deposit_wallet_factory": DEPOSIT_WALLET_FACTORY_ADDRESS,
        "already_deployed": already_deployed,
        "submit_body": {
            "type": "WALLET-CREATE",
            "from": owner_address,
            "to": DEPOSIT_WALLET_FACTORY_ADDRESS,
        },
    }
    if already_deployed:
        print(json.dumps({**preview, "submitted": False, "reason": "wallet already deployed"}, indent=2))
        return
    if not args.confirm_wallet_create or os.getenv("POLYMARKET_WALLET_CREATE_CONFIRM") != "YES":
        print(
            json.dumps(
                {
                    **preview,
                    "submitted": False,
                    "reason": "preview only",
                    "confirm_hint": (
                        "Set POLYMARKET_WALLET_CREATE_CONFIRM=YES and pass --confirm-wallet-create "
                        "to submit WALLET-CREATE."
                    ),
                },
                indent=2,
            )
        )
        return

    response = requests.post(
        f"{RELAYER_URL}/submit",
        headers=headers,
        json=preview["submit_body"],
        timeout=20,
    )
    response.raise_for_status()
    submit_payload = response.json()
    if not isinstance(submit_payload, dict):
        raise RuntimeError(f"Unexpected relayer submit payload: {submit_payload!r}")
    transaction_id = str(submit_payload["transactionID"])
    deadline = time.time() + args.poll_seconds
    last_payload = submit_payload
    while time.time() < deadline:
        time.sleep(3)
        last_payload = _get_transaction(transaction_id, headers)
        state = str(last_payload.get("state") or "")
        if state in {"STATE_CONFIRMED", "STATE_FAILED"}:
            break
    print(
        json.dumps(
            {
                **preview,
                "submitted": True,
                "transactionID": transaction_id,
                "final_transaction": last_payload,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
