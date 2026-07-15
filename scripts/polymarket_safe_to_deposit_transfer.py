from __future__ import annotations

import argparse
import json
import os
import time
from decimal import Decimal

import requests
from dotenv import load_dotenv
from eth_account import Account
from polymarket_deposit_wallet_probe import (
    DEFAULT_POLYGON_RPC_URL,
    PUSD_TOKEN_ADDRESS,
    _derive_expected_deposit_wallet,
    _derive_safe_wallet,
    _pusd_state,
)
from web3 import Web3

RELAYER_URL = "https://relayer-v2.polymarket.com"
CHAIN_ID = 137
ERC20_TRANSFER_ABI: list[dict[str, object]] = [
    {
        "constant": False,
        "inputs": [
            {"name": "to", "type": "address"},
            {"name": "amount", "type": "uint256"},
        ],
        "name": "transfer",
        "outputs": [{"name": "", "type": "bool"}],
        "type": "function",
    }
]


def _get_relayer_nonce(owner_address: str, nonce_type: str) -> str:
    response = requests.get(
        f"{RELAYER_URL}/nonce",
        params={"address": owner_address, "type": nonce_type},
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict) or "nonce" not in payload:
        raise RuntimeError(f"Unexpected nonce payload: {payload!r}")
    return str(payload["nonce"])


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


def _scale_pusd_amount(amount_usd: Decimal) -> int:
    return int(amount_usd * Decimal("1000000"))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build or submit a SAFE pUSD transfer into the canonical Polymarket deposit wallet"
    )
    parser.add_argument("--owner-address")
    parser.add_argument("--private-key")
    parser.add_argument("--rpc-url", default=DEFAULT_POLYGON_RPC_URL)
    parser.add_argument("--relayer-api-key", required=True)
    parser.add_argument("--relayer-api-address", required=True)
    parser.add_argument("--amount-usd", help="pUSD amount to transfer; defaults to the full SAFE balance")
    parser.add_argument("--poll-seconds", type=int, default=90)
    parser.add_argument("--confirm-safe-transfer", action="store_true")
    args = parser.parse_args()

    load_dotenv()
    private_key = args.private_key or os.getenv("POLYMARKET_PRIVATE_KEY")
    if not private_key:
        raise SystemExit("POLYMARKET_PRIVATE_KEY or --private-key is required")

    try:
        from py_builder_relayer_client.builder.safe import build_safe_transaction_request
        from py_builder_relayer_client.config import get_contract_config
        from py_builder_relayer_client.models import OperationType, SafeTransaction, SafeTransactionArgs
        from py_builder_relayer_client.signer import Signer
    except ImportError as exc:
        raise SystemExit(
            "py-builder-relayer-client is required; install it with `pip install py-builder-relayer-client`"
        ) from exc

    owner_address = args.owner_address or Account.from_key(private_key).address
    web3 = Web3(Web3.HTTPProvider(args.rpc_url, request_kwargs={"timeout": 20}))
    safe_wallet = _derive_safe_wallet(owner_address)
    expected_deposit = _derive_expected_deposit_wallet(web3, owner_address)
    deposit_wallet = str(expected_deposit["expected_wallet"])

    deployed_response = requests.get(
        f"{RELAYER_URL}/deployed",
        params={"address": deposit_wallet, "type": "WALLET"},
        timeout=20,
    )
    deployed_response.raise_for_status()
    deployed_payload = deployed_response.json()
    if not isinstance(deployed_payload, dict):
        raise RuntimeError(f"Unexpected deployed payload: {deployed_payload!r}")
    if not deployed_payload.get("deployed"):
        raise SystemExit(
            f"Canonical deposit wallet {deposit_wallet} is not deployed; run polymarket_deposit_wallet_create.py first"
        )

    safe_balance = _pusd_state(web3, safe_wallet)
    available_balance = Decimal(str(safe_balance["balance"]))
    requested_amount = Decimal(args.amount_usd) if args.amount_usd else available_balance
    if requested_amount <= 0:
        raise SystemExit("Transfer amount must be positive")
    if requested_amount > available_balance:
        raise SystemExit(
            f"Requested transfer {requested_amount} exceeds SAFE pUSD balance {available_balance}"
        )
    amount_raw = _scale_pusd_amount(requested_amount)

    token = web3.eth.contract(address=Web3.to_checksum_address(PUSD_TOKEN_ADDRESS), abi=ERC20_TRANSFER_ABI)
    calldata = token.functions.transfer(Web3.to_checksum_address(deposit_wallet), amount_raw)._encode_transaction_data()

    nonce = _get_relayer_nonce(owner_address, "SAFE")
    signer = Signer(private_key, CHAIN_ID)
    request = build_safe_transaction_request(
        signer=signer,
        args=SafeTransactionArgs(
            from_address=owner_address,
            nonce=nonce,
            chain_id=CHAIN_ID,
            transactions=[
                SafeTransaction(
                    to=PUSD_TOKEN_ADDRESS,
                    operation=OperationType.Call,
                    data=calldata,
                    value="0",
                )
            ],
        ),
        config=get_contract_config(CHAIN_ID),
        metadata="safe_to_deposit_pusd_transfer",
    ).to_dict()

    preview = {
        "owner_address": owner_address,
        "safe_wallet": safe_wallet,
        "deposit_wallet": deposit_wallet,
        "deposit_wallet_mode": expected_deposit.get("mode"),
        "safe_balance_pusd": float(available_balance),
        "requested_amount_pusd": float(requested_amount),
        "requested_amount_raw": str(amount_raw),
        "safe_nonce": nonce,
        "submit_body": request,
    }
    if not args.confirm_safe_transfer or os.getenv("POLYMARKET_SAFE_TRANSFER_CONFIRM") != "YES":
        print(
            json.dumps(
                {
                    **preview,
                    "submitted": False,
                    "reason": "preview only",
                    "confirm_hint": (
                        "Set POLYMARKET_SAFE_TRANSFER_CONFIRM=YES and pass --confirm-safe-transfer "
                        "to submit the SAFE transfer."
                    ),
                },
                indent=2,
            )
        )
        return

    headers = {
        "RELAYER_API_KEY": args.relayer_api_key,
        "RELAYER_API_KEY_ADDRESS": args.relayer_api_address,
    }
    response = requests.post(f"{RELAYER_URL}/submit", headers=headers, json=request, timeout=20)
    response.raise_for_status()
    submit_payload = response.json()
    if not isinstance(submit_payload, dict):
        raise RuntimeError(f"Unexpected relayer submit payload: {submit_payload!r}")

    transaction_id = str(submit_payload["transactionID"])
    deadline = time.time() + args.poll_seconds
    last_payload: dict[str, object] = submit_payload
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
