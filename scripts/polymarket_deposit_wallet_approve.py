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
    POLYMARKET_SPENDERS,
    PUSD_TOKEN_ADDRESS,
    _derive_expected_deposit_wallet,
)
from web3 import Web3

RELAYER_URL = "https://relayer-v2.polymarket.com"
CHAIN_ID = 137
MAX_UINT256 = 2**256 - 1
ERC20_APPROVE_ABI: list[dict[str, object]] = [
    {
        "constant": False,
        "inputs": [
            {"name": "spender", "type": "address"},
            {"name": "amount", "type": "uint256"},
        ],
        "name": "approve",
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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build or submit deposit-wallet pUSD approvals for Polymarket trading contracts"
    )
    parser.add_argument("--owner-address")
    parser.add_argument("--private-key")
    parser.add_argument("--rpc-url", default=DEFAULT_POLYGON_RPC_URL)
    parser.add_argument("--relayer-api-key", required=True)
    parser.add_argument("--relayer-api-address", required=True)
    parser.add_argument("--deadline-seconds", type=int, default=600)
    parser.add_argument("--poll-seconds", type=int, default=90)
    parser.add_argument("--confirm-deposit-approve", action="store_true")
    args = parser.parse_args()

    load_dotenv()
    private_key = args.private_key or os.getenv("POLYMARKET_PRIVATE_KEY")
    if not private_key:
        raise SystemExit("POLYMARKET_PRIVATE_KEY or --private-key is required")

    try:
        from py_builder_relayer_client.builder.deposit_wallet import build_deposit_wallet_batch_request
        from py_builder_relayer_client.config import get_contract_config
        from py_builder_relayer_client.models import DepositWalletCall, DepositWalletTransactionArgs
        from py_builder_relayer_client.signer import Signer
    except ImportError as exc:
        raise SystemExit(
            "py-builder-relayer-client is required; install it with `pip install py-builder-relayer-client`"
        ) from exc

    owner_address = args.owner_address or Account.from_key(private_key).address
    web3 = Web3(Web3.HTTPProvider(args.rpc_url, request_kwargs={"timeout": 20}))
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

    token = web3.eth.contract(address=Web3.to_checksum_address(PUSD_TOKEN_ADDRESS), abi=ERC20_APPROVE_ABI)
    calls = [
        DepositWalletCall(
            target=PUSD_TOKEN_ADDRESS,
            value="0",
            data=token.functions.approve(Web3.to_checksum_address(spender), MAX_UINT256)._encode_transaction_data(),
        )
        for spender in POLYMARKET_SPENDERS.values()
    ]
    nonce = _get_relayer_nonce(owner_address, "WALLET")
    deadline = str(int(time.time()) + args.deadline_seconds)
    signer = Signer(private_key, CHAIN_ID)
    request = build_deposit_wallet_batch_request(
        signer=signer,
        args=DepositWalletTransactionArgs(
            from_address=owner_address,
            chain_id=CHAIN_ID,
            wallet_address=deposit_wallet,
            nonce=nonce,
            deadline=deadline,
            calls=calls,
        ),
        config=get_contract_config(CHAIN_ID),
    ).to_dict()

    preview = {
        "owner_address": owner_address,
        "deposit_wallet": deposit_wallet,
        "deposit_wallet_mode": expected_deposit.get("mode"),
        "wallet_nonce": nonce,
        "deadline": deadline,
        "spenders": POLYMARKET_SPENDERS,
        "submit_body": request,
    }
    if not args.confirm_deposit_approve or os.getenv("POLYMARKET_DEPOSIT_APPROVE_CONFIRM") != "YES":
        print(
            json.dumps(
                {
                    **preview,
                    "submitted": False,
                    "reason": "preview only",
                    "confirm_hint": (
                        "Set POLYMARKET_DEPOSIT_APPROVE_CONFIRM=YES and pass --confirm-deposit-approve "
                        "to submit the deposit-wallet approval batch."
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
    deadline_ts = time.time() + args.poll_seconds
    last_payload: dict[str, object] = submit_payload
    while time.time() < deadline_ts:
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
