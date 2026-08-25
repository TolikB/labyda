from __future__ import annotations

import argparse
import json
import os
from typing import Any, cast

import requests
from eth_abi import encode  # type: ignore[attr-defined]
from eth_abi.packed import encode_packed
from eth_account import Account
from eth_typing import HexStr
from eth_utils import keccak, to_bytes  # type: ignore[attr-defined]
from web3 import Web3

from arbitrage_engine.config import load_config, load_operator_env

RELAYER_URL = "https://relayer-v2.polymarket.com"
PUSD_TOKEN_ADDRESS = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
SAFE_FACTORY_ADDRESS = "0xaacFeEa03eb1561C4e67d661e40682Bd20E3541b"
PROXY_FACTORY_ADDRESS = "0xaB45c5A4B0c941a2F231C04C3f49182e1A254052"
DEPOSIT_WALLET_FACTORY_ADDRESS = "0x00000000000Fb5C9ADea0298D729A0CB3823Cc07"
DEPOSIT_WALLET_IMPLEMENTATION_ADDRESS = "0x58CA52ebe0DadfdF531Cde7062e76746de4Db1eB"
DEPOSIT_WALLET_FACTORY_BEACON_ADDRESS = "0x7A18EDfe055488A3128f01F563e5B479D92ffc3a"
DEFAULT_POLYGON_RPC_URL = os.getenv("POLYGON_RPC_URL") or "https://polygon-rpc.com"
DEFAULT_POLYGON_RPC_FALLBACKS = (
    "https://polygon-bor-rpc.publicnode.com",
    "https://polygon-rpc.com",
    "https://1rpc.io/matic",
)
FACTORY_BEACON_SELECTOR = "0x49493a4d"
SAFE_INIT_CODE_HASH = "0x2bce2127ff07fb632d16c8347c4ebf501f4841168bed00d9e6ef715ddb6fcecf"
PROXY_INIT_CODE_HASH = "0xd21df8dc65880a8606f09fe0ce3df9b8869287ab0b058be05aa9e8af6330a00b"
ERC1967_CONST1 = "0xcc3735a920a3ca505d382bbc545af43d6000803e6038573d6000fd5b3d6000f3"
ERC1967_CONST2 = "0x5155f3363d3d373d3d363d7f360894a13ba1a3210667c828492db98dca3e2076"
ERC1967_PREFIX = 0x61003D3D8160233D3973
ERC1967_BEACON_CONST1 = "0xb3582b35133d50545afa5036515af43d6000803e604d573d6000fd5b3d6000f3"
ERC1967_BEACON_CONST2 = "0x1b60e01b36527fa3f0ad74e5423aebfd80d3ef4346578335a9a72aeaee59ff6c"
ERC1967_BEACON_CONST3 = "0x60195155f3363d3d373d3d363d602036600436635c60da"
ERC1967_BEACON_PREFIX = 0x6100523D8160233D3973
POLYMARKET_SPENDERS: dict[str, str] = {
    "ctf_exchange": "0xE111180000d2663C0091e4f400237545B87B996B",
    "neg_risk_adapter": "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296",
    "neg_risk_exchange": "0xe2222d279d744050d28e00520010520000310F59",
}
ERC20_ABI: list[dict[str, Any]] = [
    {
        "constant": True,
        "inputs": [{"name": "account", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [{"name": "owner", "type": "address"}, {"name": "spender", "type": "address"}],
        "name": "allowance",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "type": "function",
    },
]
DEPOSIT_WALLET_ABI: list[dict[str, Any]] = [
    {
        "constant": True,
        "inputs": [],
        "name": "getOwners",
        "outputs": [{"name": "", "type": "address[]"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "getThreshold",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "owner",
        "outputs": [{"name": "", "type": "address"}],
        "type": "function",
    },
]


def _error_text(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"


def _candidate_rpc_urls(preferred: str | None) -> list[str]:
    ordered = [preferred, os.getenv("POLYGON_RPC_URL"), *DEFAULT_POLYGON_RPC_FALLBACKS]
    urls: list[str] = []
    seen: set[str] = set()
    for value in ordered:
        candidate = str(value or "").strip()
        if not candidate or candidate in seen:
            continue
        urls.append(candidate)
        seen.add(candidate)
    return urls


def _connect_web3(preferred_rpc_url: str | None) -> tuple[Web3, str]:
    last_error: BaseException | None = None
    for rpc_url in _candidate_rpc_urls(preferred_rpc_url):
        web3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 20}))
        try:
            _ = web3.eth.chain_id
            return web3, rpc_url
        except Exception as exc:
            last_error = exc
    if last_error is not None:
        raise RuntimeError(f"Polygon RPC connection failed: {_error_text(last_error)}") from last_error
    raise RuntimeError("Polygon RPC connection failed: no candidate RPC URLs were configured")


def _get_create2_address(bytecode_hash: str, factory_address: str, salt: bytes) -> str:
    bytecode_hash_bytes = to_bytes(hexstr=bytecode_hash)
    factory_bytes = to_bytes(hexstr=Web3.to_checksum_address(factory_address))
    address_hash = keccak(b"\xff" + factory_bytes + salt + bytecode_hash_bytes)
    return Web3.to_checksum_address("0x" + address_hash[-20:].hex())


def _deposit_wallet_args(owner_address: str, factory_address: str) -> bytes:
    owner = Web3.to_checksum_address(owner_address)
    factory = Web3.to_checksum_address(factory_address)
    wallet_id = to_bytes(hexstr=owner).rjust(32, b"\x00")
    return encode(["address", "bytes32"], [factory, wallet_id])


def _init_code_hash_erc1967(implementation_address: str, args: bytes) -> str:
    n = len(args)
    combined = ERC1967_PREFIX + (n << 56)
    init_code = (
        combined.to_bytes(10, "big")
        + to_bytes(hexstr=Web3.to_checksum_address(implementation_address))
        + to_bytes(hexstr="0x6009")
        + to_bytes(hexstr=ERC1967_CONST2)
        + to_bytes(hexstr=ERC1967_CONST1)
        + args
    )
    return "0x" + keccak(init_code).hex()


def _init_code_hash_erc1967_beacon(beacon_address: str, args: bytes) -> str:
    n = len(args)
    combined = ERC1967_BEACON_PREFIX + (n << 56)
    init_code = (
        combined.to_bytes(10, "big")
        + to_bytes(hexstr=Web3.to_checksum_address(beacon_address))
        + to_bytes(hexstr=ERC1967_BEACON_CONST3)
        + to_bytes(hexstr=ERC1967_BEACON_CONST2)
        + to_bytes(hexstr=ERC1967_BEACON_CONST1)
        + args
    )
    return "0x" + keccak(init_code).hex()


def _derive_safe_wallet(owner_address: str) -> str:
    owner = Web3.to_checksum_address(owner_address)
    salt = keccak(encode(["address"], [owner]))
    return _get_create2_address(SAFE_INIT_CODE_HASH, SAFE_FACTORY_ADDRESS, salt)


def _derive_proxy_wallet(owner_address: str) -> str:
    owner = Web3.to_checksum_address(owner_address)
    salt = keccak(encode_packed(["address"], [owner]))
    return _get_create2_address(PROXY_INIT_CODE_HASH, PROXY_FACTORY_ADDRESS, salt)


def _derive_uups_deposit_wallet(owner_address: str) -> str:
    args = _deposit_wallet_args(owner_address, DEPOSIT_WALLET_FACTORY_ADDRESS)
    salt = keccak(args)
    bytecode_hash = _init_code_hash_erc1967(DEPOSIT_WALLET_IMPLEMENTATION_ADDRESS, args)
    return _get_create2_address(bytecode_hash, DEPOSIT_WALLET_FACTORY_ADDRESS, salt)


def _factory_beacon_address(web3: Web3) -> str | None:
    try:
        raw = web3.eth.call(
            {
                "to": Web3.to_checksum_address(DEPOSIT_WALLET_FACTORY_ADDRESS),
                "data": cast(HexStr, FACTORY_BEACON_SELECTOR),
            }
        )
    except Exception:
        return DEPOSIT_WALLET_FACTORY_BEACON_ADDRESS
    if not raw:
        return DEPOSIT_WALLET_FACTORY_BEACON_ADDRESS
    candidate = "0x" + raw.hex()[-40:]
    if candidate.lower() == "0x0000000000000000000000000000000000000000":
        return DEPOSIT_WALLET_FACTORY_BEACON_ADDRESS
    return Web3.to_checksum_address(candidate)


def _contract_has_code(web3: Web3, address: str) -> bool:
    try:
        return bool(web3.eth.get_code(Web3.to_checksum_address(address)))
    except Exception:
        return False


def _derive_expected_deposit_wallet(web3: Web3, owner_address: str) -> dict[str, object]:
    uups_wallet = _derive_uups_deposit_wallet(owner_address)
    result: dict[str, object] = {"uups_wallet": uups_wallet}
    beacon_address = _factory_beacon_address(web3)
    result["factory_beacon"] = beacon_address
    if beacon_address is None:
        result["expected_wallet"] = uups_wallet
        result["mode"] = "uups"
        return result
    if _contract_has_code(web3, uups_wallet):
        result["expected_wallet"] = uups_wallet
        result["mode"] = "uups_deployed"
        return result
    args = _deposit_wallet_args(owner_address, DEPOSIT_WALLET_FACTORY_ADDRESS)
    salt = keccak(args)
    beacon_hash = _init_code_hash_erc1967_beacon(beacon_address, args)
    beacon_wallet = _get_create2_address(beacon_hash, DEPOSIT_WALLET_FACTORY_ADDRESS, salt)
    result["beacon_wallet"] = beacon_wallet
    result["expected_wallet"] = beacon_wallet
    result["mode"] = "beacon"
    return result


def _relayer_deployed(wallet_address: str, wallet_type: str) -> dict[str, object]:
    try:
        response = requests.get(
            f"{RELAYER_URL}/deployed",
            params={"address": wallet_address, "type": wallet_type},
            timeout=20,
        )
        response.raise_for_status()
        return {"ok": True, "response": response.json()}
    except Exception as exc:
        return {"ok": False, "error": _error_text(exc)}


def _relayer_auth_headers(api_key: str | None, api_key_address: str | None) -> dict[str, str] | None:
    if not api_key or not api_key_address:
        return None
    return {
        "RELAYER_API_KEY": api_key,
        "RELAYER_API_KEY_ADDRESS": api_key_address,
    }


def _relayer_api_keys(headers: dict[str, str] | None) -> dict[str, object]:
    if headers is None:
        return {"ok": False, "error": "RELAYER_API_KEY and RELAYER_API_KEY_ADDRESS are required"}
    try:
        response = requests.get(f"{RELAYER_URL}/relayer/api/keys", headers=headers, timeout=20)
        response.raise_for_status()
        return {"ok": True, "response": response.json()}
    except Exception as exc:
        return {"ok": False, "error": _error_text(exc)}


def _relayer_transactions(headers: dict[str, str] | None) -> dict[str, object]:
    if headers is None:
        return {"ok": False, "error": "RELAYER_API_KEY and RELAYER_API_KEY_ADDRESS are required"}
    try:
        response = requests.get(f"{RELAYER_URL}/transactions", headers=headers, timeout=20)
        response.raise_for_status()
        transactions = response.json()
        if not isinstance(transactions, list):
            return {"ok": False, "error": f"Unexpected relayer transactions payload: {transactions!r}"}
        summarized = [
            {
                "transactionID": item.get("transactionID"),
                "transactionHash": item.get("transactionHash"),
                "from": item.get("from"),
                "to": item.get("to"),
                "proxyAddress": item.get("proxyAddress"),
                "type": item.get("type"),
                "state": item.get("state"),
                "nonce": item.get("nonce"),
                "createdAt": item.get("createdAt"),
                "updatedAt": item.get("updatedAt"),
            }
            for item in transactions[:20]
            if isinstance(item, dict)
        ]
        return {"ok": True, "count": len(transactions), "recent": summarized}
    except Exception as exc:
        return {"ok": False, "error": _error_text(exc)}


def _wallet_contract_state(web3: Web3, wallet_address: str) -> dict[str, object]:
    contract = web3.eth.contract(address=Web3.to_checksum_address(wallet_address), abi=DEPOSIT_WALLET_ABI)
    payload: dict[str, object] = {}
    try:
        owners = contract.functions.getOwners().call()
        payload["owners"] = [Web3.to_checksum_address(owner) for owner in owners]
    except Exception as exc:
        payload["owners_error"] = _error_text(exc)
    try:
        payload["threshold"] = int(contract.functions.getThreshold().call())
    except Exception as exc:
        payload["threshold_error"] = _error_text(exc)
    if "owners" not in payload:
        try:
            owner = contract.functions.owner().call()
            payload["owner"] = Web3.to_checksum_address(owner)
        except Exception as exc:
            payload["owner_error"] = _error_text(exc)
    return payload


def _pusd_state(web3: Web3, wallet_address: str) -> dict[str, object]:
    contract = web3.eth.contract(address=Web3.to_checksum_address(PUSD_TOKEN_ADDRESS), abi=ERC20_ABI)
    decimals = int(contract.functions.decimals().call())
    balance_raw = int(contract.functions.balanceOf(Web3.to_checksum_address(wallet_address)).call())
    allowances = {
        name: int(
            contract.functions.allowance(
                Web3.to_checksum_address(wallet_address),
                Web3.to_checksum_address(spender),
            ).call()
        )
        for name, spender in POLYMARKET_SPENDERS.items()
    }
    return {
        "token_address": PUSD_TOKEN_ADDRESS,
        "decimals": decimals,
        "balance_raw": str(balance_raw),
        "balance": balance_raw / (10**decimals),
        "allowances": allowances,
    }


def _probe_clob(
    *,
    private_key: str,
    signature_type: int,
    funder: str | None,
    api_key: str | None,
    api_secret: str | None,
    api_passphrase: str | None,
) -> dict[str, object]:
    from py_clob_client_v2 import AssetType, BalanceAllowanceParams, ClobClient  # type: ignore[import-untyped]
    from py_clob_client_v2.clob_types import ApiCreds  # type: ignore[import-untyped]

    try:
        if api_key and api_secret and api_passphrase:
            creds = ApiCreds(
                api_key=api_key,
                api_secret=api_secret,
                api_passphrase=api_passphrase,
            )
            creds_source = "config"
        else:
            creds = ClobClient(
                "https://clob.polymarket.com",
                key=private_key,
                chain_id=137,
            ).create_or_derive_api_key()
            creds_source = "derived"
        client = ClobClient(
            "https://clob.polymarket.com",
            key=private_key,
            chain_id=137,
            creds=creds,
            signature_type=signature_type,
            funder=funder,
        )
    except Exception as exc:
        return {"ok": False, "error": _error_text(exc)}

    payload: dict[str, object] = {"ok": True, "creds_source": creds_source}
    params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=signature_type)
    try:
        payload["get_balance_allowance"] = client.get_balance_allowance(params)
    except Exception as exc:
        payload["get_balance_allowance_error"] = _error_text(exc)
    try:
        payload["update_balance_allowance"] = client.update_balance_allowance(params)
    except Exception as exc:
        payload["update_balance_allowance_error"] = _error_text(exc)
    return payload


def _next_steps_summary(
    *,
    wallet_address: str,
    expected_safe_wallet: str,
    expected_deposit_wallet: str,
    deposit_wallet_deployed: bool,
    deposit_wallet_balance: float | None,
    clob_sig3_error: str | None,
) -> list[str]:
    steps: list[str] = []
    if wallet_address.lower() == expected_safe_wallet.lower():
        steps.append(
            f"Current funded wallet {wallet_address} is the canonical SAFE wallet for the signer, "
            "not the canonical deposit wallet."
        )
        steps.append(
            "For immediate real orders from the currently funded wallet, run Polymarket in SAFE mode "
            f"with signature_type=2 and funder={wallet_address}."
        )
    if not deposit_wallet_deployed:
        steps.append(
            f"Canonical deposit wallet {expected_deposit_wallet} is not deployed; submit relayer "
            "WALLET-CREATE before using signature_type=3."
        )
    if deposit_wallet_balance is not None and deposit_wallet_balance <= 0:
        steps.append(
            f"Canonical deposit wallet {expected_deposit_wallet} has 0 pUSD; move pUSD from SAFE wallet "
            f"{expected_safe_wallet} after deployment."
        )
    if clob_sig3_error:
        steps.append(
            "CLOB POLY_1271 is failing because the owner-to-deposit-wallet mapping is not active for "
            "the canonical deposit wallet path yet."
        )
    steps.append(
        "After deployment and funding, approve trading contracts from the deposit wallet, call "
        "/balance-allowance/update with signature_type=3, then retry real orders."
    )
    return steps


def main() -> None:
    parser = argparse.ArgumentParser(description="Probe Polymarket deposit wallet readiness")
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--wallet-address")
    parser.add_argument("--owner-address")
    parser.add_argument("--rpc-url", default=DEFAULT_POLYGON_RPC_URL)
    parser.add_argument("--relayer-api-key")
    parser.add_argument("--relayer-api-address")
    args = parser.parse_args()

    load_operator_env(args.config)
    config = load_config(args.config)
    if not config.polymarket.private_key:
        raise SystemExit("POLYMARKET_PRIVATE_KEY is required")

    signer_address = Account.from_key(config.polymarket.private_key).address
    wallet_address = args.wallet_address or config.polymarket.funder
    if not wallet_address:
        raise SystemExit("Wallet address is required: pass --wallet-address or configure polymarket.funder")
    owner_address = args.owner_address or signer_address
    relayer_api_key = args.relayer_api_key
    relayer_api_address = args.relayer_api_address or owner_address

    web3, rpc_url_used = _connect_web3(args.rpc_url)
    expected_deposit = _derive_expected_deposit_wallet(web3, owner_address)
    expected_safe = _derive_safe_wallet(owner_address)
    expected_proxy = _derive_proxy_wallet(owner_address)
    wallet_state = _wallet_contract_state(web3, wallet_address)
    owners = wallet_state.get("owners")
    owner_matches = False
    if isinstance(owners, list):
        owner_matches = any(str(owner).lower() == owner_address.lower() for owner in owners)
    elif isinstance(wallet_state.get("owner"), str):
        owner_matches = str(wallet_state["owner"]).lower() == owner_address.lower()

    relayer_headers = _relayer_auth_headers(relayer_api_key, relayer_api_address)
    result = {
        "signer_address": signer_address,
        "owner_address": owner_address,
        "wallet_address": wallet_address,
        "rpc_url_used": rpc_url_used,
        "config_signature_type": config.polymarket.signature_type,
        "config_collateral_token_address": config.polymarket.collateral_token_address,
        "official_pusd_token_address": PUSD_TOKEN_ADDRESS,
        "expected_wallets": {
            "safe_wallet": expected_safe,
            "proxy_wallet": expected_proxy,
            "deposit_wallet": expected_deposit.get("expected_wallet"),
            "deposit_wallet_mode": expected_deposit.get("mode"),
            "deposit_wallet_uups_candidate": expected_deposit.get("uups_wallet"),
            "deposit_wallet_beacon_candidate": expected_deposit.get("beacon_wallet"),
            "deposit_wallet_factory_beacon": expected_deposit.get("factory_beacon"),
            "matches_safe_wallet": wallet_address.lower() == expected_safe.lower(),
            "matches_proxy_wallet": wallet_address.lower() == expected_proxy.lower(),
            "matches_deposit_wallet": wallet_address.lower()
            == str(expected_deposit.get("expected_wallet") or "").lower(),
        },
        "wallet_contract_state": wallet_state,
        "wallet_owner_matches_expected_owner": owner_matches,
        "relayer": {
            "wallet_deployed": _relayer_deployed(wallet_address, "WALLET"),
            "safe_deployed": _relayer_deployed(wallet_address, "SAFE"),
            "api_keys": _relayer_api_keys(relayer_headers),
            "recent_transactions": _relayer_transactions(relayer_headers),
        },
        "onchain_pusd": _pusd_state(web3, wallet_address),
        "clob_signature_type_0": _probe_clob(
            private_key=config.polymarket.private_key,
            signature_type=0,
            funder=None,
            api_key=config.polymarket.api_key,
            api_secret=config.polymarket.api_secret,
            api_passphrase=config.polymarket.api_passphrase,
        ),
        "clob_signature_type_2": _probe_clob(
            private_key=config.polymarket.private_key,
            signature_type=2,
            funder=wallet_address,
            api_key=config.polymarket.api_key,
            api_secret=config.polymarket.api_secret,
            api_passphrase=config.polymarket.api_passphrase,
        ),
        "clob_signature_type_3": _probe_clob(
            private_key=config.polymarket.private_key,
            signature_type=3,
            funder=wallet_address,
            api_key=config.polymarket.api_key,
            api_secret=config.polymarket.api_secret,
            api_passphrase=config.polymarket.api_passphrase,
        ),
        "recommended_runtime_config": {
            "safe_mode_for_current_funded_wallet": {
                "signature_type": 2,
                "funder": expected_safe,
            },
            "deposit_wallet_mode_after_wallet_create": {
                "signature_type": 3,
                "funder": str(expected_deposit.get("expected_wallet")),
            },
        },
    }
    deposit_wallet_address = str(expected_deposit.get("expected_wallet"))
    deposit_wallet_relayer_state = _relayer_deployed(deposit_wallet_address, "WALLET")
    deposit_wallet_relayer_response = deposit_wallet_relayer_state.get("response")
    deposit_wallet_balance_raw = _pusd_state(web3, deposit_wallet_address).get("balance")
    deposit_wallet_balance = (
        float(deposit_wallet_balance_raw)
        if isinstance(deposit_wallet_balance_raw, (int, float))
        else None
    )
    result["next_steps"] = _next_steps_summary(
        wallet_address=wallet_address,
        expected_safe_wallet=expected_safe,
        expected_deposit_wallet=deposit_wallet_address,
        deposit_wallet_deployed=bool(
            deposit_wallet_relayer_response.get("deployed")
            if isinstance(deposit_wallet_relayer_response, dict)
            else False
        ),
        deposit_wallet_balance=deposit_wallet_balance,
        clob_sig3_error=(
            str(result["clob_signature_type_3"].get("get_balance_allowance_error"))
            if isinstance(result["clob_signature_type_3"], dict)
            and result["clob_signature_type_3"].get("get_balance_allowance_error")
            else None
        ),
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
