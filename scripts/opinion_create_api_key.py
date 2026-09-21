"""Create (or revoke) an Opinion.trade OpenAPI key with the venue's EIP-712 self-service flow.

Opinion issues one API key per wallet, on request, against a signature from
that wallet: sign ``OpinionApiKeyAuth{walletAddress, action, timestamp}`` under
the domain ``{"name": "Opinion OpenAPI", "version": "1", "chainId": 56}`` and
POST (create) or DELETE (revoke) ``/auth/api-key`` with the address, signature
and timestamp in headers. The key works for the OpenAPI, the WebSocket and the
CLOB SDK, and is live at the gateway about fifteen seconds after creation.

The signing key never leaves this process: it is read from ``OPINION_PRIVATE_KEY``
in the environment or a Compose env file, and only the resulting API key is
written back, into that same file, under ``OPINION_API_KEY``.

Run on the host that holds the env file:

    ./ops/operator_python.sh scripts/opinion_create_api_key.py --env-file .env.production --write

or, to revoke and re-issue:

    ./ops/operator_python.sh scripts/opinion_create_api_key.py --env-file .env.production --rotate --write
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

DEFAULT_HOST = "https://openapi.opinion.trade/openapi"
CHAIN_ID = 56
DOMAIN = {"name": "Opinion OpenAPI", "version": "1", "chainId": CHAIN_ID}
TYPES = {
    "OpinionApiKeyAuth": [
        {"name": "walletAddress", "type": "address"},
        {"name": "action", "type": "string"},
        {"name": "timestamp", "type": "string"},
    ]
}
_ENV_LINE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$")


def _read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        match = _ENV_LINE.match(line)
        if match:
            values[match.group(1)] = match.group(2).strip("'\"")
    return values


def _write_env_value(path: Path, key: str, value: str) -> None:
    """Replace ``key=`` in the env file or append it; keep the file's mode."""
    lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    replaced = False
    for index, line in enumerate(lines):
        match = _ENV_LINE.match(line)
        if match and match.group(1) == key:
            lines[index] = f"{key}={value}"
            replaced = True
    if not replaced:
        lines.append(f"{key}={value}")
    mode = path.stat().st_mode & 0o777 if path.exists() else 0o600
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    os.chmod(path, mode)


def _sign(private_key: str, wallet: str, action: str, timestamp: str) -> str:
    from eth_account import Account
    from eth_account.messages import encode_typed_data

    message = {"walletAddress": wallet, "action": action, "timestamp": timestamp}
    signable = encode_typed_data(domain_data=DOMAIN, message_types=TYPES, message_data=message)
    signed = Account.sign_message(signable, private_key=private_key)
    return "0x" + signed.signature.hex().removeprefix("0x")


def _request(host: str, method: str, wallet: str, signature: str, timestamp: str) -> dict:
    request = urllib.request.Request(
        f"{host.rstrip('/')}/auth/api-key",
        method=method,
        headers={
            "OPINION_ADDRESS": wallet,
            "OPINION_SIGNATURE": signature,
            "OPINION_TIMESTAMP": timestamp,
            "Accept": "application/json",
            "User-Agent": "labyda-ops/1.0",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise SystemExit(f"{method} /auth/api-key failed: HTTP {exc.code}: {body[:400]}") from exc


def _call(host: str, action: str, private_key: str, wallet: str) -> dict:
    timestamp = str(int(time.time()))
    signature = _sign(private_key, wallet, action, timestamp)
    method = "POST" if action == "create" else "DELETE"
    payload = _request(host, method, wallet, signature, timestamp)
    if int(payload.get("errno", 0)) != 0:
        raise SystemExit(f"{action} refused by Opinion: {payload.get('errmsg') or payload}")
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--env-file", default=".env.production", help="Compose env file holding OPINION_*")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--write", action="store_true", help="write OPINION_API_KEY into the env file")
    parser.add_argument("--print", action="store_true", help="print the key to stdout (default: masked)")
    parser.add_argument("--rotate", action="store_true", help="revoke the wallet's current key first")
    parser.add_argument("--delete", action="store_true", help="only revoke the wallet's current key")
    args = parser.parse_args(argv)

    env_path = Path(args.env_file)
    file_values = _read_env_file(env_path)
    private_key = os.environ.get("OPINION_PRIVATE_KEY") or file_values.get("OPINION_PRIVATE_KEY", "")
    wallet = os.environ.get("OPINION_ACCOUNT_ADDRESS") or file_values.get("OPINION_ACCOUNT_ADDRESS", "")
    if not private_key or not wallet:
        raise SystemExit("OPINION_PRIVATE_KEY and OPINION_ACCOUNT_ADDRESS must be set (environment or env file)")

    from eth_account import Account

    derived = Account.from_key(private_key).address
    if derived.lower() != wallet.lower():
        raise SystemExit(f"OPINION_ACCOUNT_ADDRESS {wallet} does not match the signing key's address {derived}")

    if args.delete or args.rotate:
        _call(args.host, "delete", private_key, wallet)
        print("revoked the wallet's current API key")
        if args.delete:
            return 0
        time.sleep(12)

    payload = _call(args.host, "create", private_key, wallet)
    api_key = str((payload.get("result") or {}).get("apiKey") or "")
    if not api_key:
        raise SystemExit(f"Opinion returned no apiKey: {payload}")
    masked = api_key[:4] + "…" + api_key[-4:] if len(api_key) > 8 else "…"
    if args.write:
        _write_env_value(env_path, "OPINION_API_KEY", api_key)
        print(f"OPINION_API_KEY={masked} written to {env_path}")
    if args.print:
        print(api_key)
    elif not args.write:
        print(f"api key {masked} (not written; pass --write or --print)")
    print("the key becomes active at the gateway in about fifteen seconds", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
