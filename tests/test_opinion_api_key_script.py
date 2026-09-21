"""The Opinion API-key script signs the venue's EIP-712 request and writes only the key back."""

from __future__ import annotations

import importlib.util
import json
import os
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from eth_account import Account
from eth_account.messages import encode_typed_data

REPO_ROOT = Path(__file__).resolve().parents[1]


def _load() -> ModuleType:
    path = REPO_ROOT / "scripts" / "opinion_create_api_key.py"
    spec = importlib.util.spec_from_file_location("opinion_create_api_key", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_signature_recovers_to_the_wallet_over_the_documented_typed_data() -> None:
    script = _load()
    account = Account.create()
    signature = script._sign(account.key.hex(), account.address, "create", "1753690000")  # noqa: SLF001

    signable = encode_typed_data(
        domain_data={"name": "Opinion OpenAPI", "version": "1", "chainId": 56},
        message_types=script.TYPES,
        message_data={"walletAddress": account.address, "action": "create", "timestamp": "1753690000"},
    )
    assert Account.recover_message(signable, signature=signature) == account.address


def test_create_writes_only_the_api_key_into_the_env_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    script = _load()
    account = Account.create()
    env_file = tmp_path / ".env.production"
    env_file.write_text(
        f"TELEGRAM_BOT_TOKEN=keep\nOPINION_PRIVATE_KEY={account.key.hex()}\nOPINION_ACCOUNT_ADDRESS={account.address}\n",
        encoding="utf-8",
    )
    os.chmod(env_file, 0o600)
    seen: list[tuple[str, dict[str, str]]] = []

    def fake_request(host: str, method: str, wallet: str, signature: str, timestamp: str) -> dict[str, Any]:
        seen.append((method, {"wallet": wallet, "signature": signature, "timestamp": timestamp}))
        assert host == script.DEFAULT_HOST
        return {"errno": 0, "errmsg": "", "result": {"apiKey": "opk_live_abcdef123456", "walletAddress": wallet}}

    monkeypatch.setattr(script, "_request", fake_request)
    monkeypatch.delenv("OPINION_PRIVATE_KEY", raising=False)
    monkeypatch.delenv("OPINION_ACCOUNT_ADDRESS", raising=False)

    assert script.main(["--env-file", str(env_file), "--write"]) == 0

    assert [method for method, _ in seen] == ["POST"]
    assert seen[0][1]["wallet"] == account.address
    assert seen[0][1]["signature"].startswith("0x")
    body = env_file.read_text(encoding="utf-8")
    assert "OPINION_API_KEY=opk_live_abcdef123456" in body
    assert "TELEGRAM_BOT_TOKEN=keep" in body
    assert body.count("OPINION_PRIVATE_KEY=") == 1
    if sys.platform != "win32":
        assert env_file.stat().st_mode & 0o777 == 0o600


def test_wallet_must_match_the_signing_key_and_refusals_surface(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    script = _load()
    account = Account.create()
    other = Account.create()
    env_file = tmp_path / ".env"
    env_file.write_text(
        f"OPINION_PRIVATE_KEY={account.key.hex()}\nOPINION_ACCOUNT_ADDRESS={other.address}\n", encoding="utf-8"
    )
    monkeypatch.delenv("OPINION_PRIVATE_KEY", raising=False)
    monkeypatch.delenv("OPINION_ACCOUNT_ADDRESS", raising=False)
    with pytest.raises(SystemExit, match="does not match"):
        script.main(["--env-file", str(env_file)])

    env_file.write_text(
        f"OPINION_PRIVATE_KEY={account.key.hex()}\nOPINION_ACCOUNT_ADDRESS={account.address}\n", encoding="utf-8"
    )
    monkeypatch.setattr(
        script, "_request", lambda *args: {"errno": 1001, "errmsg": "api key already exists", "result": None}
    )
    with pytest.raises(SystemExit, match="already exists"):
        script.main(["--env-file", str(env_file)])

    # Rotation revokes first, then creates.
    calls: list[str] = []

    def rotating(host: str, method: str, wallet: str, signature: str, timestamp: str) -> dict[str, Any]:
        calls.append(method)
        return {"errno": 0, "result": {"apiKey": "opk_new", "walletAddress": wallet}}

    monkeypatch.setattr(script, "_request", rotating)
    monkeypatch.setattr(script.time, "sleep", lambda seconds: None)
    assert script.main(["--env-file", str(env_file), "--rotate", "--print"]) == 0
    assert calls == ["DELETE", "POST"]
    assert json.loads(json.dumps(calls)) == calls
