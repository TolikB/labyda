from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any


class AsyncNonceManager:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._next_nonce: int | None = None

    async def next_nonce(self, w3: Any, address: str) -> int:
        async with self._lock:
            if self._next_nonce is None:
                self._next_nonce = await _with_backoff(lambda: w3.eth.get_transaction_count(address, "pending"))
            nonce = self._next_nonce
            self._next_nonce += 1
            return nonce

    async def reset(self) -> None:
        async with self._lock:
            self._next_nonce = None


@dataclass
class BaseWeb3Client:
    rpc_url: str
    chain_id: int
    private_key: str | None = None
    max_priority_fee_gwei: float | None = None
    confirmations: int = 1

    def __post_init__(self) -> None:
        if self.max_priority_fee_gwei is None:
            self.max_priority_fee_gwei = _default_priority_fee_gwei(self.chain_id)
        self.w3 = _get_async_web3(self.rpc_url)
        self.account = self.w3.eth.account.from_key(self.private_key) if self.private_key else None
        self._nonce_manager = AsyncNonceManager()

    async def build_eip1559_transaction(self, tx: dict[str, Any]) -> dict[str, Any]:
        if self.account is None:
            raise RuntimeError("private_key is required for transaction signing")
        latest_block = await _with_backoff(lambda: self.w3.eth.get_block("latest"))
        base_fee = int(latest_block.get("baseFeePerGas", 0))
        priority_fee = self.w3.to_wei(self.max_priority_fee_gwei, "gwei")
        enriched = dict(tx)
        enriched.setdefault("from", self.account.address)
        enriched.setdefault("chainId", self.chain_id)
        enriched.setdefault("nonce", await self._nonce_manager.next_nonce(self.w3, self.account.address))
        enriched.setdefault("maxPriorityFeePerGas", priority_fee)
        enriched.setdefault("maxFeePerGas", int(base_fee * 1.5) + priority_fee)
        if "gas" not in enriched:
            enriched["gas"] = await _with_backoff(lambda: self.w3.eth.estimate_gas(enriched))
        return enriched

    async def sign_transaction(self, tx: dict[str, Any]) -> Any:
        if self.account is None:
            raise RuntimeError("private_key is required for transaction signing")
        enriched = await self.build_eip1559_transaction(tx)
        return self.account.sign_transaction(enriched)

    async def send_transaction(self, tx: dict[str, Any]) -> str:
        signed = await self.sign_transaction(tx)
        tx_hash = await _with_backoff(lambda: self.w3.eth.send_raw_transaction(signed.raw_transaction))
        return str(self.w3.to_hex(tx_hash))

    async def wait_for_receipt(self, tx_hash: str, timeout: float) -> bool:
        receipt = await _with_backoff(lambda: self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=timeout))
        if int(receipt.get("status", 0)) != 1:
            return False
        if self.confirmations <= 0:
            return True
        receipt_block = int(receipt["blockNumber"])
        while True:
            current_block = await _with_backoff(lambda: self.w3.eth.block_number)
            if current_block - receipt_block >= self.confirmations:
                return True
            await asyncio.sleep(0.2)

    def contract(self, address: str, abi: list[dict[str, Any]]) -> Any:
        return self.w3.eth.contract(address=self.w3.to_checksum_address(address), abi=abi)


_WEB3_CACHE: dict[str, Any] = {}


async def _with_backoff(operation: object) -> Any:
    last_error: Exception | None = None
    for delay in (0.0, 0.1, 0.2, 0.5):
        if delay:
            await asyncio.sleep(delay)
        try:
            result = operation()  # type: ignore[operator]
            if asyncio.iscoroutine(result):
                return await result
            return result
        except Exception as exc:
            last_error = exc
            if "429" not in str(exc) and "Too Many Requests" not in str(exc):
                raise
    assert last_error is not None
    raise last_error


def _get_async_web3(rpc_url: str) -> Any:
    if rpc_url in _WEB3_CACHE:
        return _WEB3_CACHE[rpc_url]
    try:
        from web3 import AsyncWeb3
        from web3.providers.rpc import AsyncHTTPProvider
    except ImportError as exc:
        raise RuntimeError("web3.py is required for async Web3 connectivity") from exc

    _WEB3_CACHE[rpc_url] = AsyncWeb3(AsyncHTTPProvider(rpc_url))
    return _WEB3_CACHE[rpc_url]


def _default_priority_fee_gwei(chain_id: int) -> float:
    if chain_id in (56, 97):
        return 2.0
    if chain_id in (137, 80002):
        return 20.0
    return 3.0
