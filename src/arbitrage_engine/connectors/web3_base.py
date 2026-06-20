from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any


class TransactionTimeoutException(TimeoutError):
    """Raised when a transaction or its required confirmations exceed risk limits."""


class AsyncNonceManager:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._next_nonce: int | None = None
        self._reserved: set[int] = set()
        self._submitted: dict[int, str] = {}

    async def next_nonce(self, w3: Any, address: str, rpc_call: Any | None = None) -> int:
        async with self._lock:
            if self._next_nonce is None:
                if rpc_call is not None:
                    self._next_nonce = await rpc_call(
                        lambda current_w3: current_w3.eth.get_transaction_count(address, "pending")
                    )
                else:
                    self._next_nonce = await _with_backoff(lambda: w3.eth.get_transaction_count(address, "pending"))
                local_floor = max((*self._reserved, *self._submitted.keys()), default=-1) + 1
                self._next_nonce = max(self._next_nonce, local_floor)
            nonce = self._next_nonce
            self._next_nonce += 1
            self._reserved.add(nonce)
            return nonce

    async def mark_submitted(self, nonce: int, tx_hash: str) -> None:
        async with self._lock:
            self._reserved.discard(nonce)
            self._submitted[nonce] = tx_hash

    async def release(self, nonce: int) -> None:
        async with self._lock:
            self._reserved.discard(nonce)

    async def mark_finalized(self, tx_hash: str) -> None:
        async with self._lock:
            self._submitted = {nonce: value for nonce, value in self._submitted.items() if value != tx_hash}

    async def reset(self) -> None:
        async with self._lock:
            self._next_nonce = None


@dataclass
class BaseWeb3Client:
    rpc_url: str | list[str]
    chain_id: int
    private_key: str | None = None
    max_priority_fee_gwei: float | None = None
    confirmations: int = 1

    def __post_init__(self) -> None:
        if self.max_priority_fee_gwei is None:
            self.max_priority_fee_gwei = _default_priority_fee_gwei(self.chain_id)
        self.rpc_urls = [self.rpc_url] if isinstance(self.rpc_url, str) else list(self.rpc_url)
        if not self.rpc_urls:
            raise RuntimeError("at least one rpc_url is required")
        self._rpc_index = 0
        self.w3 = _get_async_web3(self.rpc_urls[self._rpc_index])
        self.account = self.w3.eth.account.from_key(self.private_key) if self.private_key else None
        self._nonce_manager = AsyncNonceManager()

    async def build_eip1559_transaction(self, tx: dict[str, Any]) -> dict[str, Any]:
        if self.account is None:
            raise RuntimeError("private_key is required for transaction signing")
        latest_block = await self._rpc_call(lambda w3: w3.eth.get_block("latest"))
        base_fee = int(latest_block.get("baseFeePerGas", 0))
        priority_fee = self.w3.to_wei(self.max_priority_fee_gwei, "gwei")
        enriched = dict(tx)
        enriched.setdefault("from", self.account.address)
        enriched.setdefault("chainId", self.chain_id)
        if "nonce" not in enriched:
            enriched["nonce"] = await self._nonce_manager.next_nonce(
                self.w3,
                self.account.address,
                self._rpc_call,
            )
        enriched.setdefault("maxPriorityFeePerGas", priority_fee)
        enriched.setdefault("maxFeePerGas", int(base_fee * 1.5) + priority_fee)
        if "gas" not in enriched:
            enriched["gas"] = await self._rpc_call(lambda w3: w3.eth.estimate_gas(enriched))
        return enriched

    async def sign_transaction(self, tx: dict[str, Any]) -> Any:
        if self.account is None:
            raise RuntimeError("private_key is required for transaction signing")
        enriched = await self.build_eip1559_transaction(tx)
        return self.account.sign_transaction(enriched)

    async def send_transaction(self, tx: dict[str, Any]) -> str:
        nonce: int | None = None
        broadcast_attempted = False
        try:
            if self.account is None:
                raise RuntimeError("private_key is required for transaction signing")
            enriched = await self.build_eip1559_transaction(tx)
            nonce = int(enriched["nonce"])
            signed = self.account.sign_transaction(enriched)
            broadcast_attempted = True
            tx_hash = await self._rpc_call(lambda w3: w3.eth.send_raw_transaction(signed.raw_transaction))
            normalized_hash = str(self.w3.to_hex(tx_hash))
            await self._nonce_manager.mark_submitted(nonce, normalized_hash)
            return normalized_hash
        except Exception:
            # Keep "pending" as the authoritative source; "latest" can reuse a
            # nonce that is already in the mempool. Reset forces a fresh pending
            # read after transport errors or transaction timeouts.
            if nonce is not None and broadcast_attempted:
                await self._nonce_manager.mark_submitted(nonce, f"unknown:{nonce}")
            elif nonce is not None:
                await self._nonce_manager.release(nonce)
            await self._nonce_manager.reset()
            raise

    async def wait_for_receipt(self, tx_hash: str, timeout_seconds: float, max_blocks_to_wait: int = 64) -> bool:
        if timeout_seconds <= 0 or max_blocks_to_wait <= 0:
            raise ValueError("timeout and max_blocks_to_wait must be positive")
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_seconds
        try:
            receipt = await asyncio.wait_for(
                self._rpc_call(
                    lambda w3: w3.eth.wait_for_transaction_receipt(tx_hash, timeout=timeout_seconds),
                    timeout_seconds=timeout_seconds,
                ),
                timeout=timeout_seconds,
            )
        except Exception as exc:
            if not _is_timeout_error(exc):
                raise
            raise TransactionTimeoutException(f"Timed out waiting for transaction receipt {tx_hash}") from exc
        if int(receipt.get("status", 0)) != 1:
            await self._nonce_manager.mark_finalized(tx_hash)
            return False
        if self.confirmations <= 0:
            await self._nonce_manager.mark_finalized(tx_hash)
            return True
        receipt_block = int(receipt["blockNumber"])
        if self.confirmations > max_blocks_to_wait:
            raise TransactionTimeoutException(
                f"Requested {self.confirmations} confirmations exceeds max_blocks_to_wait={max_blocks_to_wait}"
            )
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise TransactionTimeoutException(f"Timed out waiting for confirmations for {tx_hash}")
            try:
                current_block = await self._rpc_call(
                    lambda w3: w3.eth.block_number,
                    timeout_seconds=min(0.4, remaining),
                )
            except TimeoutError as exc:
                if loop.time() >= deadline:
                    raise TransactionTimeoutException(f"Timed out waiting for confirmations for {tx_hash}") from exc
                continue
            if current_block - receipt_block >= self.confirmations:
                await self._nonce_manager.mark_finalized(tx_hash)
                return True
            if current_block - receipt_block >= max_blocks_to_wait:
                raise TransactionTimeoutException(
                    f"Confirmation block limit exceeded for {tx_hash}: {max_blocks_to_wait}"
                )
            await asyncio.sleep(min(0.2, max(0.0, deadline - loop.time())))

    def contract(self, address: str, abi: list[dict[str, Any]]) -> Any:
        return self.w3.eth.contract(address=self.w3.to_checksum_address(address), abi=abi)

    async def _rpc_call(self, operation: object, timeout_seconds: float = 0.4) -> Any:
        last_error: Exception | None = None
        for _ in range(max(1, len(self.rpc_urls))):
            try:
                result = operation(self.w3)  # type: ignore[operator]
                if asyncio.iscoroutine(result):
                    return await asyncio.wait_for(result, timeout=timeout_seconds)
                return result
            except Exception as exc:
                last_error = exc
                self._rotate_rpc()
        assert last_error is not None
        raise last_error

    def _rotate_rpc(self) -> None:
        if len(self.rpc_urls) <= 1:
            return
        self._rpc_index = (self._rpc_index + 1) % len(self.rpc_urls)
        self.w3 = _get_async_web3(self.rpc_urls[self._rpc_index])


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


def _is_timeout_error(exc: BaseException) -> bool:
    return isinstance(exc, (asyncio.TimeoutError, TimeoutError)) or type(exc).__name__ in {
        "TimeExhausted",
        "ReadTimeout",
    }
