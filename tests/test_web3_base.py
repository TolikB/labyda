import unittest
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from arbitrage_engine.connectors.web3_base import AsyncNonceManager, BaseWeb3Client, TransactionTimeoutException


class Web3ReceiptTests(unittest.IsolatedAsyncioTestCase):
    async def test_nonce_manager_resets_after_send_failure(self) -> None:
        client = object.__new__(BaseWeb3Client)
        client.account = MagicMock()
        client.build_eip1559_transaction = AsyncMock(side_effect=TimeoutError("rpc timeout"))  # type: ignore[method-assign]
        nonce_manager = MagicMock()
        nonce_manager.release = AsyncMock()
        nonce_manager.reset = AsyncMock()
        client._nonce_manager = nonce_manager

        with self.assertRaises(TimeoutError):
            await client.send_transaction({})

        nonce_manager.reset.assert_awaited_once()

    async def test_nonce_manager_keeps_submitted_nonce_above_stale_pending_rpc(self) -> None:
        manager = AsyncNonceManager()
        w3 = MagicMock()
        w3.eth.get_transaction_count = AsyncMock(return_value=5)

        first = await manager.next_nonce(w3, "0xabc")
        await manager.mark_submitted(first, "0xtx")
        await manager.reset()
        second = await manager.next_nonce(w3, "0xabc")

        self.assertEqual((first, second), (5, 6))

    async def test_confirmation_wait_has_hard_timeout(self) -> None:
        client = object.__new__(BaseWeb3Client)
        client.confirmations = 1
        calls = 0

        async def rpc_call(operation: Any, timeout_seconds: float = 0.4) -> Any:
            nonlocal calls
            del operation, timeout_seconds
            calls += 1
            if calls == 1:
                return {"status": 1, "blockNumber": 100}
            return 100

        client._rpc_call = rpc_call  # type: ignore[method-assign]

        with self.assertRaises(TransactionTimeoutException):
            await client.wait_for_receipt("0xtx", timeout_seconds=0.01, max_blocks_to_wait=4)


if __name__ == "__main__":
    unittest.main()
