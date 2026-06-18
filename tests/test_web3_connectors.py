import unittest
from unittest.mock import AsyncMock, Mock

from eth_account import Account

from arbitrage_engine.connectors.web3_base import BaseWeb3Client


class Web3ConnectorTests(unittest.IsolatedAsyncioTestCase):
    async def test_sign_transaction_without_broadcast(self) -> None:
        private_key = "0x" + "1" * 64
        client = BaseWeb3Client("https://example.invalid", 8453, private_key)
        account = Account.from_key(private_key)
        fake_w3 = Mock()
        fake_w3.eth.account = Account
        fake_w3.eth.get_transaction_count = AsyncMock(return_value=7)
        fake_w3.eth.get_block = AsyncMock(return_value={"baseFeePerGas": 1_000_000_000})
        fake_w3.eth.estimate_gas = AsyncMock(return_value=21000)
        fake_w3.to_wei = lambda value, unit: int(value * 1_000_000_000)
        client.w3 = fake_w3
        client.account = account

        signed = await client.sign_transaction(
            {
                "to": "0x0000000000000000000000000000000000000001",
                "value": 0,
                "data": "0x",
            }
        )

        self.assertTrue(signed.raw_transaction)
        self.assertEqual(fake_w3.eth.get_transaction_count.await_count, 1)

    async def test_build_transaction_uses_aggressive_priority_fee(self) -> None:
        private_key = "0x" + "3" * 64
        client = BaseWeb3Client("https://example.invalid", 56, private_key)
        account = Account.from_key(private_key)
        fake_w3 = Mock()
        fake_w3.eth.account = Account
        fake_w3.eth.get_transaction_count = AsyncMock(return_value=7)
        fake_w3.eth.get_block = AsyncMock(return_value={"baseFeePerGas": 1_000_000_000})
        fake_w3.eth.estimate_gas = AsyncMock(return_value=21000)
        fake_w3.to_wei = lambda value, unit: int(value * 1_000_000_000)
        client.w3 = fake_w3
        client.account = account

        tx = await client.build_eip1559_transaction(
            {"to": "0x0000000000000000000000000000000000000001", "value": 0, "data": "0x"}
        )

        self.assertEqual(tx["maxPriorityFeePerGas"], 2_000_000_000)
        self.assertEqual(tx["maxFeePerGas"], 3_500_000_000)

    async def test_nonce_manager_increments_locally(self) -> None:
        private_key = "0x" + "2" * 64
        client = BaseWeb3Client("https://example.invalid", 8453, private_key)
        account = Account.from_key(private_key)
        fake_w3 = Mock()
        fake_w3.eth.account = Account
        fake_w3.eth.get_transaction_count = AsyncMock(return_value=7)
        fake_w3.eth.get_block = AsyncMock(return_value={"baseFeePerGas": 1_000_000_000})
        fake_w3.eth.estimate_gas = AsyncMock(return_value=21000)
        fake_w3.to_wei = lambda value, unit: int(value * 1_000_000_000)
        client.w3 = fake_w3
        client.account = account

        tx = {"to": "0x0000000000000000000000000000000000000001", "value": 0, "data": "0x"}
        first = await client.build_eip1559_transaction(tx)
        second = await client.build_eip1559_transaction(tx)

        self.assertEqual(first["nonce"], 7)
        self.assertEqual(second["nonce"], 8)
        self.assertEqual(fake_w3.eth.get_transaction_count.await_count, 1)


if __name__ == "__main__":
    unittest.main()
