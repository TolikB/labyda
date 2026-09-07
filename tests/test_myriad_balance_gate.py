"""The Myriad balance gate must compare on-chain against connector, not skip.

`venue_balance_gate` guards every cross-check behind `direct_balance is not
None`. The Myriad branch of the production audit used to read `get_balances()`,
which no connector overrides — the base returns `{"cash": ...}`, so a lookup by
collateral symbol always yielded `None` and the mismatch checks never ran. The
gate then passed on the venue's self-reported number alone, which is exactly the
failure it exists to catch.
"""

from __future__ import annotations

import unittest
from typing import Any

from arbitrage_engine.production_audit import venue_balance_gate

MINIMUM = 50.0


def runtime_audit(effective: float) -> dict[str, Any]:
    return {
        "balance_state": {
            "venues": {
                "Myriad": {
                    "effective_balance": effective,
                    "connector_balance": effective,
                    "capital_reservations": 0.0,
                    "optimistic_debits": 0.0,
                }
            }
        }
    }


class MyriadBalanceGateTests(unittest.TestCase):
    def _gate(self, *, connector: float, direct: float | None) -> dict[str, Any]:
        return venue_balance_gate(
            venue="Myriad",
            minimum_balance_usd=MINIMUM,
            connector_balance=connector,
            direct_balance=direct,
            runtime_audit=runtime_audit(connector),
        )

    def test_matching_balances_pass(self) -> None:
        gate = self._gate(connector=125.0, direct=125.0)

        self.assertTrue(gate["passed"], msg=gate.get("blocking_reasons"))

    def test_on_chain_disagreeing_with_the_connector_blocks(self) -> None:
        # The venue claims funds the chain does not show. This is the case the
        # cross-check exists for.
        gate = self._gate(connector=125.0, direct=0.0)

        self.assertFalse(gate["passed"])
        self.assertIn("direct_vs_connector_balance_mismatch", gate["blocking_reasons"])

    def test_on_chain_below_minimum_blocks_even_if_connector_is_healthy(self) -> None:
        gate = self._gate(connector=125.0, direct=10.0)

        self.assertFalse(gate["passed"])
        self.assertIn("direct_balance_below_minimum", gate["blocking_reasons"])

    def test_absent_direct_balance_silently_skips_every_cross_check(self) -> None:
        # Documents the hazard the Myriad branch used to sit in: with no direct
        # reading, a wrong connector balance passes unchallenged.
        gate = self._gate(connector=125.0, direct=None)

        self.assertTrue(gate["passed"])
        self.assertNotIn("direct_vs_connector_balance_mismatch", gate["blocking_reasons"])


class MyriadBalanceDetailsTests(unittest.IsolatedAsyncioTestCase):
    def _client(self, raw_balance: int, decimals: int) -> Any:
        from arbitrage_engine.config import MyriadMarketsConfig
        from arbitrage_engine.connectors.myriad import MyriadClient

        client = MyriadClient(
            MyriadMarketsConfig(
                api_url="https://api-v2.myriadprotocol.com",
                ws_url="wss://ws.myriadprotocol.com/ws",
                api_key=None,
                private_key="11" * 32,
                rpc_url="https://bsc-dataseed.binance.org",
                rpc_urls=["https://bsc-dataseed.binance.org"],
                chain_id=56,
                exchange_address="0xExchange",
                conditional_tokens_address="0xCTF",
                collateral_tokens={"USDT": "0x55d398326f99059fF775485246999027B3197955"},
                collateral_symbol="USDT",
                trading_fee_pct=0.0,
                max_slippage_pct=0.015,
                enabled=True,
            )
        )

        class FakeCall:
            def __init__(self, value: object) -> None:
                self._value = value

            async def call(self) -> object:
                return self._value

        class FakeFunctions:
            def balanceOf(self, address: str) -> FakeCall:  # noqa: N802 - ERC-20 ABI name
                assert address == "0xSIGNER"
                return FakeCall(raw_balance)

            def decimals(self) -> FakeCall:
                return FakeCall(decimals)

        class FakeAccount:
            address = "0xsigner"

        class FakeWeb3:
            account = FakeAccount()

            class w3:  # noqa: N801 - mirrors the web3 attribute name
                @staticmethod
                def to_checksum_address(value: str) -> str:
                    return value.upper().replace("0X", "0x")

            def contract(self, address: str, abi: object) -> object:
                del address, abi
                return type("Token", (), {"functions": FakeFunctions()})()

        client._web3_client = FakeWeb3()  # type: ignore[assignment]  # noqa: SLF001
        return client

    async def test_details_expose_the_components_the_audit_compares(self) -> None:
        client = self._client(raw_balance=125 * 10**18, decimals=18)

        details = await client.get_cash_balance_details()

        self.assertEqual(details["balance"], 125.0)
        self.assertEqual(details["balance_raw"], str(125 * 10**18))
        self.assertEqual(details["decimals"], 18)
        self.assertEqual(details["collateral_symbol"], "USDT")
        self.assertEqual(details["wallet_address"], "0xSIGNER")

    async def test_cash_balance_agrees_with_the_details_it_derives_from(self) -> None:
        client = self._client(raw_balance=7 * 10**6, decimals=6)

        self.assertEqual(await client.get_cash_balance(), 7.0)

    async def test_missing_collateral_configuration_fails_closed(self) -> None:
        from arbitrage_engine.config import MyriadMarketsConfig
        from arbitrage_engine.connectors.myriad import MyriadClient

        client = MyriadClient(
            MyriadMarketsConfig(
                api_url="u",
                ws_url="w",
                api_key=None,
                private_key="11" * 32,
                rpc_url="r",
                rpc_urls=["r"],
                chain_id=56,
                exchange_address="0x",
                conditional_tokens_address="0x",
                collateral_tokens={},
                collateral_symbol="USDT",
                trading_fee_pct=0.0,
                max_slippage_pct=0.015,
                enabled=True,
            )
        )

        with self.assertRaises(RuntimeError):
            await client.get_cash_balance_details()


if __name__ == "__main__":
    unittest.main()
