import unittest
import asyncio

from arbitrage_engine.config import MyriadMarketsConfig
from arbitrage_engine.connectors.myriad import MyriadClient, _normalize_order_amount, _order_book_from_payload, _to_units
from arbitrage_engine.models import BinarySide


class MyriadTests(unittest.TestCase):
    def test_to_units_uses_expected_decimals(self) -> None:
        self.assertEqual(_to_units(1.0, 6), 1_000_000)
        self.assertEqual(_to_units(0.4, 18), 400_000_000_000_000_000)

    def test_normalize_order_amount_supports_wei_and_human_units(self) -> None:
        self.assertEqual(_normalize_order_amount(40.0, 100.0), 40.0)
        self.assertEqual(_normalize_order_amount(40 * 10**18, 100.0), 40.0)

    def test_sign_order_builds_eip712_payload(self) -> None:
        client = MyriadClient(_config())

        signed = asyncio.run(client.sign_order(market_id=123, outcome_id=1, side=0, contracts=10, price=0.4))

        self.assertEqual(signed.order["marketId"], "123")
        self.assertEqual(signed.order["outcomeId"], 1)
        self.assertEqual(signed.order["amount"], str(10 * 10**18))
        self.assertEqual(signed.order["price"], "400000000000000000")
        self.assertTrue(signed.signature.startswith("0x") or len(signed.signature) >= 128)

    def test_sign_order_uses_unique_nonce_under_concurrency(self) -> None:
        async def run() -> list[str]:
            client = MyriadClient(_config())
            signed = await asyncio.gather(
                *[
                    client.sign_order(market_id=123, outcome_id=1, side=0, contracts=1, price=0.4)
                    for _ in range(10)
                ]
            )
            return [str(item.order["nonce"]) for item in signed]

        nonces = asyncio.run(run())

        self.assertEqual(len(set(nonces)), 10)

    def test_order_book_uses_requested_outcome_book(self) -> None:
        payload = {
            "orderbook": {
                "YES": {"bids": [{"price": "0.40", "size": "10"}], "asks": [{"price": "0.41", "size": "12"}]},
                "NO": {"bids": [{"price": "0.58", "size": "20"}], "asks": [{"price": "0.59", "size": "22"}]},
            }
        }

        book = _order_book_from_payload(payload, BinarySide.NO)

        self.assertEqual(book.best_bid.price, 0.58)
        self.assertEqual(book.best_ask.price, 0.59)


def _config() -> MyriadMarketsConfig:
    return MyriadMarketsConfig(
        api_url="https://api-v2.myriadprotocol.com",
        api_key="key",
        private_key="0x" + "1" * 64,
        rpc_url="https://bsc-dataseed.binance.org",
        rpc_urls=["https://bsc-dataseed.binance.org"],
        chain_id=56,
        exchange_address="0xa0b6f8ef8EdB64f395018D1933f2273Ce9f0f16A",
        conditional_tokens_address="0x6413734f92248D4B29ae35883290BD93212654Dc",
        collateral_tokens={
            "USD1": "0x8d0D000Ee44948FC98c9B98A4FA4921476f08B0d",
            "USDT": "0x55d398326f99059fF775485246999027B3197955",
        },
        collateral_symbol="USDT",
        trading_fee_pct=0.0,
        max_slippage_pct=0.015,
        enabled=True,
    )


if __name__ == "__main__":
    unittest.main()
