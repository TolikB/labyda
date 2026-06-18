import unittest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from arbitrage_engine.config import MyriadMarketsConfig
from arbitrage_engine.connectors.myriad import (
    MyriadClient,
    _apply_orderbook_changes,
    _normalize_order_amount,
    _order_book_from_payload,
    _orderbook_query_params,
    _to_units,
)
from arbitrage_engine.models import BinarySide, OrderBook, OrderBookLevel


class MyriadTests(unittest.TestCase):
    def test_to_units_uses_expected_decimals(self) -> None:
        self.assertEqual(_to_units(1.0, 6), 1_000_000)
        self.assertEqual(_to_units(0.4, 18), 400_000_000_000_000_000)

    def test_normalize_order_amount_supports_wei_and_human_units(self) -> None:
        self.assertEqual(_normalize_order_amount(40.0, 100.0), 40.0)
        self.assertEqual(_normalize_order_amount(40 * 10**18, 100.0), 40.0)

    def test_orderbook_query_includes_network_outcome_and_clob_model(self) -> None:
        self.assertEqual(
            _orderbook_query_params(56, 1),
            {"network_id": 56, "outcome": 1, "trading_model": "ob"},
        )

    def test_websocket_delta_updates_local_orderbook(self) -> None:
        book = OrderBook(
            bids=[OrderBookLevel(0.40, 10)],
            asks=[OrderBookLevel(0.42, 10)],
        )

        updated = _apply_orderbook_changes(
            book,
            [
                {"outcome": 0, "side": "BUY", "price": "0.41", "size": "5"},
                {"outcome": 0, "side": "SELL", "price": "0.42", "size": "0"},
            ],
            BinarySide.YES,
        )

        self.assertEqual(updated.best_bid.price, 0.41)
        self.assertEqual(updated.asks, [])

    def test_websocket_payload_cannot_cross_market_cache_boundary(self) -> None:
        client = MyriadClient(_config())
        token_id = "553:NO"
        channel = "orderbook:56:553"
        original = OrderBook(bids=[OrderBookLevel(0.23, 10)], asks=[OrderBookLevel(0.24, 10)])
        client._channel_tokens[channel] = {token_id}
        client._books[token_id] = original

        client._handle_ws_payload(
            {
                "push": {
                    "channel": channel,
                    "pub": {
                        "data": {
                            "networkId": 56,
                            "marketId": 999,
                            "changes": [
                                {"outcome": 1, "side": "ask", "price": "0.99", "amount": "1000000000000000000"}
                            ],
                        }
                    },
                }
            }
        )

        self.assertIs(client._books[token_id], original)

    def test_websocket_delta_updates_only_matching_market_and_outcome(self) -> None:
        client = MyriadClient(_config())
        token_id = "553:NO"
        channel = "orderbook:56:553"
        client._channel_tokens[channel] = {token_id}
        client._books[token_id] = OrderBook(bids=[OrderBookLevel(0.23, 10)], asks=[OrderBookLevel(0.24, 10)])

        client._handle_ws_payload(
            {
                "push": {
                    "channel": channel,
                    "pub": {
                        "data": {
                            "networkId": 56,
                            "marketId": 553,
                            "changes": [
                                {"outcome": 0, "side": "ask", "price": "0.01", "amount": "1000000000000000000"},
                                {"outcome": 1, "side": "ask", "price": "240000000000000000", "amount": "2000000000000000000"},
                            ],
                        }
                    },
                }
            }
        )

        self.assertEqual(client._books[token_id].best_ask, OrderBookLevel(0.24, 2.0))

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

    def test_order_book_normalizes_api_integer_scales(self) -> None:
        book = _order_book_from_payload(
            {
                "bids": [["500000000000000000", "3000000000000000000"]],
                "asks": [["510000000000000000", "2000000000000000000"]],
            }
        )

        self.assertEqual(book.best_bid, OrderBookLevel(0.5, 3.0))
        self.assertEqual(book.best_ask, OrderBookLevel(0.51, 2.0))

    def test_sign_order_rejects_off_tick_price(self) -> None:
        client = MyriadClient(_config())

        with self.assertRaisesRegex(ValueError, "0.01 tick"):
            asyncio.run(client.sign_order(market_id=123, outcome_id=1, side=0, contracts=10, price=0.405))


class MyriadHttpTests(unittest.IsolatedAsyncioTestCase):
    async def test_place_uses_fak_and_cancel_sends_original_signature(self) -> None:
        client = MyriadClient(_config())
        signed = await client.sign_order(market_id=123, outcome_id=0, side=0, contracts=1, price=0.4)
        response = MagicMock()
        response.json = AsyncMock(return_value={"orderHash": "0xorder", "status": "open"})
        response.raise_for_status.return_value = None
        response_context = MagicMock()
        response_context.__aenter__.return_value = response
        response_context.__aexit__.return_value = False
        session = MagicMock()
        session.post.return_value = response_context
        session.delete.return_value = response_context
        session_context = MagicMock()
        session_context.__aenter__.return_value = session
        session_context.__aexit__.return_value = False

        with patch("aiohttp.ClientSession", return_value=session_context):
            order_id = await client.place_order(signed)
            await client.cancel_order(order_id)

        place_payload = session.post.call_args.kwargs["json"]
        self.assertEqual(place_payload["time_in_force"], "FAK")
        cancel_payload = session.delete.call_args.kwargs["json"]
        self.assertEqual(cancel_payload["order"], signed.order)
        self.assertEqual(cancel_payload["signature"], signed.signature)


def _config() -> MyriadMarketsConfig:
    return MyriadMarketsConfig(
        api_url="https://api-v2.myriadprotocol.com",
        ws_url="wss://ws.myriadprotocol.com/ws",
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
