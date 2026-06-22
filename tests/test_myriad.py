import asyncio
import time
import unittest
from dataclasses import replace
from unittest.mock import AsyncMock, MagicMock, patch

from arbitrage_engine.config import MyriadMarketsConfig
from arbitrage_engine.connectors.base import OrderBookUnavailableException
from arbitrage_engine.connectors.myriad import (
    MyriadClient,
    _apply_orderbook_changes,
    _normalize_order_amount,
    _order_book_from_payload,
    _orderbook_query_params,
    _to_units,
)
from arbitrage_engine.models import BinarySide, OrderBook, OrderBookLevel
from arbitrage_engine.myriad_discovery import _has_next_page


class MyriadTests(unittest.TestCase):
    def test_discovery_supports_total_pages_pagination(self) -> None:
        self.assertTrue(_has_next_page({"pagination": {"totalPages": 3}}, 1))
        self.assertFalse(_has_next_page({"pagination": {"totalPages": 3}}, 3))

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
                                {
                                    "outcome": 1,
                                    "side": "ask",
                                    "price": "240000000000000000",
                                    "amount": "2000000000000000000",
                                },
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
                *[client.sign_order(market_id=123, outcome_id=1, side=0, contracts=1, price=0.4) for _ in range(10)]
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

    def test_sign_order_quantizes_off_tick_price_down(self) -> None:
        client = MyriadClient(_config())

        signed = asyncio.run(client.sign_order(market_id=123, outcome_id=1, side=0, contracts=10, price=0.405))

        self.assertEqual(signed.order["price"], "400000000000000000")

    def test_orderbook_rest_requests_share_one_client_session(self) -> None:
        client = MyriadClient(_config())
        session = MagicMock()
        session.closed = False

        with patch("arbitrage_engine.connectors.myriad.client_session", return_value=session) as factory:
            first = client._get_rest_session()
            second = client._get_rest_session()

        self.assertIs(first, session)
        self.assertIs(second, session)
        factory.assert_called_once()

    def test_websocket_session_is_reused_without_rest_headers(self) -> None:
        client = MyriadClient(_config())
        session = MagicMock()
        session.closed = False

        with patch("arbitrage_engine.connectors.myriad.client_session", return_value=session) as factory:
            first = client._get_ws_session()
            second = client._get_ws_session()

        self.assertIs(first, session)
        self.assertIs(second, session)
        factory.assert_called_once_with()

    def test_stream_health_tracks_latest_venue_event_and_requires_all_tokens(self) -> None:
        client = MyriadClient(_config())
        client._channel_tokens["orderbook:56:1"] = {"1:YES", "1:NO"}
        client._books["1:YES"] = OrderBook([], [])
        client._book_timestamps["1:YES"] = time.monotonic() - 0.1

        self.assertFalse(client.market_data_ready())

        client._books["1:NO"] = OrderBook([], [])
        client._book_timestamps["1:NO"] = time.monotonic() - 30
        self.assertTrue(client.market_data_ready())
        self.assertLess(client.market_data_age_seconds() or 1.0, 0.5)


class MyriadHttpTests(unittest.IsolatedAsyncioTestCase):
    async def test_rest_timeout_is_classified_as_orderbook_unavailable(self) -> None:
        client = MyriadClient(_config())
        response_context = MagicMock()
        response_context.__aenter__ = AsyncMock(side_effect=TimeoutError("synthetic timeout"))
        response_context.__aexit__ = AsyncMock(return_value=False)
        session = MagicMock()
        session.closed = False
        session.get.return_value = response_context

        with (
            patch("arbitrage_engine.connectors.myriad.client_session", return_value=session),
            self.assertRaisesRegex(OrderBookUnavailableException, "unavailable"),
        ):
            await client.get_orderbook(553, 1)

    async def test_configured_ttl_rejects_stalled_websocket_book(self) -> None:
        client = MyriadClient(replace(_config(), order_book_ttl_ms=10, websocket_stale_after_ms=20))
        client._ensure_ws_task = MagicMock()  # type: ignore[method-assign]
        token_id = "553:NO"
        client._books[token_id] = OrderBook(
            bids=[OrderBookLevel(0.23, 1.0)],
            asks=[OrderBookLevel(0.24, 1.0)],
        )
        client._book_timestamps[token_id] = time.monotonic() - 0.03
        client._book_events[token_id] = asyncio.Event()

        with self.assertRaisesRegex(Exception, "websocket stalled"):
            await client.watch_order_book(token_id)

    async def test_close_releases_rest_and_websocket_sessions(self) -> None:
        client = MyriadClient(_config())
        rest_session = MagicMock()
        rest_session.closed = False
        rest_session.close = AsyncMock()
        ws_session = MagicMock()
        ws_session.closed = False
        ws_session.close = AsyncMock()
        client._rest_session = rest_session
        client._ws_session = ws_session

        await client.close()

        rest_session.close.assert_awaited_once()
        ws_session.close.assert_awaited_once()
        self.assertIsNone(client._rest_session)
        self.assertIsNone(client._ws_session)

    async def test_stale_cached_book_is_rejected_without_rest_fallback(self) -> None:
        client = BootstrapTrackingClient(_config())
        expected = OrderBook(bids=[OrderBookLevel(0.23, 1)], asks=[OrderBookLevel(0.24, 1)])
        client._books["553:NO"] = expected
        client._book_timestamps["553:NO"] = 0.0

        with self.assertRaisesRegex(RuntimeError, "stale"):
            await client.watch_order_book("553:NO")

        self.assertEqual(client.calls, 0)

    async def test_bootstrap_snapshots_are_limited_to_five_concurrent_requests(self) -> None:
        client = BootstrapTrackingClient(_config())

        books = await asyncio.gather(*(client.watch_order_book(f"{market_id}:YES") for market_id in range(100, 112)))

        self.assertEqual(len(books), 12)
        self.assertEqual(client.calls, 12)
        self.assertLessEqual(client.max_active, 5)

    async def test_concurrent_watchers_share_one_bootstrap_request(self) -> None:
        client = BootstrapTrackingClient(_config())

        books = await asyncio.gather(*(client.watch_order_book("553:NO") for _ in range(10)))

        self.assertEqual(client.calls, 1)
        self.assertTrue(all(book is books[0] for book in books))

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
        session.closed = False
        session.post.return_value = response_context
        session.delete.return_value = response_context

        with patch("arbitrage_engine.connectors.myriad.client_session", return_value=session):
            order_id = await client.place_order(signed)
            await client.cancel_order(order_id)

        place_payload = session.post.call_args.kwargs["json"]
        self.assertEqual(place_payload["time_in_force"], "FAK")
        cancel_payload = session.delete.call_args.kwargs["json"]
        self.assertEqual(cancel_payload["order"], signed.order)
        self.assertEqual(cancel_payload["signature"], signed.signature)


class BootstrapTrackingClient(MyriadClient):
    def __init__(self, config: MyriadMarketsConfig) -> None:
        super().__init__(config)
        self.calls = 0
        self.active = 0
        self.max_active = 0

    def _ensure_ws_task(self) -> None:
        return

    async def get_orderbook(self, market_id: int, outcome_id: int) -> dict[str, object]:
        self.calls += 1
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            await asyncio.sleep(0.01)
            return {
                "marketId": market_id,
                "outcomeId": outcome_id,
                "bids": [["230000000000000000", "1000000000000000000"]],
                "asks": [["240000000000000000", "1000000000000000000"]],
            }
        finally:
            self.active -= 1


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
