import asyncio
from concurrent.futures import ThreadPoolExecutor
import sys
import threading
import time
import types
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from arbitrage_engine.config import PolymarketConfig
from arbitrage_engine.connectors.polymarket import PolymarketClobClient
from arbitrage_engine.connectors.polymarket import _apply_price_changes, _clob_ws_url, _order_book_from_payload
from arbitrage_engine.models import OrderBook, OrderBookLevel


class PolymarketWsTests(unittest.TestCase):
    def test_sdk_client_initialization_is_thread_safe(self) -> None:
        calls = 0
        derives = 0
        counter_lock = threading.Lock()

        class FakeClobClient:
            def __init__(self, *args, **kwargs) -> None:
                nonlocal calls
                time.sleep(0.005)
                with counter_lock:
                    calls += 1

            def create_or_derive_api_key(self) -> str:
                nonlocal derives
                with counter_lock:
                    derives += 1
                return "creds"

        module = types.SimpleNamespace(ClobClient=FakeClobClient)
        client = PolymarketClobClient(
            PolymarketConfig("key", "https://clob.polymarket.com", 137, 0, None)
        )

        with patch.dict(sys.modules, {"py_clob_client_v2": module}):
            with ThreadPoolExecutor(max_workers=8) as executor:
                clients = list(executor.map(lambda _: client._get_sdk_client(), range(16)))

        self.assertTrue(all(item is clients[0] for item in clients))
        self.assertEqual(calls, 2)
        self.assertEqual(derives, 1)

    def test_clob_ws_url_is_derived_from_api_base_url(self) -> None:
        self.assertEqual(
            _clob_ws_url("https://clob.polymarket.com"),
            "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        )

    def test_order_book_snapshot_is_sorted(self) -> None:
        book = _order_book_from_payload(
            {
                "bids": [{"price": "0.41", "size": "10"}, {"price": "0.43", "size": "5"}],
                "asks": [{"price": "0.48", "size": "10"}, {"price": "0.46", "size": "5"}],
            }
        )

        assert book is not None
        self.assertEqual(book.best_bid.price, 0.43)
        self.assertEqual(book.best_ask.price, 0.46)

    def test_price_changes_update_target_side(self) -> None:
        book = OrderBook(
            bids=[OrderBookLevel(0.43, 5)],
            asks=[OrderBookLevel(0.46, 5)],
        )

        updated = _apply_price_changes(
            book,
            [
                {"side": "BUY", "price": "0.44", "size": "10"},
                {"side": "SELL", "price": "0.46", "size": "0"},
            ],
        )

        self.assertEqual(updated.best_bid.price, 0.44)
        self.assertEqual(updated.asks, [])

    def test_price_changes_ignore_other_assets(self) -> None:
        book = OrderBook(
            bids=[OrderBookLevel(0.43, 5)],
            asks=[OrderBookLevel(0.46, 5)],
        )

        updated = _apply_price_changes(
            book,
            [{"asset_id": "other", "side": "BUY", "price": "0.99", "size": "10"}],
            token_id="target",
        )

        self.assertEqual(updated.best_bid.price, 0.43)

    def test_rest_session_is_reused(self) -> None:
        client = PolymarketClobClient(
            PolymarketConfig(None, "https://clob.polymarket.com", 137, 0, None)
        )
        session = MagicMock()
        session.closed = False

        with patch("arbitrage_engine.connectors.polymarket.client_session", return_value=session) as factory:
            self.assertIs(client._get_rest_session(), session)
            self.assertIs(client._get_rest_session(), session)

        factory.assert_called_once()

    def test_stream_health_uses_orderbook_updates_not_socket_pongs(self) -> None:
        client = PolymarketClobClient(
            PolymarketConfig(None, "https://clob.polymarket.com", 137, 0, None)
        )
        client._desired_tokens.add("token")
        client._books["token"] = OrderBook([], [], timestamp=time.time() - 30)
        client._book_timestamps["token"] = time.monotonic() - 30

        age = client.market_data_age_seconds()

        self.assertIsNotNone(age)
        self.assertGreater(age or 0.0, 29.0)


class PolymarketLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_http_orderbook_requests_are_limited_to_twenty(self) -> None:
        client = PolymarketClobClient(
            PolymarketConfig(None, "https://clob.polymarket.com", 137, 0, None)
        )
        active = 0
        max_active = 0

        class Response:
            async def __aenter__(self):
                nonlocal active, max_active
                active += 1
                max_active = max(max_active, active)
                return self

            async def __aexit__(self, exc_type, exc, traceback):
                nonlocal active
                active -= 1

            def raise_for_status(self) -> None:
                return

            async def json(self):
                await asyncio.sleep(0.01)
                return {
                    "bids": [{"price": "0.40", "size": "10"}],
                    "asks": [{"price": "0.41", "size": "10"}],
                }

        session = MagicMock()
        session.closed = False
        session.get.side_effect = lambda *args, **kwargs: Response()
        client._rest_session = session

        books = await asyncio.gather(*(client._fetch_order_book_http(str(index)) for index in range(50)))

        self.assertEqual(len(books), 50)
        self.assertLessEqual(max_active, 20)

    async def test_close_releases_sessions_and_ws_task(self) -> None:
        client = PolymarketClobClient(
            PolymarketConfig(None, "https://clob.polymarket.com", 137, 0, None)
        )
        session = MagicMock()
        session.closed = False
        session.close = AsyncMock()
        client._rest_session = session
        ws_session = MagicMock()
        ws_session.closed = False
        ws_session.close = AsyncMock()
        client._ws_session = ws_session
        task = asyncio.create_task(asyncio.sleep(60))
        client._ws_task = task

        await client.close()

        session.close.assert_awaited_once()
        ws_session.close.assert_awaited_once()
        self.assertTrue(task.cancelled())
        self.assertIsNone(client._ws_task)

    async def test_all_tokens_share_one_ws_task(self) -> None:
        client = PolymarketClobClient(
            PolymarketConfig(None, "https://clob.polymarket.com", 137, 0, None)
        )
        blocker = asyncio.Event()

        async def run_ws() -> None:
            await blocker.wait()

        with patch.object(client, "_run_order_book_ws", side_effect=run_ws):
            client._register_token("yes-token")
            task = client._ws_task
            client._register_token("no-token")
            self.assertIs(client._ws_task, task)
            self.assertEqual(client._desired_tokens, {"yes-token", "no-token"})

        await client.close()

    async def test_payload_changes_are_isolated_by_asset_id(self) -> None:
        client = PolymarketClobClient(
            PolymarketConfig(None, "https://clob.polymarket.com", 137, 0, None)
        )
        client._desired_tokens.update({"a", "b"})
        client._books = {
            "a": OrderBook([OrderBookLevel(0.4, 1)], [OrderBookLevel(0.5, 1)]),
            "b": OrderBook([OrderBookLevel(0.6, 1)], [OrderBookLevel(0.7, 1)]),
        }

        client._handle_ws_payload(
            {
                "changes": [
                    {"asset_id": "a", "side": "BUY", "price": "0.45", "size": "2"},
                    {"asset_id": "b", "side": "SELL", "price": "0.65", "size": "3"},
                ]
            }
        )

        self.assertEqual(client._books["a"].best_bid.price, 0.45)
        self.assertEqual(client._books["a"].best_ask.price, 0.5)
        self.assertEqual(client._books["b"].best_bid.price, 0.6)
        self.assertEqual(client._books["b"].best_ask.price, 0.65)


if __name__ == "__main__":
    unittest.main()
