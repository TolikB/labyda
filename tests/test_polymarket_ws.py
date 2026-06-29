import asyncio
import sys
import threading
import time
import types
import unittest
from concurrent.futures import ThreadPoolExecutor
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

from arbitrage_engine.config import PolymarketConfig
from arbitrage_engine.connectors.base import WebSocketReconnectBackoff
from arbitrage_engine.connectors.polymarket import (
    PolymarketClobClient,
    _apply_price_changes,
    _clob_ws_url,
    _normalize_collateral_balance,
    _order_book_from_payload,
    _subscription_payload,
)
from arbitrage_engine.models import MarketDataStatus, OrderBook, OrderBookLevel


class PolymarketWsTests(unittest.TestCase):
    def test_sdk_client_initialization_is_thread_safe(self) -> None:
        calls = 0
        derives = 0
        counter_lock = threading.Lock()

        class FakeClobClient:
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                del args, kwargs
                nonlocal calls
                time.sleep(0.005)
                with counter_lock:
                    calls += 1

            def create_or_derive_api_key(self) -> str:
                nonlocal derives
                with counter_lock:
                    derives += 1
                return "creds"

        class FakeApiCreds:
            def __init__(self, api_key: str, api_secret: str, api_passphrase: str) -> None:
                self.api_key = api_key
                self.api_secret = api_secret
                self.api_passphrase = api_passphrase

        module = types.SimpleNamespace(ClobClient=FakeClobClient)
        module_clob_types = types.SimpleNamespace(ApiCreds=FakeApiCreds)
        client = PolymarketClobClient(PolymarketConfig("key", "https://clob.polymarket.com", 137, 0, None))

        with patch.dict(sys.modules, {"py_clob_client_v2": module, "py_clob_client_v2.clob_types": module_clob_types}):
            with ThreadPoolExecutor(max_workers=8) as executor:
                clients = list(executor.map(lambda _: client._get_sdk_client(), range(16)))

        self.assertTrue(all(item is clients[0] for item in clients))
        self.assertEqual(calls, 2)
        self.assertEqual(derives, 1)

    def test_sdk_client_uses_explicit_api_creds_without_deriving(self) -> None:
        calls = 0
        derives = 0

        class FakeApiCreds:
            def __init__(self, api_key: str, api_secret: str, api_passphrase: str) -> None:
                self.api_key = api_key
                self.api_secret = api_secret
                self.api_passphrase = api_passphrase

        class FakeClobClient:
            instances: list["FakeClobClient"] = []

            def __init__(self, *args: Any, **kwargs: Any) -> None:
                del args
                nonlocal calls
                calls += 1
                self.kwargs = kwargs
                FakeClobClient.instances.append(self)

            def create_or_derive_api_key(self) -> str:
                nonlocal derives
                derives += 1
                return "derived"

        module = types.SimpleNamespace(ClobClient=FakeClobClient)
        module_clob_types = types.SimpleNamespace(ApiCreds=FakeApiCreds)
        client = PolymarketClobClient(
            PolymarketConfig(
                "key",
                "https://clob.polymarket.com",
                137,
                0,
                None,
                api_key="api-key",
                api_secret="api-secret",
                api_passphrase="api-passphrase",
            )
        )

        with patch.dict(
            sys.modules,
            {
                "py_clob_client_v2": module,
                "py_clob_client_v2.clob_types": module_clob_types,
            },
        ):
            sdk_client = client._get_sdk_client()

        self.assertIs(sdk_client, FakeClobClient.instances[-1])
        self.assertEqual(calls, 1)
        self.assertEqual(derives, 0)
        creds = sdk_client.kwargs["creds"]
        self.assertEqual(creds.api_key, "api-key")
        self.assertEqual(creds.api_secret, "api-secret")
        self.assertEqual(creds.api_passphrase, "api-passphrase")

    def test_clob_ws_url_is_derived_from_api_base_url(self) -> None:
        self.assertEqual(
            _clob_ws_url("https://clob.polymarket.com"),
            "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        )

    def test_normalize_collateral_balance_scales_micro_units(self) -> None:
        self.assertEqual(_normalize_collateral_balance("362536920"), 362.53692)

    def test_normalize_collateral_balance_preserves_decimal_strings(self) -> None:
        self.assertEqual(_normalize_collateral_balance("12.5"), 12.5)

    def test_sdk_call_retries_after_transient_disconnect(self) -> None:
        client = PolymarketClobClient(PolymarketConfig(None, "https://clob.polymarket.com", 137, 0, None))
        first = MagicMock()
        second = MagicMock()
        first.get_balance_allowance.side_effect = RuntimeError("Server disconnected")
        second.get_balance_allowance.return_value = {"balance": "12.5"}

        with (
            patch.object(client, "_get_sdk_client", side_effect=[first, second]),
            patch.object(client, "_reset_sdk_client") as reset_sdk_client,
        ):
            result = client._sdk_call(lambda sdk: sdk.get_balance_allowance())

        self.assertEqual(result, {"balance": "12.5"})
        reset_sdk_client.assert_called_once_with()
        first.get_balance_allowance.assert_called_once_with()
        second.get_balance_allowance.assert_called_once_with()

    def test_sdk_call_does_not_retry_non_transient_error(self) -> None:
        client = PolymarketClobClient(PolymarketConfig(None, "https://clob.polymarket.com", 137, 0, None))
        first = MagicMock()
        first.get_order.side_effect = RuntimeError("invalid market")

        with (
            patch.object(client, "_get_sdk_client", return_value=first),
            patch.object(client, "_reset_sdk_client") as reset_sdk_client,
            self.assertRaisesRegex(RuntimeError, "invalid market"),
        ):
            client._sdk_call(lambda sdk: sdk.get_order("abc"))

        reset_sdk_client.assert_not_called()
        first.get_order.assert_called_once_with("abc")

    def test_incremental_subscription_uses_subscribe_operation(self) -> None:
        self.assertEqual(
            _subscription_payload(["token"], operation="subscribe"),
            {
                "assets_ids": ["token"],
                "custom_feature_enabled": True,
                "operation": "subscribe",
            },
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
        client = PolymarketClobClient(PolymarketConfig(None, "https://clob.polymarket.com", 137, 0, None))
        session = MagicMock()
        session.closed = False

        with patch("arbitrage_engine.connectors.polymarket.client_session", return_value=session) as factory:
            self.assertIs(client._get_rest_session(), session)
            self.assertIs(client._get_rest_session(), session)

        factory.assert_called_once()

    def test_stream_health_uses_orderbook_updates_not_socket_pongs(self) -> None:
        client = PolymarketClobClient(PolymarketConfig(None, "https://clob.polymarket.com", 137, 0, None))
        client._desired_tokens.add("token")
        client._books["token"] = OrderBook([], [], timestamp=time.time() - 30)
        client._book_timestamps["token"] = time.monotonic() - 30

        age = client.market_data_age_seconds()

        self.assertIsNotNone(age)
        self.assertGreater(age or 0.0, 29.0)

    def test_stream_health_tracks_latest_venue_event_not_stalest_book(self) -> None:
        client = PolymarketClobClient(PolymarketConfig(None, "https://clob.polymarket.com", 137, 0, None))
        client._desired_tokens.update({"stale", "fresh"})
        client._ws_connected = True
        client._books = {"stale": OrderBook([], []), "fresh": OrderBook([], [])}
        client._book_timestamps = {
            "stale": time.monotonic() - 30,
            "fresh": time.monotonic() - 0.1,
        }

        self.assertLess(client.market_data_age_seconds() or 1.0, 0.5)
        self.assertTrue(client.market_data_ready())

    def test_reconnect_backoff_is_bounded_and_never_zero(self) -> None:
        backoff = WebSocketReconnectBackoff()

        with patch("arbitrage_engine.connectors.base.random.uniform", side_effect=lambda _low, high: high):
            delays = [backoff.next_delay() for _ in range(8)]

        self.assertEqual(delays, [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0, 30.0])
        backoff.reset()
        with patch("arbitrage_engine.connectors.base.random.uniform", return_value=0.0):
            self.assertEqual(backoff.next_delay(), 0.1)

    def test_stream_is_not_ready_until_every_desired_token_is_bootstrapped(self) -> None:
        client = PolymarketClobClient(PolymarketConfig(None, "https://clob.polymarket.com", 137, 0, None))
        client._desired_tokens.update({"present", "missing"})
        client._books["present"] = OrderBook([], [])

        self.assertFalse(client.market_data_ready())


class PolymarketLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_list_open_orders_reads_only_first_page(self) -> None:
        client = PolymarketClobClient(PolymarketConfig(None, "https://clob.polymarket.com", 137, 0, None))
        sdk_client = MagicMock()
        sdk_client.get_open_orders.return_value = []

        with patch.object(client, "_get_sdk_client", return_value=sdk_client):
            orders = await client.list_open_orders()

        self.assertEqual(orders, [])
        sdk_client.get_open_orders.assert_called_once_with(None, True)

    async def test_concurrent_reconnect_is_idempotent_and_preserves_book_age(self) -> None:
        client = PolymarketClobClient(PolymarketConfig(None, "https://clob.polymarket.com", 137, 0, None))
        client._desired_tokens.add("token")
        client._books["token"] = OrderBook([], [])
        timestamp = time.monotonic() - 5
        client._book_timestamps["token"] = timestamp
        client._ws = MagicMock(closed=False)
        client._ws.close = AsyncMock()
        client._ws_task = asyncio.create_task(asyncio.sleep(3600))

        await asyncio.gather(client.reconnect_market_data(), client.reconnect_market_data())

        client._ws.close.assert_awaited_once()
        self.assertEqual(client._book_timestamps["token"], timestamp)
        self.assertIs(client._books["token"].status, MarketDataStatus.STALE)
        self.assertTrue(client._reconnecting)
        client._ws_task.cancel()
        await asyncio.gather(client._ws_task, return_exceptions=True)

    async def test_http_orderbook_requests_are_limited_to_twenty(self) -> None:
        client = PolymarketClobClient(PolymarketConfig(None, "https://clob.polymarket.com", 137, 0, None))
        active = 0
        max_active = 0

        class Response:
            async def __aenter__(self) -> "Response":
                nonlocal active, max_active
                active += 1
                max_active = max(max_active, active)
                return self

            async def __aexit__(self, *args: Any) -> None:
                del args
                nonlocal active
                active -= 1

            def raise_for_status(self) -> None:
                return

            async def json(self) -> dict[str, Any]:
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

    async def test_passively_fresh_cached_book_is_reused_without_snapshot_timeout(self) -> None:
        client = PolymarketClobClient(PolymarketConfig(None, "https://clob.polymarket.com", 137, 0, None))
        token_id = "token"
        expected = OrderBook(
            bids=[OrderBookLevel(0.4, 1.0)],
            asks=[OrderBookLevel(0.41, 1.0)],
            timestamp=time.time() - 0.5,
        )
        client._books[token_id] = expected
        client._book_timestamps[token_id] = time.monotonic() - 0.5
        client._snapshot_timestamps[token_id] = time.monotonic()
        client._book_events[token_id] = asyncio.Event()
        client._ws_task = asyncio.create_task(asyncio.sleep(60))

        book = await client.watch_order_book(token_id)

        self.assertIs(book, expected)
        self.assertEqual(client._snapshot_timeout_count, 0)
        await client.close()

    async def test_stale_cached_book_uses_single_http_refresh_without_timeout_accounting(self) -> None:
        client = PolymarketClobClient(PolymarketConfig(None, "https://clob.polymarket.com", 137, 0, None))
        token_id = "token"
        client._books[token_id] = OrderBook(
            bids=[OrderBookLevel(0.4, 1.0)],
            asks=[OrderBookLevel(0.41, 1.0)],
            timestamp=time.time() - 5.0,
        )
        client._book_timestamps[token_id] = time.monotonic() - 5.0
        client._book_events[token_id] = asyncio.Event()

        fresh = OrderBook(
            bids=[OrderBookLevel(0.42, 1.0)],
            asks=[OrderBookLevel(0.43, 1.0)],
            timestamp=time.time(),
        )

        async def refresh(token: str) -> OrderBook:
            self.assertEqual(token, token_id)
            client._update_book(token_id, fresh)
            client._snapshot_timestamps[token_id] = time.monotonic()
            return fresh

        client._fetch_order_book_http = AsyncMock(side_effect=refresh)  # type: ignore[method-assign]

        book = await client.watch_order_book(token_id)

        self.assertIs(book, fresh)
        self.assertEqual(client._snapshot_timeout_count, 0)
        client._fetch_order_book_http.assert_awaited_once()

    async def test_failed_stale_refresh_is_cooldown_bounded(self) -> None:
        client = PolymarketClobClient(PolymarketConfig(None, "https://clob.polymarket.com", 137, 0, None))
        token_id = "token"
        client._books[token_id] = OrderBook(
            bids=[OrderBookLevel(0.4, 1.0)],
            asks=[OrderBookLevel(0.41, 1.0)],
            timestamp=time.time() - 5.0,
        )
        client._book_timestamps[token_id] = time.monotonic() - 5.0
        client._book_events[token_id] = asyncio.Event()
        client._fetch_order_book_http = AsyncMock(side_effect=RuntimeError("boom"))  # type: ignore[method-assign]

        with self.assertRaisesRegex(RuntimeError, "boom"):
            await client.watch_order_book(token_id)
        with self.assertRaisesRegex(RuntimeError, "cooling down"):
            await client.watch_order_book(token_id)

        client._fetch_order_book_http.assert_awaited_once()

    async def test_close_releases_sessions_and_ws_task(self) -> None:
        client = PolymarketClobClient(PolymarketConfig(None, "https://clob.polymarket.com", 137, 0, None))
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

    async def test_sync_market_data_targets_prunes_stale_history_and_restores_readiness(self) -> None:
        client = PolymarketClobClient(PolymarketConfig(None, "https://clob.polymarket.com", 137, 0, None))
        stale_task = asyncio.create_task(asyncio.sleep(60))
        client._desired_tokens.update({"fresh", "stale"})
        client._bootstrap_tasks["stale"] = cast(asyncio.Task[OrderBook], stale_task)
        client._ws_connected = True
        client._books = {
            "fresh": OrderBook([], []),
            "stale": OrderBook([], [], status=MarketDataStatus.STALE),
        }
        client._book_timestamps = {
            "fresh": time.monotonic() - 0.1,
            "stale": time.monotonic() - 30,
        }

        client.sync_market_data_targets({"fresh"})
        await asyncio.gather(stale_task, return_exceptions=True)

        self.assertEqual(client._desired_tokens, {"fresh"})
        self.assertNotIn("stale", client._books)
        self.assertNotIn("stale", client._book_timestamps)
        self.assertTrue(stale_task.cancelled())
        self.assertTrue(client.market_data_ready())

    async def test_sync_market_data_targets_bootstraps_added_tokens(self) -> None:
        client = PolymarketClobClient(PolymarketConfig(None, "https://clob.polymarket.com", 137, 0, None))
        client._ws_connected = True
        client._ws_task = asyncio.create_task(asyncio.sleep(60))

        async def refresh(token_id: str) -> OrderBook:
            await asyncio.sleep(0.01)
            book = OrderBook([OrderBookLevel(0.4, 1.0)], [OrderBookLevel(0.41, 1.0)], timestamp=time.time())
            client._update_book(token_id, book)
            client._snapshot_timestamps[token_id] = time.monotonic()
            return book

        client._fetch_order_book_http = AsyncMock(side_effect=refresh)  # type: ignore[method-assign]

        client.sync_market_data_targets({"token-a", "token-b"})
        tasks = list(client._bootstrap_tasks.values())
        await asyncio.gather(*tasks)

        self.assertEqual(client._fetch_order_book_http.await_count, 2)
        self.assertEqual(set(client._books), {"token-a", "token-b"})
        self.assertTrue(client.market_data_ready())
        await client.close()

    async def test_reconnect_failure_recycles_ws_session(self) -> None:
        client = PolymarketClobClient(PolymarketConfig(None, "https://clob.polymarket.com", 137, 0, None))
        session = MagicMock()
        session.closed = False
        session.close = AsyncMock()
        session.ws_connect.side_effect = RuntimeError("boom")
        client._ws_session = session

        with patch.object(client, "_get_ws_session", return_value=session):
            task = asyncio.create_task(client._run_order_book_ws())
            for _ in range(20):
                if session.close.await_count:
                    break
                await asyncio.sleep(0.01)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        session.close.assert_awaited()
        self.assertIsNone(client._ws_session)

    async def test_all_tokens_share_one_ws_task(self) -> None:
        client = PolymarketClobClient(PolymarketConfig(None, "https://clob.polymarket.com", 137, 0, None))
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
        client = PolymarketClobClient(PolymarketConfig(None, "https://clob.polymarket.com", 137, 0, None))
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
