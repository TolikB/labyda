import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from arbitrage_engine.config import PolymarketConfig
from arbitrage_engine.connectors.polymarket import PolymarketClobClient
from arbitrage_engine.connectors.polymarket import _apply_price_changes, _clob_ws_url, _order_book_from_payload
from arbitrage_engine.models import OrderBook, OrderBookLevel


class PolymarketWsTests(unittest.TestCase):
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


class PolymarketLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_close_releases_session_and_ws_tasks(self) -> None:
        client = PolymarketClobClient(
            PolymarketConfig(None, "https://clob.polymarket.com", 137, 0, None)
        )
        session = MagicMock()
        session.closed = False
        session.close = AsyncMock()
        client._rest_session = session
        task = asyncio.create_task(asyncio.sleep(60))
        client._ws_tasks["token"] = task

        await client.close()

        session.close.assert_awaited_once()
        self.assertTrue(task.cancelled())
        self.assertEqual(client._ws_tasks, {})


if __name__ == "__main__":
    unittest.main()
