import unittest

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


if __name__ == "__main__":
    unittest.main()
