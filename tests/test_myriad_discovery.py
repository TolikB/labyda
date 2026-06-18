import unittest
from types import SimpleNamespace

from arbitrage_engine.myriad_discovery import (
    MyriadMarketResolver,
    _extract_market_list,
    _market_query_params,
    _market_text,
)


class MyriadDiscoveryTests(unittest.TestCase):
    def test_market_query_requests_orderbook_trading_model(self) -> None:
        self.assertEqual(
            _market_query_params(56),
            {"network_id": 56, "trading_model": "ob", "active": "true"},
        )

    def test_extract_market_list_supports_wrapped_data(self) -> None:
        payload = {"data": {"markets": [{"id": 1}, {"id": 2}]}}

        self.assertEqual([item["id"] for item in _extract_market_list(payload)], [1, 2])

    def test_market_text_reads_common_myriad_fields(self) -> None:
        market = _market_text(
            {
                "marketId": 123,
                "question": "Will BTC close above $75,000?",
                "expiresAt": "2026-06-30T12:00:00Z",
                "outcomes": [{"name": "YES"}, {"name": "NO"}],
            }
        )

        self.assertIsNotNone(market)
        assert market is not None
        self.assertEqual(market.market_id, "123")
        self.assertEqual(market.yes_label, "YES")
        self.assertEqual(market.no_label, "NO")


class MyriadScanAllTests(unittest.IsolatedAsyncioTestCase):
    async def test_scan_all_returns_every_valid_myriad_market(self) -> None:
        payloads = [
            {
                "marketId": 123,
                "question": "Will BTC exceed 100000?",
                "expiresAt": "2026-12-31T00:00:00Z",
                "outcomes": [{"name": "YES"}, {"name": "NO"}],
            },
            {
                "marketId": 456,
                "question": "Will candidate X win?",
                "expiresAt": "2026-11-01T00:00:00Z",
                "outcomes": [{"name": "YES"}, {"name": "NO"}],
            },
        ]

        class Resolver(MyriadMarketResolver):
            async def _fetch_markets(self):
                return payloads

        config = SimpleNamespace(enabled=True)
        markets = await Resolver(config, scan_all=True).resolve([])  # type: ignore[arg-type]

        self.assertEqual([market.myriad_market_id for market in markets], ["123", "456"])


if __name__ == "__main__":
    unittest.main()
