import unittest
from datetime import UTC
from types import SimpleNamespace
from typing import Any

from arbitrage_engine.myriad_discovery import (
    MyriadMarketResolver,
    _extract_market_list,
    _market_category,
    _market_query_params,
    _market_text,
    _parse_datetime,
)


class MyriadDiscoveryTests(unittest.TestCase):
    def test_timezone_less_expiry_is_normalized_to_utc(self) -> None:
        parsed = _parse_datetime("2026-06-30T12:00:00")
        self.assertEqual(parsed and parsed.tzinfo, UTC)

    def test_market_query_requests_orderbook_trading_model(self) -> None:
        self.assertEqual(
            _market_query_params(56),
            {"network_id": 56, "trading_model": "ob", "state": "open", "limit": 100},
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

    def test_market_text_maps_outcomes_by_id_and_keeps_polymarket_reference(self) -> None:
        market = _market_text(
            {
                "id": 553,
                "title": "Will England defeat Panama?",
                "expiresAt": "2026-06-28T21:00:00Z",
                "outcomes": [{"id": 1, "title": "No"}, {"id": 0, "title": "Yes"}],
                "externalSources": [{"providerName": "polymarket", "externalMarketId": "1897417"}],
            }
        )

        self.assertIsNotNone(market)
        assert market is not None
        self.assertEqual(market.yes_label, "Yes")
        self.assertEqual(market.no_label, "No")
        self.assertEqual(market.external_market_id, "1897417")

    def test_market_text_rejects_ambiguous_outcome_ids(self) -> None:
        market = _market_text(
            {
                "id": 553,
                "title": "Will England defeat Panama?",
                "expiresAt": "2026-06-28T21:00:00Z",
                "outcomes": [{"id": 0, "title": "No"}, {"id": 1, "title": "Yes"}],
            }
        )

        self.assertIsNone(market)

    def test_market_category_uses_topics_for_sports_payloads(self) -> None:
        category = _market_category(
            {
                "topics": ["Sports"],
                "scoreboard": {"type": "soccer"},
                "tags": [{"type": "league", "title": "World Cup"}],
            }
        )

        self.assertEqual(category, "Sports")


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
            async def _fetch_markets(self) -> list[dict[str, Any]]:
                return payloads

        config = SimpleNamespace(enabled=True)
        markets = await Resolver(config, scan_all=True).resolve([])  # type: ignore[arg-type]

        self.assertEqual([market.myriad_market_id for market in markets], ["123", "456"])

    async def test_scan_all_filters_to_allowed_categories(self) -> None:
        payloads = [
            {
                "marketId": 123,
                "question": "Will Arsenal win?",
                "expiresAt": "2026-12-31T00:00:00Z",
                "outcomes": [{"name": "YES"}, {"name": "NO"}],
                "category": "sport",
            },
            {
                "marketId": 456,
                "question": "Will BTC exceed 100000?",
                "expiresAt": "2026-12-31T00:00:00Z",
                "outcomes": [{"name": "YES"}, {"name": "NO"}],
                "category": "finance",
            },
        ]

        class Resolver(MyriadMarketResolver):
            async def _fetch_markets(self) -> list[dict[str, Any]]:
                return payloads

        config = SimpleNamespace(enabled=True)
        markets = await Resolver(config, scan_all=True, categories_to_scan=["sport"]).resolve([])  # type: ignore[arg-type]

        self.assertEqual([market.myriad_market_id for market in markets], ["123"])


if __name__ == "__main__":
    unittest.main()
