import unittest
from datetime import UTC
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

from arbitrage_engine.models import BinarySide, MarketSpec
from arbitrage_engine.myriad_discovery import (
    MyriadMarketResolver,
    _expiry_window_seconds_for_market,
    _extract_market_list,
    _market_category,
    _market_query_params,
    _market_text,
    _min_similarity_for_market,
    _parse_datetime,
    _source_market_text,
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

    def test_market_text_accepts_custom_binary_labels_with_explicit_outcome_ids(self) -> None:
        market = _market_text(
            {
                "id": 2386,
                "title": "Phillies vs. Cardinals: Who wins?",
                "expiresAt": "2026-08-13T18:15:00Z",
                "outcomes": [
                    {"id": 0, "title": "Phillies"},
                    {"id": 1, "title": "Cardinals"},
                ],
                "externalSources": [
                    {"providerName": "polymarket", "externalMarketId": "3379516"}
                ],
            }
        )

        self.assertIsNotNone(market)
        assert market is not None
        self.assertEqual((market.yes_label, market.no_label), ("Phillies", "Cardinals"))
        self.assertEqual(market.external_market_id, "3379516")

    def test_market_text_rejects_custom_binary_labels_without_explicit_outcome_ids(self) -> None:
        market = _market_text(
            {
                "id": 2386,
                "title": "Phillies vs. Cardinals: Who wins?",
                "expiresAt": "2026-08-13T18:15:00Z",
                "outcomes": [{"title": "Phillies"}, {"title": "Cardinals"}],
            }
        )

        self.assertIsNone(market)

    def test_market_text_rejects_multi_outcome_markets(self) -> None:
        market = _market_text(
            {
                "id": 2386,
                "title": "Phillies vs. Cardinals vs. Draw",
                "expiresAt": "2026-08-13T18:15:00Z",
                "outcomes": [
                    {"id": 0, "title": "Phillies"},
                    {"id": 1, "title": "Cardinals"},
                    {"id": 2, "title": "Draw"},
                ],
            }
        )

        self.assertIsNone(market)

    def test_market_text_uses_nested_token_address_when_flat_collateral_is_missing(self) -> None:
        market = _market_text(
            {
                "id": 410,
                "title": "Will Switzerland win Group E?",
                "expiresAt": "2026-06-30T12:00:00Z",
                "outcomes": [{"name": "YES"}, {"name": "NO"}],
                "token": {
                    "name": "World Liberty Financial USD",
                    "address": "0x8d0D000Ee44948FC98c9B98A4FA4921476f08B0d",
                    "symbol": "USD1",
                },
            }
        )

        self.assertIsNotNone(market)
        assert market is not None
        self.assertEqual(market.collateral_token, "0x8d0D000Ee44948FC98c9B98A4FA4921476f08B0d")

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
    async def test_cross_catalog_resolution_uses_cpu_executor(self) -> None:
        payloads = [
            {
                "marketId": 123,
                "question": "Will BTC exceed 100000?",
                "expiresAt": "2026-12-31T00:00:00Z",
                "outcomes": [{"name": "YES"}, {"name": "NO"}],
            }
        ]

        class Resolver(MyriadMarketResolver):
            async def _fetch_markets(self) -> list[dict[str, Any]]:
                return payloads

        calls: list[str] = []

        async def run_in_test_executor(function: Any, *args: Any, **kwargs: Any) -> Any:
            calls.append(function.__name__)
            return function(*args, **kwargs)

        market = MarketSpec(
            symbol="Will BTC exceed 100000?",
            target_label="YES",
            polymarket_token_id="poly-token",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="",
            predict_fun_side=BinarySide.NO,
            expires_at=_parse_datetime("2026-12-31T00:00:00Z"),
        )
        config = SimpleNamespace(enabled=True)
        with patch(
            "arbitrage_engine.myriad_discovery.run_discovery_cpu",
            new=run_in_test_executor,
        ):
            resolved = await Resolver(config, scan_all=True).resolve([market])  # type: ignore[arg-type]

        self.assertIn("_resolve_market_specs", calls)
        self.assertEqual(resolved[0].myriad_market_id, "123")

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

        self.assertEqual([market.myriad_market_id for market in markets], ["123", "123", "456", "456"])
        self.assertEqual([market.venue_b_label for market in markets], ["Myriad"] * 4)
        self.assertEqual(
            [(market.polymarket_side, market.myriad_side) for market in markets],
            [
                (BinarySide.YES, BinarySide.NO),
                (BinarySide.NO, BinarySide.YES),
                (BinarySide.YES, BinarySide.NO),
                (BinarySide.NO, BinarySide.YES),
            ],
        )

    async def test_scan_all_preserves_custom_outcome_labels_for_polymarket_token_selection(self) -> None:
        payloads = [
            {
                "id": 2386,
                "title": "Phillies vs. Cardinals: Who wins?",
                "expiresAt": "2026-08-13T18:15:00Z",
                "outcomes": [
                    {"id": 0, "title": "Phillies"},
                    {"id": 1, "title": "Cardinals"},
                ],
                "externalSources": [
                    {"providerName": "polymarket", "externalMarketId": "3379516"}
                ],
                "moneyline": True,
            }
        ]

        class Resolver(MyriadMarketResolver):
            async def _fetch_markets(self) -> list[dict[str, Any]]:
                return payloads

        config = SimpleNamespace(enabled=True)
        resolver = Resolver(
            config,  # type: ignore[arg-type]
            scan_all=True,
            categories_to_scan=["sports"],
        )
        markets = await resolver.resolve([])

        self.assertEqual([market.target_label for market in markets], ["Phillies", "Cardinals"])
        self.assertEqual([market.polymarket_market_id for market in markets], ["3379516", "3379516"])

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

        self.assertEqual([market.myriad_market_id for market in markets], ["123", "123"])
        self.assertEqual(
            [(market.polymarket_side, market.myriad_side) for market in markets],
            [(BinarySide.YES, BinarySide.NO), (BinarySide.NO, BinarySide.YES)],
        )

    async def test_resolve_normalizes_sports_category_for_expiry_tolerance(self) -> None:
        payloads = [
            {
                "marketId": 123,
                "question": "Will Spain win the 2026 FIFA World Cup?",
                "expiresAt": "2026-07-20T23:59:00Z",
                "outcomes": [{"name": "YES"}, {"name": "NO"}],
                "category": "Sports",
            }
        ]

        class Resolver(MyriadMarketResolver):
            async def _fetch_markets(self) -> list[dict[str, Any]]:
                return payloads

        market = MarketSpec(
            symbol="Will Spain win the 2026 FIFA World Cup?",
            target_label="YES",
            polymarket_token_id="poly-token",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="",
            predict_fun_side=BinarySide.NO,
            venue_b_label="Myriad",
            category="Sports",
            expires_at=_parse_datetime("2026-07-18T23:59:00Z"),
        )

        resolved = await Resolver(SimpleNamespace(enabled=True), scan_all=True).resolve([market])  # type: ignore[arg-type]

        self.assertEqual(resolved[0].myriad_market_id, "123")

    async def test_sx_market_matches_myriad_with_sports_window_and_symbol_title(self) -> None:
        payloads = [
            {
                "marketId": 396,
                "question": "Will France win the 2026 FIFA World Cup?",
                "expiresAt": "2026-07-20T23:59:00Z",
                "outcomes": [{"name": "YES"}, {"name": "NO"}],
                "category": "sports",
            }
        ]

        class Resolver(MyriadMarketResolver):
            async def _fetch_markets(self) -> list[dict[str, Any]]:
                return payloads

        config = SimpleNamespace(enabled=True)
        market = MarketSpec(
            symbol="Will France win the World Cup?",
            target_label="France",
            polymarket_token_id="",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="0xsx:NO",
            predict_fun_side=BinarySide.NO,
            venue_b_label="SX Bet",
            predict_fun_market_id="0xsx",
            expires_at=_parse_datetime("2026-07-22T12:00:00Z"),
            category="sports",
        )

        resolved = await Resolver(config).resolve([market])  # type: ignore[arg-type]

        self.assertEqual(resolved[0].myriad_market_id, "396")
        self.assertEqual(resolved[0].myriad_side, BinarySide.NO)

    async def test_existing_myriad_market_id_backfills_missing_settlement_metadata(self) -> None:
        payloads = [
            {
                "marketId": 396,
                "question": "Will France win the 2026 FIFA World Cup?",
                "expiresAt": "2026-07-20T23:59:00Z",
                "outcomes": [{"name": "YES"}, {"name": "NO"}],
                "conditionId": "condition-396",
                "collateralToken": "USD1",
                "category": "sports",
            }
        ]

        class Resolver(MyriadMarketResolver):
            async def _fetch_markets(self) -> list[dict[str, Any]]:
                return payloads

        config = SimpleNamespace(enabled=True)
        market = MarketSpec(
            symbol="Will France win the 2026 FIFA World Cup?",
            target_label="YES",
            polymarket_token_id="poly-token",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="",
            predict_fun_side=BinarySide.NO,
            myriad_market_id="396",
            myriad_side=BinarySide.NO,
            expires_at=_parse_datetime("2026-07-20T23:59:00Z"),
        )

        resolved = await Resolver(config).resolve([market])  # type: ignore[arg-type]

        self.assertEqual(resolved[0].myriad_condition_id, "condition-396")
        self.assertEqual(resolved[0].myriad_collateral_token, "USD1")

    def test_sx_market_uses_symbol_only_and_relaxed_similarity(self) -> None:
        market = MarketSpec(
            symbol="Will France win the World Cup?",
            target_label="France",
            polymarket_token_id="",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="0xsx:NO",
            predict_fun_side=BinarySide.NO,
            venue_b_label="SX Bet",
            expires_at=_parse_datetime("2026-07-22T12:00:00Z"),
            category="sports",
        )

        source = _source_market_text(market)

        self.assertEqual(source.title, "Will France win the World Cup?")
        self.assertEqual(_expiry_window_seconds_for_market(market), 7 * 24 * 60 * 60)
        self.assertEqual(_min_similarity_for_market(market), 0.78)


if __name__ == "__main__":
    unittest.main()
