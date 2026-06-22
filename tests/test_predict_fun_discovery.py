import unittest
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

from arbitrage_engine.models import BinarySide, MarketSpec
from arbitrage_engine.predict_fun_discovery import (
    PREDICT_MARKETS_PATH,
    PredictFunMarketResolver,
    _best_candidate,
    _extract_market_list,
    _next_cursor,
    _optional_bool,
    _parse_datetime,
    _token_id_for_side,
)


class PredictFunDiscoveryTests(unittest.TestCase):
    def test_timezone_less_expiry_is_normalized_to_utc(self) -> None:
        parsed = _parse_datetime("2026-06-30T12:00:00")
        self.assertEqual(parsed and parsed.tzinfo, UTC)

    def test_nested_page_info_cursor_is_supported(self) -> None:
        payload = {"data": {"pageInfo": {"hasNextPage": True, "endCursor": "next-page"}}}

        self.assertEqual(_next_cursor(payload, None), "next-page")

    def test_outcome_mapping_rejects_unlabelled_index_order(self) -> None:
        candidate = {"tokenIds": ["first", "second"]}

        self.assertIsNone(_token_id_for_side(candidate, BinarySide.YES))
        self.assertIsNone(_token_id_for_side(candidate, BinarySide.NO))

    def test_token_mapping_supports_live_api_on_chain_id(self) -> None:
        payload = {
            "outcomes": [
                {"name": "Yes", "onChainId": "101"},
                {"name": "No", "onChainId": "202"},
            ]
        }

        self.assertEqual(_token_id_for_side(payload, BinarySide.YES), "101")
        self.assertEqual(_token_id_for_side(payload, BinarySide.NO), "202")

    def test_discovery_uses_current_v1_markets_endpoint(self) -> None:
        self.assertEqual(PREDICT_MARKETS_PATH, "/v1/markets")

    def test_extract_market_list_supports_wrapped_data(self) -> None:
        payload = {"data": {"markets": [{"id": "one"}, {"id": "two"}]}}

        self.assertEqual([item["id"] for item in _extract_market_list(payload)], ["one", "two"])

    def test_best_candidate_scores_symbol_and_target(self) -> None:
        market = MarketSpec(
            symbol="BTC-USD",
            target_label=">$75,000",
            polymarket_token_id="",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="",
            predict_fun_side=BinarySide.NO,
        )
        candidates: list[dict[str, Any]] = [
            {"question": "Will ETH be above 5000?"},
            {"question": "Will BTC USD be above $75,000?", "tokens": []},
        ]

        self.assertEqual(_best_candidate(candidates, market), candidates[1])

    def test_best_candidate_rejects_more_specific_superset_market(self) -> None:
        market = MarketSpec(
            symbol="Will Turkiye win the 2026 FIFA World Cup?",
            target_label="Will Turkiye win the 2026 FIFA World Cup?",
            polymarket_token_id="",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="",
            predict_fun_side=BinarySide.NO,
            expires_at=datetime(2026, 7, 19, tzinfo=UTC),
        )
        candidates = [
            {
                "id": "opening-match",
                "question": "Will Turkiye win the 2026 FIFA World Cup opening match?",
                "expiresAt": "2026-07-19T00:00:00Z",
            },
            {
                "id": "group",
                "question": "Will Turkiye win their group in the 2026 FIFA World Cup?",
                "expiresAt": "2026-07-19T00:00:00Z",
            },
        ]

        self.assertIsNone(_best_candidate(candidates, market))

    def test_best_candidate_rejects_ambiguous_equal_titles(self) -> None:
        market = MarketSpec(
            symbol="Will Turkiye win the 2026 FIFA World Cup?",
            target_label="Will Turkiye win the 2026 FIFA World Cup?",
            polymarket_token_id="",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="",
            predict_fun_side=BinarySide.NO,
        )
        candidates = [
            {"id": "one", "question": market.symbol},
            {"id": "two", "question": market.symbol},
        ]

        self.assertIsNone(_best_candidate(candidates, market))

    def test_best_candidate_requires_expiry_when_source_has_expiry(self) -> None:
        market = MarketSpec(
            symbol="Will Turkiye win the 2026 FIFA World Cup?",
            target_label="Will Turkiye win the 2026 FIFA World Cup?",
            polymarket_token_id="",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="",
            predict_fun_side=BinarySide.NO,
            expires_at=datetime(2026, 7, 19, tzinfo=UTC),
        )

        self.assertIsNone(_best_candidate([{"id": "missing-expiry", "question": market.symbol}], market))

    def test_token_id_for_side_supports_outcome_objects(self) -> None:
        candidate = {
            "tokens": [
                {"side": "YES", "tokenId": "yes-token"},
                {"side": "NO", "tokenId": "no-token"},
            ]
        }

        self.assertEqual(_token_id_for_side(candidate, BinarySide.YES), "yes-token")
        self.assertEqual(_token_id_for_side(candidate, BinarySide.NO), "no-token")

    def test_optional_bool_supports_predict_fun_neg_risk_fields(self) -> None:
        self.assertTrue(_optional_bool({"isNegRisk": "true"}, ("isNegRisk",)))
        self.assertFalse(_optional_bool({"negRisk": False}, ("negRisk",)))


class PredictFunScanAllTests(unittest.IsolatedAsyncioTestCase):
    async def test_scan_all_does_not_hide_discovery_api_failure(self) -> None:
        class Resolver(PredictFunMarketResolver):
            async def _fetch_markets(self) -> list[dict[str, Any]]:
                raise RuntimeError("authentication rejected")

        config = SimpleNamespace(api_base_url="https://api.predict.fun", api_key=None)

        with self.assertRaisesRegex(RuntimeError, "Predict.fun discovery failed"):
            await Resolver(config, scan_all=True).resolve([])  # type: ignore[arg-type]

    async def test_scan_all_returns_every_valid_api_market_without_text_filter(self) -> None:
        payloads: list[dict[str, Any]] = [
            {
                "id": "btc",
                "question": "Will BTC exceed 100000?",
                "expiresAt": "2026-12-31T00:00:00Z",
                "feeRateBps": 125,
                "tokens": [{"side": "YES", "tokenId": "btc-yes"}, {"side": "NO", "tokenId": "btc-no"}],
            },
            {
                "id": "election",
                "question": "Will candidate X win?",
                "expiresAt": "2026-11-01T00:00:00Z",
                "tokens": [{"side": "YES", "tokenId": "x-yes"}, {"side": "NO", "tokenId": "x-no"}],
            },
        ]

        class Resolver(PredictFunMarketResolver):
            async def _fetch_markets(self) -> list[dict[str, Any]]:
                return payloads

        config = SimpleNamespace(api_base_url="https://example.invalid", api_key=None)
        markets = await Resolver(config, scan_all=True).resolve([])  # type: ignore[arg-type]

        self.assertEqual([market.predict_fun_market_id for market in markets], ["btc", "election"])
        self.assertEqual([market.predict_fun_token_id for market in markets], ["btc-no", "x-no"])
        self.assertEqual(markets[0].predict_fun_fee_rate_bps, 125)


if __name__ == "__main__":
    unittest.main()
