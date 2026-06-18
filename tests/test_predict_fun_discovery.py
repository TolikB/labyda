import unittest
from types import SimpleNamespace

from arbitrage_engine.models import BinarySide, MarketSpec
from arbitrage_engine.predict_fun_discovery import (
    _best_candidate,
    _extract_market_list,
    _optional_bool,
    _token_id_for_side,
    PREDICT_MARKETS_PATH,
    PredictFunMarketResolver,
)


class PredictFunDiscoveryTests(unittest.TestCase):
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
        candidates = [
            {"question": "Will ETH be above 5000?"},
            {"question": "Will BTC USD be above $75,000?", "tokens": []},
        ]

        self.assertEqual(_best_candidate(candidates, market), candidates[1])

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
            async def _fetch_markets(self):
                raise RuntimeError("authentication rejected")

        config = SimpleNamespace(api_base_url="https://api.predict.fun", api_key=None)

        with self.assertRaisesRegex(RuntimeError, "Predict.fun discovery failed"):
            await Resolver(config, scan_all=True).resolve([])  # type: ignore[arg-type]

    async def test_scan_all_returns_every_valid_api_market_without_text_filter(self) -> None:
        payloads = [
            {
                "id": "btc",
                "question": "Will BTC exceed 100000?",
                "expiresAt": "2026-12-31T00:00:00Z",
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
            async def _fetch_markets(self):
                return payloads

        config = SimpleNamespace(api_base_url="https://example.invalid", api_key=None)
        markets = await Resolver(config, scan_all=True).resolve([])  # type: ignore[arg-type]

        self.assertEqual([market.predict_fun_market_id for market in markets], ["btc", "election"])
        self.assertEqual([market.predict_fun_token_id for market in markets], ["btc-no", "x-no"])


if __name__ == "__main__":
    unittest.main()
