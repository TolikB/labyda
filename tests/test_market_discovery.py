import unittest
from datetime import datetime, timezone

from arbitrage_engine.market_discovery import _best_candidate, _token_id_for_side
from arbitrage_engine.models import BinarySide, MarketSpec


def _market(*, external_id: str | None = None) -> MarketSpec:
    return MarketSpec(
        symbol="Will England defeat Panama?",
        target_label="Will England defeat Panama?",
        polymarket_token_id="",
        polymarket_side=BinarySide.YES,
        predict_fun_token_id="",
        predict_fun_side=BinarySide.NO,
        expires_at=datetime(2026, 6, 28, 21, tzinfo=timezone.utc),
        polymarket_market_id=external_id,
    )


class GammaDiscoveryTests(unittest.TestCase):
    def test_search_fallback_rejects_unrelated_gamma_results(self) -> None:
        candidates = [
            {
                "id": "1",
                "question": "New Rihanna Album before GTA VI?",
                "endDate": "2026-07-31T12:00:00Z",
            }
        ]

        self.assertIsNone(_best_candidate(candidates, _market()))

    def test_external_market_id_is_an_exact_lookup_key(self) -> None:
        candidates = [
            {"id": "other", "question": "Will England defeat Panama?"},
            {"id": "1897417", "question": "Will England win on 2026-06-27?"},
        ]

        selected = _best_candidate(candidates, _market(external_id="1897417"))

        self.assertIsNotNone(selected)
        assert selected is not None
        self.assertEqual(selected["id"], "1897417")

    def test_token_mapping_uses_outcome_labels_not_array_position(self) -> None:
        candidate = {
            "outcomes": '["No", "Yes"]',
            "clobTokenIds": '["no-token", "yes-token"]',
        }

        self.assertEqual(_token_id_for_side(candidate, BinarySide.YES), "yes-token")
        self.assertEqual(_token_id_for_side(candidate, BinarySide.NO), "no-token")


if __name__ == "__main__":
    unittest.main()
