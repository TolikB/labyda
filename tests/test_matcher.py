import unittest
from datetime import datetime, timedelta, timezone

from arbitrage_engine.matcher import MarketText, SemanticMarketMatcher, normalize_text, text_similarity
from arbitrage_engine.models import BinarySide


class MatcherTests(unittest.TestCase):
    def test_normalize_text_removes_stop_words(self) -> None:
        self.assertEqual(normalize_text("Will BTC be the price above $75,000?"), "btc above 75 000")

    def test_text_similarity_handles_title_variants(self) -> None:
        score = text_similarity("Will Arsenal beat Chelsea?", "Arsenal vs Chelsea")

        self.assertGreater(score, 0.5)

    def test_matcher_rejects_expiry_difference_over_30_minutes(self) -> None:
        now = datetime.now(timezone.utc)
        left = [MarketText("poly", "1", "Will BTC be above 75000?", now)]
        right = [MarketText("predict", "2", "Will BTC be above 75000?", now + timedelta(minutes=31))]

        self.assertEqual(SemanticMarketMatcher().match(left, right), [])

    def test_matcher_returns_opposite_side_for_same_yes_label(self) -> None:
        now = datetime.now(timezone.utc)
        left = [MarketText("poly", "1", "Will BTC be above 75000?", now, yes_label="YES")]
        right = [MarketText("predict", "2", "BTC above 75000", now + timedelta(minutes=10), yes_label="YES")]

        matches = SemanticMarketMatcher(min_similarity=0.5).match(left, right)

        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].left_side, BinarySide.YES)
        self.assertEqual(matches[0].right_side, BinarySide.NO)


if __name__ == "__main__":
    unittest.main()
