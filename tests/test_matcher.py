import unittest
from datetime import datetime, timedelta, timezone

from arbitrage_engine.matcher import MarketText, SemanticMarketMatcher, normalize_text, text_similarity
from arbitrage_engine.models import BinarySide


class MatcherTests(unittest.TestCase):
    def test_normalize_text_removes_stop_words(self) -> None:
        self.assertEqual(normalize_text("Will BTC be the price above $75,000?"), "btc above 75000")

    def test_normalize_text_translates_token_bounded_aliases(self) -> None:
        left = "Will Bitcoin be greater than $75,000?"
        right = "BTC above 75000"

        self.assertEqual(normalize_text(left), normalize_text(right))
        self.assertEqual(normalize_text("Ethereum versus Solana"), "eth vs sol")
        self.assertEqual(normalize_text("turnover"), "turnover")

    def test_normalize_text_removes_platform_date_time_suffixes(self) -> None:
        canonical = normalize_text("Bitcoin above $75,000")

        self.assertEqual(normalize_text("Bitcoin above $75,000 (June 20, 2026 12:00 PM ET)"), canonical)
        self.assertEqual(normalize_text("BTC above 75000 - expires: 2026-06-20 16:00 UTC"), canonical)
        self.assertEqual(normalize_text("BTC above 75000 | 20/06/2026 16:00 UTC"), canonical)

    def test_normalize_text_preserves_semantic_dates_and_cutoff_words(self) -> None:
        self.assertEqual(
            normalize_text("Will Turkiye win the 2026 FIFA World Cup?"),
            "turkiye win 2026 fifa world cup",
        )
        self.assertEqual(
            normalize_text("Will BTC be above 75000 by June 30, 2026?"),
            "btc above 75000 by june 30 2026",
        )

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
