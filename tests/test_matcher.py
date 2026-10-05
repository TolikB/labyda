import unittest
from datetime import UTC, datetime, timedelta

from arbitrage_engine.matcher import MarketText, SemanticMarketMatcher, normalize_text, text_similarity
from arbitrage_engine.models import BinarySide


class MatcherTests(unittest.TestCase):
    def test_normalize_text_removes_stop_words(self) -> None:
        self.assertEqual(normalize_text("Will BTC be the price above $75,000?"), "bitcoin above 75000")

    def test_normalize_text_translates_token_bounded_aliases(self) -> None:
        left = "Will Bitcoin be greater than $75,000?"
        right = "BTC above 75000"

        self.assertEqual(normalize_text(left), normalize_text(right))
        self.assertEqual(normalize_text("Ethereum versus Solana"), "ethereum vs solana")
        self.assertEqual(normalize_text("turnover"), "turnover")

    def test_normalize_text_uses_full_crypto_canonical_names(self) -> None:
        self.assertEqual(normalize_text("BTC XBT Bitcoin"), "bitcoin bitcoin bitcoin")
        self.assertEqual(normalize_text("ETH Ether Ethereum"), "ethereum ethereum ethereum")
        self.assertEqual(normalize_text("SOL DOGE USDT"), "solana dogecoin tether")

    def test_normalize_text_handles_unicode_diacritics_and_number_suffixes(self) -> None:
        pairs = (
            ("Will Bítcoin be greater than $100k?", "BTC above 100000"),
            ("Will Ethereum be over $1.5m?", "ETH above 1500000"),
            ("Will TÜRKİYE qualify?", "Will Turkiye qualify?"),
        )

        for left, right in pairs:
            with self.subTest(left=left, right=right):
                self.assertEqual(normalize_text(left), normalize_text(right))

    def test_normalize_text_removes_platform_date_time_suffixes(self) -> None:
        canonical = normalize_text("Bitcoin above $75,000")

        self.assertEqual(normalize_text("Bitcoin above $75,000 (June 20, 2026 12:00 PM ET)"), canonical)
        self.assertEqual(normalize_text("BTC above 75000 - expires: 2026-06-20 16:00 UTC"), canonical)
        self.assertEqual(normalize_text("BTC above 75000 | 20/06/2026 16:00 UTC"), canonical)
        self.assertEqual(normalize_text("BTC above 75000 Resolution time: 2026-06-20 16:00 UTC"), canonical)
        self.assertEqual(normalize_text("BTC above 75000 2026-06-20T16:00:00Z"), canonical)
        self.assertEqual(normalize_text("BTC above 75000 4pm UTC 2026"), canonical)

    def test_normalize_text_preserves_semantic_time_suffixes(self) -> None:
        self.assertEqual(
            normalize_text("Will BTC be above 75000 by 4pm UTC 2026?"),
            "bitcoin above 75000 by 4pm utc 2026",
        )

    def test_normalize_text_preserves_semantic_dates_and_cutoff_words(self) -> None:
        self.assertEqual(
            normalize_text("Will Turkiye win the 2026 FIFA World Cup?"),
            "turkiye win 2026 fifa world cup",
        )
        self.assertEqual(
            normalize_text("Will BTC be above 75000 by June 30, 2026?"),
            "bitcoin above 75000 by june 30 2026",
        )
        self.assertNotEqual(
            normalize_text("Will BTC be above 75000 by June 30, 2026?"),
            normalize_text("Will BTC be above 75000 by June 30, 2027?"),
        )
        self.assertNotEqual(
            normalize_text("Will Arsenal or Chelsea win?"),
            normalize_text("Will Arsenal and Chelsea win?"),
        )

    def test_text_similarity_handles_title_variants(self) -> None:
        score = text_similarity("Will Arsenal beat Chelsea?", "Arsenal vs Chelsea")

        self.assertGreater(score, 0.5)

    def test_matcher_rejects_expiry_difference_over_30_minutes(self) -> None:
        now = datetime.now(UTC)
        left = [MarketText("poly", "1", "Will BTC be above 75000?", now)]
        right = [MarketText("predict", "2", "Will BTC be above 75000?", now + timedelta(minutes=31))]

        self.assertEqual(SemanticMarketMatcher().match(left, right), [])

    def test_matcher_returns_opposite_side_for_same_yes_label(self) -> None:
        now = datetime.now(UTC)
        left = [MarketText("poly", "1", "Will BTC be above 75000?", now, yes_label="YES")]
        right = [MarketText("predict", "2", "BTC above 75000", now + timedelta(minutes=10), yes_label="YES")]

        matches = SemanticMarketMatcher(min_similarity=0.5).match(left, right)

        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].left_side, BinarySide.YES)
        self.assertEqual(matches[0].right_side, BinarySide.NO)

    def test_matcher_finds_exactly_what_comparing_every_pair_in_full_finds(self) -> None:
        # The matcher skips the full comparison when SequenceMatcher's cheap upper
        # bounds already fall short of the floor, and caches normalized titles.
        # Neither may change a single answer: same pairs, same sides, same scores.
        now = datetime(2026, 10, 6, tzinfo=UTC)
        teams = ("Le Mans FC", "SC Freiburg", "Arsenal", "Chelsea", "Bitcoin", "Ethereum", "Lakers", "Celtics")
        shapes = (
            "Will {a} beat {b}?",
            "{a} vs {b}: total goals over 2.5?",
            "Will {a} win on October {d}?",
            "{a} leading at halftime?",
            "Will {a} be above ${n} on October {d}?",
        )
        titles = [
            shape.format(a=a, b=b, d=day, n=day * 1000)
            for shape in shapes
            for a in teams
            for b in teams
            if a != b
            for day in (5, 6)
        ]
        left = [
            MarketText("poly", f"l{index}", f"{title} Yes", now + timedelta(minutes=index % 50))
            for index, title in enumerate(titles[::7])
        ]
        right = [
            MarketText("myriad", f"r{index}", title, now + timedelta(minutes=index % 40), yes_label="Yes")
            for index, title in enumerate(titles[::3])
        ]

        for floor in (0.5, 0.78, 0.85):
            with self.subTest(floor=floor):
                matcher = SemanticMarketMatcher(min_similarity=floor, expiry_window_seconds=1800)
                expected = _match_by_comparing_every_pair_in_full(left, right, floor, 1800)
                actual = [
                    (pair.left.market_id, pair.right.market_id, pair.right_side, pair.similarity)
                    for pair in matcher.match(left, right)
                ]
                self.assertEqual(actual, expected)
                self.assertTrue(expected)  # the comparison means something only if pairs match


def _match_by_comparing_every_pair_in_full(
    left_markets: list[MarketText], right_markets: list[MarketText], floor: float, window_seconds: int
) -> list[tuple[str, str, BinarySide, float]]:
    matches = []
    for left in left_markets:
        best: tuple[str, str, BinarySide, float] | None = None
        for right in right_markets:
            if abs((left.expires_at - right.expires_at).total_seconds()) > window_seconds:
                continue
            similarity = text_similarity(left.title, right.title)
            if similarity < floor:
                continue
            side = BinarySide.NO if text_similarity(left.yes_label, right.yes_label) >= 0.85 else BinarySide.YES
            if best is None or similarity > best[3]:
                best = (left.market_id, right.market_id, side, similarity)
        if best is not None:
            matches.append(best)
    return matches


if __name__ == "__main__":
    unittest.main()
