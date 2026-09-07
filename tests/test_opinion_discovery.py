import unittest
from datetime import UTC, datetime, timedelta

from arbitrage_engine.matcher import MarketText
from arbitrage_engine.models import BinarySide, MarketSpec
from arbitrage_engine.opinion_discovery import (
    _market_specs_from_text,
    _market_text,
    _opinion_token_id,
    _resolve_market_specs,
    _scan_all_market_texts,
)

YES_TOKEN = "yes-token-id"
NO_TOKEN = "no-token-id"
CUTOFF_AT = 1_767_225_600  # 2026-01-01T00:00:00Z


def make_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "marketId": 813,
        "marketTitle": "Will BTC close above $100k on Dec 31?",
        "status": 2,
        "marketType": 0,
        "yesLabel": "Yes",
        "noLabel": "No",
        "yesTokenId": YES_TOKEN,
        "noTokenId": NO_TOKEN,
        "conditionId": "0xcondition",
        "cutoffAt": CUTOFF_AT,
        "volume": "125000",
        "labels": ["Crypto"],
        "slug": "btc-100k",
    }
    payload.update(overrides)
    return payload


class OpinionMarketTextTests(unittest.TestCase):
    def test_binary_activated_market_is_parsed(self) -> None:
        text = _market_text(make_payload())

        assert text is not None
        self.assertEqual(text.platform, "opinion")
        self.assertEqual(text.market_id, "813")
        self.assertEqual(text.expires_at, datetime.fromtimestamp(CUTOFF_AT, tz=UTC))
        self.assertEqual(text.condition_id, "0xcondition")
        self.assertEqual(text.volume_usd, 125_000.0)
        self.assertEqual(text.public_url, "https://app.opinion.trade/market/btc-100k")

    def test_categorical_and_unresolved_markets_are_skipped(self) -> None:
        self.assertIsNone(_market_text(make_payload(marketType=1)))
        self.assertIsNone(_market_text(make_payload(status=4)))

    def test_market_without_both_outcome_tokens_is_skipped(self) -> None:
        self.assertIsNone(_market_text(make_payload(noTokenId=None)))

    def test_millisecond_cutoff_is_normalized_to_seconds(self) -> None:
        text = _market_text(make_payload(cutoffAt=CUTOFF_AT * 1_000))

        assert text is not None
        self.assertEqual(text.expires_at, datetime.fromtimestamp(CUTOFF_AT, tz=UTC))

    def test_category_filter_keeps_only_requested_categories(self) -> None:
        payloads = [make_payload(), make_payload(marketId=814, labels=["Sports"])]

        crypto_only = _scan_all_market_texts(payloads, {"finance"})
        unfiltered = _scan_all_market_texts(payloads, set())

        self.assertEqual([text.market_id for text in crypto_only], ["813"])
        self.assertEqual(len(unfiltered), 2)

    def test_execution_tokens_pair_each_outcome_with_its_market(self) -> None:
        text = _market_text(make_payload())

        assert text is not None
        self.assertEqual(_opinion_token_id(text, BinarySide.YES), f"813:{YES_TOKEN}")
        self.assertEqual(_opinion_token_id(text, BinarySide.NO), f"813:{NO_TOKEN}")


class OpinionSeedSpecTests(unittest.TestCase):
    def test_seed_specs_hedge_each_polymarket_outcome_with_the_opposite_token(self) -> None:
        text = _market_text(make_payload())
        assert text is not None

        specs = _market_specs_from_text(text)

        self.assertEqual(len(specs), 2)
        for spec in specs:
            self.assertEqual(spec.venue_b_label, "Opinion")
            self.assertEqual(spec.predict_fun_market_id, "813")
            self.assertNotEqual(spec.polymarket_side, spec.predict_fun_side)
        self.assertEqual(specs[0].predict_fun_token_id, f"813:{NO_TOKEN}")
        self.assertEqual(specs[1].predict_fun_token_id, f"813:{YES_TOKEN}")

    def test_seed_specs_are_empty_without_usable_outcome_tokens(self) -> None:
        text = MarketText(
            platform="opinion",
            market_id="813",
            title="Missing tokens",
            expires_at=datetime.now(UTC) + timedelta(hours=2),
        )

        self.assertEqual(_market_specs_from_text(text), [])


class OpinionResolveTests(unittest.TestCase):
    def _candidate(self) -> MarketText:
        text = _market_text(make_payload())
        assert text is not None
        return text

    def test_condition_id_match_hedges_the_opposite_polymarket_outcome(self) -> None:
        candidate = self._candidate()
        markets = [
            MarketSpec(
                symbol="Will BTC close above $100k on Dec 31?",
                target_label=side.value,
                polymarket_token_id=f"poly-{side.value.lower()}",
                polymarket_side=side,
                predict_fun_token_id="",
                predict_fun_side=BinarySide.NO,
                condition_id="0xcondition",
                expires_at=candidate.expires_at,
            )
            for side in (BinarySide.YES, BinarySide.NO)
        ]

        resolved = _resolve_market_specs(markets, [candidate], False)

        self.assertEqual(
            [market.predict_fun_side for market in resolved],
            [BinarySide.NO, BinarySide.YES],
        )
        self.assertEqual(
            [market.predict_fun_token_id for market in resolved],
            [f"813:{NO_TOKEN}", f"813:{YES_TOKEN}"],
        )
        self.assertEqual({market.venue_b_label for market in resolved}, {"Opinion"})
        self.assertEqual({market.mapping_strategy for market in resolved}, {"exact_id"})

    def test_already_resolved_opinion_market_is_left_untouched(self) -> None:
        candidate = self._candidate()
        market = MarketSpec(
            symbol="Will BTC close above $100k on Dec 31?",
            target_label="YES",
            polymarket_token_id="poly-yes",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id=f"813:{NO_TOKEN}",
            predict_fun_side=BinarySide.NO,
            venue_b_label="Opinion",
            expires_at=candidate.expires_at,
        )

        resolved = _resolve_market_specs([market], [candidate], False)

        self.assertIs(resolved[0], market)

    def test_market_without_expiry_is_not_matched(self) -> None:
        market = MarketSpec(
            symbol="Will BTC close above $100k on Dec 31?",
            target_label="YES",
            polymarket_token_id="poly-yes",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="",
            predict_fun_side=BinarySide.NO,
        )

        resolved = _resolve_market_specs([market], [self._candidate()], False)

        self.assertEqual(resolved[0].venue_b_label, "Predict.fun")
        self.assertEqual(resolved[0].predict_fun_token_id, "")

    def test_unrelated_market_is_returned_unresolved(self) -> None:
        candidate = self._candidate()
        market = MarketSpec(
            symbol="Will the Lakers win the title?",
            target_label="YES",
            polymarket_token_id="poly-yes",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="",
            predict_fun_side=BinarySide.NO,
            expires_at=candidate.expires_at,
        )

        resolved = _resolve_market_specs([market], [candidate], False)

        self.assertEqual(resolved[0].predict_fun_token_id, "")


if __name__ == "__main__":
    unittest.main()
