import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta

from arbitrage_engine.market_mapping import (
    filter_markets_for_categories,
    filter_markets_for_launch_horizon,
    is_live_mapping_eligible,
    normalize_category,
    route_key,
    rules_fingerprint,
)
from arbitrage_engine.models import BinarySide, ExecutionMode, MappingStatus, MarketSpec
from arbitrage_engine.utils.ids import uuid7


def _market() -> MarketSpec:
    return MarketSpec(
        symbol="BTC-USD",
        target_label="Bitcoin above 100k",
        polymarket_token_id="poly",
        polymarket_side=BinarySide.YES,
        predict_fun_token_id="predict",
        predict_fun_side=BinarySide.NO,
        category="finance",
        mapping_status=MappingStatus.VERIFIED,
        rules_fingerprint="fingerprint",
        resolution_source="Coinbase BTC/USD close",
        outcome_semantics="YES if close is strictly above 100000 USD",
        verified_routes=frozenset({"polymarket_predict"}),
    )


class MarketMappingTests(unittest.TestCase):
    def test_sports_taxonomy_aliases_share_the_sports_horizon(self) -> None:
        now = datetime(2026, 7, 15, 12, tzinfo=UTC)
        football = replace(_market(), category="football", cutoff_at=now + timedelta(hours=199))
        esports = replace(_market(), category="esports", cutoff_at=now + timedelta(hours=201))

        result = filter_markets_for_launch_horizon(
            [football, esports],
            ["sports"],
            sports_horizon_hours=200,
            crypto_horizon_hours=200,
            now=now,
        )

        self.assertEqual(normalize_category("football"), "sports")
        self.assertEqual(normalize_category("esports"), "sports")
        self.assertEqual(result, [football])

    def test_unknown_category_is_shadow_only(self) -> None:
        market = replace(_market(), category=None)

        self.assertEqual(filter_markets_for_categories([market], ["finance"], ExecutionMode.SHADOW), [market])
        self.assertEqual(filter_markets_for_categories([market], ["finance"], ExecutionMode.CANARY), [])

    def test_live_route_requires_verified_mapping_and_rules(self) -> None:
        market = _market()

        self.assertTrue(is_live_mapping_eligible(market, ExecutionMode.CANARY, "polymarket_predict"))
        self.assertFalse(is_live_mapping_eligible(market, ExecutionMode.CANARY, "polymarket_myriad"))
        self.assertFalse(
            is_live_mapping_eligible(
                replace(market, mapping_status=MappingStatus.STALE),
                ExecutionMode.CANARY,
                "polymarket_predict",
            )
        )

    def test_launch_horizon_keeps_only_near_term_sports_and_crypto(self) -> None:
        now = datetime(2026, 7, 15, 12, tzinfo=UTC)
        sports_near = replace(
            _market(),
            symbol="Live football",
            target_label="Team A wins",
            category="sports",
            cutoff_at=now + timedelta(hours=47),
        )
        sports_far = replace(sports_near, symbol="World Cup outright", cutoff_at=now + timedelta(hours=49))
        crypto_near = replace(
            _market(),
            category="crypto",
            cutoff_at=now + timedelta(hours=23),
        )
        crypto_far = replace(crypto_near, cutoff_at=now + timedelta(hours=25))
        macro = replace(
            _market(),
            symbol="FOMC rate decision",
            target_label="Fed cuts rates",
            category="finance",
            cutoff_at=now + timedelta(hours=12),
        )

        result = filter_markets_for_launch_horizon(
            [sports_near, sports_far, crypto_near, crypto_far, macro],
            ["sports", "crypto"],
            sports_horizon_hours=48,
            crypto_horizon_hours=24,
            now=now,
        )

        self.assertEqual(result, [sports_near, crypto_near])

    def test_launch_horizon_rejects_expired_or_missing_cutoff_markets(self) -> None:
        now = datetime(2026, 7, 15, 12, tzinfo=UTC)
        expired = replace(_market(), category="sports", cutoff_at=now - timedelta(seconds=1))
        missing = replace(_market(), category="sports", cutoff_at=None, expires_at=None)

        result = filter_markets_for_launch_horizon(
            [expired, missing],
            ["sports"],
            sports_horizon_hours=48,
            crypto_horizon_hours=24,
            now=now,
        )

        self.assertEqual(result, [])

    def test_rules_fingerprint_is_canonical(self) -> None:
        cutoff = datetime(2026, 6, 20, 12, tzinfo=UTC)
        first = rules_fingerprint(
            title=" Bitcoin   Above 100k ",
            resolution_source="Coinbase BTC/USD Close",
            cutoff_at=cutoff,
            outcome_semantics="YES if close is above",
        )
        second = rules_fingerprint(
            title="bitcoin above 100k",
            resolution_source="coinbase btc/usd close",
            cutoff_at=cutoff,
            outcome_semantics="yes if close is above",
        )

        self.assertEqual(first, second)

    def test_route_key_supports_sx_bet_alias(self) -> None:
        self.assertEqual(route_key("Polymarket", "SX Bet"), "polymarket_sx")
        self.assertEqual(route_key("SX Bet", "Myriad"), "sx_myriad")
        self.assertEqual(route_key("Predict.fun", "SX Bet"), "predict_sx")

    def test_client_order_id_is_uuid7(self) -> None:
        generated = uuid7()

        self.assertEqual(generated.version, 7)
        self.assertEqual(generated.variant, "specified in RFC 4122")


if __name__ == "__main__":
    unittest.main()
