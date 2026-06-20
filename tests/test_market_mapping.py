import unittest
from dataclasses import replace
from datetime import datetime, timezone

from arbitrage_engine.market_mapping import (
    filter_markets_for_categories,
    is_live_mapping_eligible,
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

    def test_rules_fingerprint_is_canonical(self) -> None:
        cutoff = datetime(2026, 6, 20, 12, tzinfo=timezone.utc)
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

    def test_client_order_id_is_uuid7(self) -> None:
        generated = uuid7()

        self.assertEqual(generated.version, 7)
        self.assertEqual(generated.variant, "specified in RFC 4122")


if __name__ == "__main__":
    unittest.main()
