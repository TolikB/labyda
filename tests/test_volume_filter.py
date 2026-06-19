import unittest
from types import SimpleNamespace

from arbitrage_engine.main import _deduplicate_markets, _filter_markets_by_volume, _maximum_market_volume
from arbitrage_engine.models import BinarySide, MarketSpec


def _market(symbol: str, **volumes: float | None) -> MarketSpec:
    return MarketSpec(
        symbol=symbol,
        target_label=symbol,
        polymarket_token_id="poly",
        polymarket_side=BinarySide.YES,
        predict_fun_token_id="hedge",
        predict_fun_side=BinarySide.NO,
        **volumes,
    )


class VolumeFilterTests(unittest.TestCase):
    def test_duplicate_catalog_entries_are_merged_by_polymarket_outcome(self) -> None:
        predict = _market("same", predict_fun_volume_usd=30_000)
        myriad = MarketSpec(
            symbol="same",
            target_label="same",
            polymarket_token_id="poly",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="",
            predict_fun_side=BinarySide.NO,
            myriad_market_id="123",
            myriad_side=BinarySide.NO,
            myriad_volume_usd=40_000,
        )

        result = _deduplicate_markets([predict, myriad])

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].predict_fun_token_id, "hedge")
        self.assertEqual(result[0].myriad_market_id, "123")

    def test_conflicting_cross_venue_mapping_is_rejected(self) -> None:
        first = _market("same")
        second = MarketSpec(
            symbol="same",
            target_label="same",
            polymarket_token_id="poly",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="different",
            predict_fun_side=BinarySide.NO,
        )

        self.assertEqual(_deduplicate_markets([first, second]), [])

    def test_uses_largest_available_cross_venue_volume(self) -> None:
        market = _market("kept", polymarket_volume_usd=10_000, myriad_volume_usd=30_000)

        self.assertEqual(_maximum_market_volume(market), 30_000)

    def test_drops_unknown_and_low_volume_markets(self) -> None:
        markets = [
            _market("unknown"),
            _market("low", polymarket_volume_usd=24_999),
            _market("kept", predict_fun_volume_usd=25_000),
        ]

        filtered = _filter_markets_by_volume(markets, SimpleNamespace(min_market_volume_usd=25_000))  # type: ignore[arg-type]

        self.assertEqual([market.symbol for market in filtered], ["kept"])


if __name__ == "__main__":
    unittest.main()
