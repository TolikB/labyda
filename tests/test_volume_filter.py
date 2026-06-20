import unittest
from dataclasses import replace
from types import SimpleNamespace

from arbitrage_engine.discovery_lifecycle import DiscoveryDiagnostics, DiscoveryResult
from arbitrage_engine.main import (
    _assert_once_discovery_ready,
    _deduplicate_markets,
    _filter_markets_by_volume,
    _jittered_retry_delay,
    _maximum_market_volume,
    _missing_discovery_routes,
    _next_discovery_retry_delay,
    _should_retry_discovery,
    _verified_active_markets,
)
from arbitrage_engine.models import BinarySide, ExecutionMode, MappingStatus, MarketSpec


def _market(
    symbol: str,
    *,
    polymarket_volume_usd: float | None = None,
    predict_fun_volume_usd: float | None = None,
    myriad_volume_usd: float | None = None,
) -> MarketSpec:
    return MarketSpec(
        symbol=symbol,
        target_label=symbol,
        polymarket_token_id="poly",
        polymarket_side=BinarySide.YES,
        predict_fun_token_id="hedge",
        predict_fun_side=BinarySide.NO,
        polymarket_volume_usd=polymarket_volume_usd,
        predict_fun_volume_usd=predict_fun_volume_usd,
        myriad_volume_usd=myriad_volume_usd,
    )


class VolumeFilterTests(unittest.TestCase):
    def test_once_fails_when_no_complete_route_is_discovered(self) -> None:
        result = DiscoveryResult(
            (),
            ("polymarket_myriad",),
            DiscoveryDiagnostics(stages=(("tradable", 0),), rejection_reasons=(("no_safe_match", 5),)),
        )

        with self.assertRaisesRegex(RuntimeError, "no complete tradable route set"):
            _assert_once_discovery_ready(result)

    def test_once_accepts_nonempty_complete_route_set(self) -> None:
        _assert_once_discovery_ready(DiscoveryResult((_market("ready"),), ()))

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

    def test_scan_all_retries_until_every_enabled_route_is_available(self) -> None:
        market = replace(_market("candidate"), myriad_market_id="myriad")
        config = SimpleNamespace(
            scan_all=True,
            execution_mode=ExecutionMode.SHADOW,
            routes=SimpleNamespace(
                polymarket_myriad=True,
                polymarket_predict=True,
                predict_myriad=True,
            ),
            markets=[market],
        )

        missing = _missing_discovery_routes(config)  # type: ignore[arg-type]

        self.assertEqual(missing, [])
        self.assertFalse(_should_retry_discovery(config, False, missing))  # type: ignore[arg-type]

    def test_live_scan_all_waits_for_verified_route_mapping(self) -> None:
        candidate = replace(_market("candidate"), myriad_market_id="myriad")
        verified = replace(
            candidate,
            mapping_status=MappingStatus.VERIFIED,
            verified_routes=frozenset({"polymarket_myriad"}),
        )
        config = SimpleNamespace(
            scan_all=True,
            execution_mode=ExecutionMode.CANARY,
            routes=SimpleNamespace(
                polymarket_myriad=True,
                polymarket_predict=False,
                predict_myriad=False,
            ),
            markets=[candidate],
        )

        missing = _missing_discovery_routes(config)  # type: ignore[arg-type]

        self.assertEqual(missing, ["polymarket_myriad"])
        self.assertTrue(_should_retry_discovery(config, False, missing))  # type: ignore[arg-type]
        self.assertFalse(_should_retry_discovery(config, True, missing))  # type: ignore[arg-type]
        self.assertEqual(_verified_active_markets(config), [])  # type: ignore[arg-type]
        config.markets = [verified]
        self.assertEqual(_missing_discovery_routes(config), [])  # type: ignore[arg-type]
        self.assertEqual(_verified_active_markets(config), [verified])  # type: ignore[arg-type]

    def test_discovery_retry_uses_bounded_schedule_and_jitter(self) -> None:
        delays = [5.0]
        for _ in range(7):
            delays.append(_next_discovery_retry_delay(delays[-1]))

        self.assertEqual(delays, [5.0, 10.0, 20.0, 40.0, 60.0, 120.0, 240.0, 300.0])
        self.assertEqual(_jittered_retry_delay(100.0, 0.0), 80.0)
        self.assertEqual(_jittered_retry_delay(100.0, 0.5), 100.0)
        self.assertAlmostEqual(_jittered_retry_delay(100.0, 1.0), 120.0)


if __name__ == "__main__":
    unittest.main()
