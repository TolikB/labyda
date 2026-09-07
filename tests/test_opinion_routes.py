import json
import tempfile
import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

from arbitrage_engine.config import RouteConfig, load_config, validate_config
from arbitrage_engine.database import _active_venues_for_routes, _mapping_route_pairs, _route_name
from arbitrage_engine.main import _build_route_market_snapshot, _route_scoped_persistence_candidates
from arbitrage_engine.market_mapping import route_key
from arbitrage_engine.models import (
    EXECUTION_ROUTES,
    OPINION_ROUTES,
    BinarySide,
    ExecutionMode,
    MarketSpec,
    execution_route_for_market,
    market_supports_execution_route,
    myriad_execution_token_for_route,
    route_execution_sides_are_complementary,
    route_venue_labels,
)
from arbitrage_engine.production_audit import ROUTE_NAMES, enabled_execution_venues

EXPIRES_AT = datetime.now(UTC) + timedelta(hours=2)


def polymarket_opinion_market(
    polymarket_side: BinarySide = BinarySide.YES,
    opinion_side: BinarySide = BinarySide.NO,
) -> MarketSpec:
    return MarketSpec(
        symbol="Will Team A win?",
        target_label=polymarket_side.value,
        polymarket_token_id=f"poly-{polymarket_side.value.lower()}",
        polymarket_side=polymarket_side,
        predict_fun_token_id=f"813:opinion-{opinion_side.value.lower()}",
        predict_fun_side=opinion_side,
        venue_b_label="Opinion",
        polymarket_market_id="poly-market",
        predict_fun_market_id="813",
        expires_at=EXPIRES_AT,
    )


class OpinionRouteModelTests(unittest.TestCase):
    def test_every_opinion_route_is_registered_with_venue_labels(self) -> None:
        for route in sorted(OPINION_ROUTES):
            self.assertIn(route, EXECUTION_ROUTES)
            first, second = route_venue_labels(route)
            self.assertIn("Opinion", (first, second))

    def test_route_names_match_the_venue_label_pairs(self) -> None:
        self.assertEqual(route_venue_labels("polymarket_opinion"), ("Polymarket", "Opinion"))
        self.assertEqual(route_venue_labels("predict_opinion"), ("Predict.fun", "Opinion"))
        self.assertEqual(route_venue_labels("sx_opinion"), ("SX Bet", "Opinion"))
        self.assertEqual(route_venue_labels("opinion_myriad"), ("Opinion", "Myriad"))

    def test_unknown_route_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            route_venue_labels("polymarket_unknown")

    def test_execution_route_is_derived_from_the_venue_labels(self) -> None:
        market = polymarket_opinion_market()

        self.assertEqual(execution_route_for_market(market), "polymarket_opinion")
        self.assertEqual(
            execution_route_for_market(replace(market, venue_a_label="Predict.fun")),
            "predict_opinion",
        )
        self.assertEqual(
            execution_route_for_market(replace(market, venue_a_label="SX Bet")),
            "sx_opinion",
        )
        self.assertEqual(
            execution_route_for_market(
                replace(market, venue_a_label="Opinion", venue_b_label="Myriad")
            ),
            "opinion_myriad",
        )

    def test_existing_routes_keep_their_identity(self) -> None:
        self.assertEqual(route_venue_labels("polymarket_predict"), ("Polymarket", "Predict.fun"))
        self.assertEqual(route_venue_labels("sx_myriad"), ("SX Bet", "Myriad"))

    def test_generic_slot_routes_require_both_leg_tokens(self) -> None:
        market = polymarket_opinion_market()

        self.assertTrue(market_supports_execution_route(market, "polymarket_opinion"))
        self.assertFalse(
            market_supports_execution_route(replace(market, predict_fun_token_id=""), "polymarket_opinion")
        )
        self.assertFalse(market_supports_execution_route(market, "predict_opinion"))

    def test_complementary_sides_are_required_for_a_hedge(self) -> None:
        market = polymarket_opinion_market()

        self.assertTrue(route_execution_sides_are_complementary(market, "polymarket_opinion"))
        self.assertFalse(
            route_execution_sides_are_complementary(
                replace(market, predict_fun_side=BinarySide.YES), "polymarket_opinion"
            )
        )

    def test_opinion_myriad_derives_the_opposite_myriad_token(self) -> None:
        market = replace(
            polymarket_opinion_market(),
            myriad_market_id="1335",
            myriad_side=BinarySide.NO,
        )

        self.assertTrue(market_supports_execution_route(market, "opinion_myriad"))
        self.assertTrue(route_execution_sides_are_complementary(market, "opinion_myriad"))
        self.assertEqual(myriad_execution_token_for_route(market, "opinion_myriad"), "1335:YES")

    def test_opinion_myriad_rejects_a_same_side_hedge(self) -> None:
        market = replace(
            polymarket_opinion_market(),
            myriad_market_id="1335",
            myriad_side=BinarySide.YES,
        )

        self.assertFalse(route_execution_sides_are_complementary(market, "opinion_myriad"))

    def test_route_key_and_persistence_names_agree(self) -> None:
        for route in sorted(OPINION_ROUTES):
            first, second = route_venue_labels(route)
            self.assertEqual(route_key(first, second), route)
            self.assertEqual(_route_name(first, second), route)


class OpinionRouteConfigTests(unittest.TestCase):
    def _write_config(self, payload: dict[str, object]) -> Path:
        directory = tempfile.mkdtemp()
        path = Path(directory) / "config.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    def test_opinion_routes_default_to_disabled(self) -> None:
        routes = RouteConfig()

        for route in sorted(OPINION_ROUTES):
            self.assertFalse(getattr(routes, route))
        self.assertNotIn("polymarket_opinion", routes.enabled_names())

    def test_enabled_names_follow_the_canonical_route_order(self) -> None:
        routes = RouteConfig(polymarket_opinion=True, opinion_myriad=True)

        self.assertEqual(routes.enabled_names()[-2:], ("polymarket_opinion", "opinion_myriad"))

    def test_config_parses_opinion_routes_and_venue_block(self) -> None:
        path = self._write_config(
            {
                "enable_opinion": True,
                "routes": {"polymarket_opinion": True},
                "opinion": {"enabled": True, "taker_fee_rate_bps": 250, "minimum_notional_usd": 5.0},
            }
        )

        config = load_config(path)

        self.assertTrue(config.enable_opinion)
        self.assertTrue(config.routes.polymarket_opinion)
        self.assertTrue(config.opinion.enabled)
        self.assertEqual(config.opinion.taker_fee_rate_bps, 250)
        self.assertEqual(config.opinion.api_base_url, "https://openapi.opinion.trade/openapi")
        self.assertEqual(config.opinion.ws_url, "wss://ws.opinion.trade")

    def test_invalid_opinion_route_boolean_is_rejected(self) -> None:
        path = self._write_config({"routes": {"polymarket_opinion": "yes"}})

        with self.assertRaises(ValueError):
            load_config(path)

    def test_opinion_only_hedge_venue_satisfies_the_active_venue_requirement(self) -> None:
        path = self._write_config(
            {
                "enable_opinion": True,
                "routes": {
                    "polymarket_myriad": False,
                    "polymarket_predict": False,
                    "predict_myriad": False,
                    "polymarket_opinion": True,
                },
                "opinion": {"enabled": True},
                "myriad_markets": {"enabled": False},
            }
        )

        validate_config(load_config(path))

    def test_funded_opinion_route_requires_the_venue_to_be_enabled(self) -> None:
        path = self._write_config(
            {
                "execution_mode": "canary",
                "live_trading_confirmed": True,
                "database_url": "postgresql+asyncpg://user:pass@localhost:5432/db",
                "runtime_instance_id": "test",
                "routes": {
                    "polymarket_myriad": False,
                    "polymarket_predict": False,
                    "predict_myriad": False,
                    "polymarket_opinion": True,
                },
                "funded_routes": {"polymarket_opinion": True},
                "opinion": {"enabled": False},
                "myriad_markets": {"enabled": True},
            }
        )

        with self.assertRaises(ValueError) as caught:
            validate_config(load_config(path))

        self.assertIn("funded Opinion routes require", str(caught.exception))

    def test_invalid_opinion_venue_settings_are_reported(self) -> None:
        path = self._write_config(
            {
                "enable_opinion": True,
                "routes": {"polymarket_opinion": True},
                "opinion": {
                    "enabled": True,
                    "taker_fee_rate_bps": 20_000,
                    "minimum_notional_usd": 0,
                    "market_page_limit": 50,
                },
            }
        )

        with self.assertRaises(ValueError) as caught:
            validate_config(load_config(path))

        message = str(caught.exception)
        self.assertIn("opinion.taker_fee_rate_bps", message)
        self.assertIn("opinion.minimum_notional_usd", message)
        self.assertIn("opinion.market_page_limit", message)


class OpinionRouteSnapshotTests(unittest.TestCase):
    def _polymarket_anchored(self, venue: str, token: str, side: BinarySide) -> MarketSpec:
        return MarketSpec(
            symbol="Will Team A win?",
            target_label=side.value,
            polymarket_token_id=f"poly-{side.value.lower()}",
            polymarket_side=side,
            predict_fun_token_id=token,
            predict_fun_side=side,
            polymarket_market_id="poly-market",
            predict_fun_market_id=f"{venue.lower()}-market",
            venue_b_label=venue,
            expires_at=EXPIRES_AT,
        )

    def test_snapshot_synthesizes_predict_opinion_from_both_families(self) -> None:
        predict = self._polymarket_anchored("Predict.fun", "predict-no", BinarySide.NO)
        opinion = self._polymarket_anchored("Opinion", "813:opinion-yes", BinarySide.YES)

        snapshot = _build_route_market_snapshot([predict, opinion])

        route = next(
            market
            for market in snapshot
            if market.venue_a_label == "Predict.fun" and market.venue_b_label == "Opinion"
        )
        self.assertEqual(route.polymarket_token_id, "predict-no")
        self.assertIs(route.polymarket_side, BinarySide.NO)
        self.assertEqual(route.predict_fun_token_id, "813:opinion-yes")
        self.assertIs(route.predict_fun_side, BinarySide.YES)
        self.assertTrue(market_supports_execution_route(route, "predict_opinion"))
        self.assertTrue(route_execution_sides_are_complementary(route, "predict_opinion"))

    def test_snapshot_synthesizes_sx_opinion_from_both_families(self) -> None:
        sx = self._polymarket_anchored("SX Bet", "sx-market:NO", BinarySide.NO)
        opinion = self._polymarket_anchored("Opinion", "813:opinion-yes", BinarySide.YES)

        snapshot = _build_route_market_snapshot([sx, opinion])

        route = next(
            market
            for market in snapshot
            if market.venue_a_label == "SX Bet" and market.venue_b_label == "Opinion"
        )
        self.assertEqual(execution_route_for_market(route), "sx_opinion")
        self.assertTrue(route_execution_sides_are_complementary(route, "sx_opinion"))

    def test_snapshot_keeps_the_direct_polymarket_opinion_route(self) -> None:
        opinion = self._polymarket_anchored("Opinion", "813:opinion-no", BinarySide.NO)

        snapshot = _build_route_market_snapshot([opinion])

        routes = {execution_route_for_market(market) for market in snapshot}
        self.assertIn("polymarket_opinion", routes)

    def test_same_side_families_do_not_synthesize_a_route(self) -> None:
        predict = self._polymarket_anchored("Predict.fun", "predict-yes", BinarySide.YES)
        opinion = self._polymarket_anchored("Opinion", "813:opinion-yes", BinarySide.YES)

        snapshot = _build_route_market_snapshot([predict, opinion])

        self.assertFalse(
            any(
                market.venue_a_label == "Predict.fun" and market.venue_b_label == "Opinion"
                for market in snapshot
            )
        )

    def test_predict_sx_synthesis_still_works_alongside_opinion(self) -> None:
        predict = self._polymarket_anchored("Predict.fun", "predict-no", BinarySide.NO)
        sx = self._polymarket_anchored("SX Bet", "sx-market:YES", BinarySide.YES)

        snapshot = _build_route_market_snapshot([predict, sx])

        self.assertTrue(
            any(
                market.venue_a_label == "Predict.fun" and market.venue_b_label == "SX Bet"
                for market in snapshot
            )
        )

    def test_opinion_myriad_persistence_projection_uses_the_opinion_first_leg(self) -> None:
        market = replace(
            polymarket_opinion_market(),
            myriad_market_id="1335",
            myriad_side=BinarySide.NO,
        )

        projections = _route_scoped_persistence_candidates([market], ("opinion_myriad",))

        self.assertEqual(len(projections), 1)
        projection = projections[0]
        self.assertEqual(projection.venue_a_label, "Opinion")
        self.assertEqual(projection.venue_b_label, "Myriad")
        self.assertEqual(projection.polymarket_token_id, market.predict_fun_token_id)
        self.assertEqual(projection.polymarket_market_id, "813")
        self.assertIsNone(projection.condition_id)

    def test_generic_slot_persistence_projection_drops_myriad_metadata(self) -> None:
        market = replace(
            polymarket_opinion_market(),
            myriad_market_id="1335",
            myriad_url="https://myriad.example/1335",
        )

        projections = _route_scoped_persistence_candidates([market], ("polymarket_opinion",))

        self.assertEqual(len(projections), 1)
        self.assertIsNone(projections[0].myriad_market_id)
        self.assertIsNone(projections[0].myriad_url)


class OpinionRouteAuditTests(unittest.TestCase):
    def test_audit_route_names_cover_every_execution_route(self) -> None:
        self.assertEqual(tuple(ROUTE_NAMES), EXECUTION_ROUTES)

    def test_funded_opinion_routes_require_the_opinion_venue(self) -> None:
        from arbitrage_engine.config import AppConfig

        path = tempfile.mkdtemp()
        config_path = Path(path) / "config.json"
        config_path.write_text(
            json.dumps(
                {
                    "enable_opinion": True,
                    "routes": {
                        "polymarket_myriad": False,
                        "polymarket_predict": False,
                        "predict_myriad": False,
                        "polymarket_opinion": True,
                    },
                    "funded_routes": {"polymarket_opinion": True},
                    "opinion": {"enabled": True},
                }
            ),
            encoding="utf-8",
        )
        config: AppConfig = load_config(config_path)

        self.assertEqual(enabled_execution_venues(config), ("Polymarket", "Opinion"))

    def test_persistence_venue_pairs_include_opinion(self) -> None:
        self.assertEqual(
            _active_venues_for_routes(["polymarket_opinion", "opinion_myriad"]),
            ("Myriad", "Opinion", "Polymarket"),
        )
        self.assertEqual(
            sorted(_mapping_route_pairs(["sx_opinion"])),
            [("Opinion", "SX Bet"), ("SX Bet", "Opinion")],
        )

    def test_unknown_routes_are_ignored_by_the_persistence_helpers(self) -> None:
        self.assertEqual(_active_venues_for_routes(["not_a_route"]), ())
        self.assertEqual(_mapping_route_pairs(["not_a_route"]), set())


class OpinionRouteLatencyTests(unittest.TestCase):
    def test_opinion_routes_use_the_opinion_fill_timeout(self) -> None:
        from arbitrage_engine.engine import ArbitrageEngine

        directory = tempfile.mkdtemp()
        path = Path(directory) / "config.json"
        path.write_text(
            json.dumps(
                {
                    "enable_opinion": True,
                    "routes": {"polymarket_opinion": True},
                    "opinion": {"enabled": True},
                    "opinion_fill_timeout_ms": 6_000,
                    "polymarket_fill_timeout_ms": 500,
                }
            ),
            encoding="utf-8",
        )
        config = load_config(path)
        engine = ArbitrageEngine(config, polymarket=object(), predict_fun=None, execution=None)  # type: ignore[arg-type]

        horizon = engine._execution_latency_horizon_seconds("polymarket_opinion")  # noqa: SLF001

        self.assertAlmostEqual(horizon, 6.5)

    def test_opinion_venue_fee_uses_the_curve_peak(self) -> None:
        from arbitrage_engine.engine import ArbitrageEngine

        directory = tempfile.mkdtemp()
        path = Path(directory) / "config.json"
        path.write_text(
            json.dumps({"opinion": {"enabled": True, "taker_fee_rate_bps": 400}}),
            encoding="utf-8",
        )
        config = load_config(path)
        engine = ArbitrageEngine(config, polymarket=object(), predict_fun=None, execution=None)  # type: ignore[arg-type]

        fee = engine._venue_fee_pct("Opinion", polymarket_opinion_market())  # noqa: SLF001

        self.assertAlmostEqual(fee, 0.01)


class OpinionExecutionModeTests(unittest.TestCase):
    def test_shadow_mode_does_not_require_verified_mappings_for_new_routes(self) -> None:
        from arbitrage_engine.market_mapping import is_live_mapping_eligible

        market = polymarket_opinion_market()

        self.assertTrue(is_live_mapping_eligible(market, ExecutionMode.SHADOW, "polymarket_opinion"))
        self.assertFalse(is_live_mapping_eligible(market, ExecutionMode.CANARY, "polymarket_opinion"))


if __name__ == "__main__":
    unittest.main()
