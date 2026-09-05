import json
import os
import tempfile
import unittest
from dataclasses import replace
from datetime import UTC
from pathlib import Path
from unittest.mock import patch

from arbitrage_engine.config import _parse_datetime, load_config, load_operator_env, validate_config
from arbitrage_engine.models import BinarySide, ExecutionMode, MappingStatus, MarketSpec


class ConfigTests(unittest.TestCase):
    def test_route_and_live_confirmation_strings_are_parsed_strictly(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "routes": {"polymarket_predict": "false"},
                        "funded_routes": {"polymarket_predict": "false"},
                        "live_trading_confirmed": "false",
                    }
                ),
                encoding="utf-8",
            )

            config = load_config(path)

            self.assertFalse(config.routes.polymarket_predict)
            assert config.funded_routes is not None
            self.assertFalse(config.funded_routes.polymarket_predict)
            self.assertFalse(config.live_trading_confirmed)

    def test_invalid_route_boolean_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(json.dumps({"routes": {"polymarket_predict": "no"}}), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "routes.polymarket_predict must be a boolean"):
                load_config(path)

    def test_funded_predict_route_requires_enabled_venue_and_api_key(self) -> None:
        base = load_config(Path(__file__).parents[1] / "config.example.json")
        funded = replace(
            base.routes,
            polymarket_myriad=False,
            polymarket_predict=True,
            predict_myriad=False,
            predict_sx=False,
            polymarket_sx=False,
            sx_myriad=False,
        )
        live = replace(
            base,
            execution_mode=ExecutionMode.CANARY,
            funded_routes=funded,
            database_url="postgresql+asyncpg://example.invalid/db",
            live_trading_confirmed=True,
            scan_all=True,
            market_horizon_filter_enabled=True,
            predict_fun=replace(base.predict_fun, api_key=None),
        )

        with self.assertRaisesRegex(ValueError, "PREDICT_FUN_API_KEY is required"):
            validate_config(live, require_verified_mappings=False)
        with self.assertRaisesRegex(ValueError, "funded Predict.fun routes require"):
            validate_config(
                replace(
                    live,
                    enable_predict_fun=False,
                    predict_fun=replace(base.predict_fun, api_key="catalog-key"),
                ),
                require_verified_mappings=False,
            )

    def test_canary_requires_nonempty_funded_subset_but_shadow_allows_empty(self) -> None:
        base = load_config(Path(__file__).parents[1] / "config.example.json")
        validate_config(base)

        canary = replace(
            base,
            execution_mode=ExecutionMode.CANARY,
            is_test=False,
            database_url="postgresql+asyncpg://example.invalid/db",
            live_trading_confirmed=True,
            scan_all=True,
            market_horizon_filter_enabled=True,
        )
        with self.assertRaisesRegex(ValueError, "at least one funded route must be enabled"):
            validate_config(canary, require_verified_mappings=False)

        funded_outside_discovery = replace(
            base.routes,
            polymarket_myriad=False,
            polymarket_predict=True,
        )
        with self.assertRaisesRegex(ValueError, "funded_routes must be a subset of routes"):
            validate_config(
                replace(canary, funded_routes=funded_outside_discovery),
                require_verified_mappings=False,
            )

    def test_route_validation_allows_cross_route_myriad_metadata(self) -> None:
        base = load_config(Path(__file__).parents[1] / "config.example.json")
        market = MarketSpec(
            symbol="Shared Predict and Myriad market",
            target_label="YES",
            polymarket_token_id="poly-yes",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="predict-no",
            predict_fun_side=BinarySide.NO,
            myriad_market_id="myriad-market",
            myriad_side=BinarySide.YES,
            venue_b_label="Predict.fun",
        )

        validate_config(
            replace(
                base,
                scan_all=True,
                markets=[market],
                routes=replace(
                    base.routes,
                    polymarket_myriad=True,
                    polymarket_predict=True,
                    predict_myriad=True,
                ),
            )
        )

    def test_route_validation_rejects_same_side_primary_myriad_pair(self) -> None:
        base = load_config(Path(__file__).parents[1] / "config.example.json")
        market = MarketSpec(
            symbol="Unsafe Polymarket and Myriad market",
            target_label="YES",
            polymarket_token_id="poly-yes",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="",
            predict_fun_side=BinarySide.NO,
            myriad_market_id="myriad-market",
            myriad_side=BinarySide.YES,
            venue_b_label="Myriad",
        )

        with self.assertRaisesRegex(
            ValueError,
            "execution orientation is inconsistent for route polymarket_myriad",
        ):
            validate_config(
                replace(
                    base,
                    scan_all=True,
                    markets=[market],
                    routes=replace(base.routes, predict_myriad=True),
                )
            )

    def test_route_validation_rejects_inconsistent_verified_cross_myriad_pair(self) -> None:
        base = load_config(Path(__file__).parents[1] / "config.example.json")
        market = MarketSpec(
            symbol="Unsafe Predict and Myriad market",
            target_label="YES",
            polymarket_token_id="poly-yes",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="predict-no",
            predict_fun_side=BinarySide.NO,
            myriad_market_id="myriad-market",
            myriad_side=BinarySide.YES,
            venue_b_label="Predict.fun",
            mapping_status=MappingStatus.VERIFIED,
            verified_routes=frozenset({"predict_myriad"}),
        )

        with self.assertRaisesRegex(
            ValueError,
            "execution orientation is inconsistent for route predict_myriad",
        ):
            validate_config(
                replace(
                    base,
                    scan_all=True,
                    markets=[market],
                    routes=replace(base.routes, predict_myriad=True),
                )
            )

    def test_config_repr_redacts_credentials_and_private_endpoints(self) -> None:
        secrets = {
            "telegram.bot_token": "test-only-telegram-secret",
            "database_url": "postgresql://test-only-db-secret",
            "polymarket.private_key": "test-only-poly-private",
            "polymarket.api_key": "test-only-poly-api",
            "polymarket.api_secret": "test-only-poly-signing",
            "polymarket.api_passphrase": "test-only-poly-passphrase",
            "polymarket.rpc_url": "https://test-only-poly-rpc",
            "predict_fun.private_key": "test-only-predict-private",
            "predict_fun.api_key": "test-only-predict-api",
            "predict_fun.rpc_url": "https://test-only-predict-rpc",
            "sx_bet.api_key": "test-only-sx-api",
            "sx_bet.private_key": "test-only-sx-private",
            "sx_bet.rpc_url": "https://test-only-sx-rpc",
            "myriad_markets.api_key": "test-only-myriad-api",
            "myriad_markets.private_key": "test-only-myriad-private",
            "myriad_markets.rpc_url": "https://test-only-myriad-rpc",
        }
        payload: dict[str, object] = {}
        for dotted_key, value in secrets.items():
            target = payload
            parts = dotted_key.split(".")
            for part in parts[:-1]:
                target = target.setdefault(part, {})  # type: ignore[assignment]
            target[parts[-1]] = value

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            rendered = repr(load_config(path))

        for secret in secrets.values():
            with self.subTest(secret=secret):
                self.assertNotIn(secret, rendered)
        self.assertIn("SxBetConfig", rendered)

    def test_optional_config_strings_strip_environment_whitespace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps({"polymarket": {"funder": "${POLYMARKET_FUNDER_ADDRESS}"}}),
                encoding="utf-8",
            )

            with patch.dict(
                os.environ,
                {"POLYMARKET_FUNDER_ADDRESS": " 0x0000000000000000000000000000000000000001 \t"},
                clear=False,
            ):
                config = load_config(path)

            self.assertEqual(config.polymarket.funder, "0x0000000000000000000000000000000000000001")

    def test_execution_mode_environment_override_can_force_shadow(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "execution_mode": "canary",
                        "isTest": False,
                        "live_trading_confirmed": True,
                    }
                ),
                encoding="utf-8",
            )

            with patch.dict(os.environ, {"ARBITRAGE_EXECUTION_MODE_OVERRIDE": "shadow"}, clear=False):
                config = load_config(path)

            self.assertEqual(config.execution_mode, ExecutionMode.SHADOW)

    def test_shadow_verified_mapping_gate_is_typed_and_defaults_off(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(json.dumps({"shadow_require_verified_mappings": True}), encoding="utf-8")

            configured = load_config(path)
            defaulted = load_config(Path(__file__).parents[1] / "config.example.json")

            self.assertTrue(configured.shadow_require_verified_mappings)
            self.assertFalse(defaulted.shadow_require_verified_mappings)

            for invalid in ("false", 0, 1, None):
                path.write_text(
                    json.dumps({"shadow_require_verified_mappings": invalid}),
                    encoding="utf-8",
                )
                with self.subTest(invalid=invalid), self.assertRaisesRegex(
                    ValueError,
                    "shadow_require_verified_mappings must be a JSON boolean",
                ):
                    load_config(path)

    def test_timezone_less_expiry_is_normalized_to_utc(self) -> None:
        parsed = _parse_datetime("2026-06-30T12:00:00")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed and parsed.tzinfo, UTC)

    def test_orderbook_age_guard_is_restricted_to_hft_range(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "isTest": True,
                        "scan_all": True,
                        "myriad_markets": {
                            "enabled": True,
                            "collateral_tokens": {"USDT": "0x1"},
                        },
                    }
                ),
                encoding="utf-8",
            )
            config = load_config(path)

            validate_config(replace(config, max_orderbook_age_seconds=1.5))
            validate_config(replace(config, max_orderbook_age_seconds=2.0))
            with self.assertRaisesRegex(ValueError, "between 1.5 and 2.0"):
                validate_config(replace(config, max_orderbook_age_seconds=1.49))

    def test_discovery_staleness_budget_is_typed_and_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "isTest": True,
                        "discovery_max_stale_seconds": 1800,
                        "scan_all": True,
                        "myriad_markets": {
                            "enabled": True,
                            "collateral_tokens": {"USDT": "0x1"},
                        },
                    }
                ),
                encoding="utf-8",
            )
            config = load_config(path)

            self.assertEqual(config.discovery_max_stale_seconds, 1800.0)
            validate_config(config)
            with self.assertRaisesRegex(ValueError, "must be at least 900"):
                validate_config(replace(config, discovery_max_stale_seconds=899.0))

    def test_route_market_data_prefetch_policy_is_typed_and_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "isTest": True,
                        "scan_all": True,
                        "market_data_target_hold_seconds_by_route": {"polymarket_myriad": 60},
                        "market_data_executable_priority_seconds": 30,
                        "market_data_executable_priority_seconds_by_route": {
                            "polymarket_myriad": 300
                        },
                        "shadow_preflight_samples": 3,
                        "shadow_preflight_sample_interval_seconds": 0.15,
                        "shadow_preflight_cooldown_seconds": 30,
                        "shadow_preflight_evidence_ttl_seconds": 900,
                        "market_data_exploration_fraction": 0.25,
                        "market_data_exploration_fraction_by_route": {"polymarket_myriad": 0.5},
                        "market_data_prefetch_multiplier_by_route": {"polymarket_myriad": 4},
                        "market_evaluation_weight_by_route": {"polymarket_myriad": 2},
                        "max_concurrent_market_evaluations": 20,
                        "max_concurrent_market_evaluations_by_route": {
                            "polymarket_myriad": 12
                        },
                        "myriad_markets": {
                            "enabled": True,
                            "collateral_tokens": {"USDT": "0x1"},
                        },
                    }
                ),
                encoding="utf-8",
            )
            config = load_config(path)

            self.assertEqual(config.market_data_target_hold_for("polymarket_myriad"), 60.0)
            self.assertEqual(config.market_data_executable_priority_for("polymarket_myriad"), 300.0)
            self.assertEqual(config.market_data_executable_priority_for("polymarket_predict"), 30.0)
            self.assertEqual(config.shadow_preflight_samples, 3)
            self.assertEqual(config.shadow_preflight_sample_interval_seconds, 0.15)
            self.assertEqual(config.shadow_preflight_cooldown_seconds, 30.0)
            self.assertEqual(config.shadow_preflight_evidence_ttl_seconds, 900.0)
            self.assertEqual(config.market_data_exploration_fraction_for("polymarket_myriad"), 0.5)
            self.assertEqual(config.market_data_exploration_fraction_for("polymarket_predict"), 0.25)
            self.assertEqual(config.market_data_prefetch_multiplier_for("polymarket_myriad"), 4)
            self.assertEqual(config.market_data_prefetch_multiplier_for("polymarket_predict"), 1)
            self.assertEqual(config.market_evaluation_weight_for("polymarket_myriad"), 2)
            self.assertEqual(config.market_evaluation_weight_for("polymarket_predict"), 1)
            self.assertEqual(
                config.max_concurrent_market_evaluations_for("polymarket_myriad"),
                12,
            )
            self.assertEqual(
                config.max_concurrent_market_evaluations_for("polymarket_predict"),
                20,
            )
            validate_config(config)
            with self.assertRaisesRegex(ValueError, "values between 1 and 4"):
                validate_config(
                    replace(
                        config,
                        market_data_prefetch_multiplier_by_route={"polymarket_myriad": 5},
                    )
                )
            with self.assertRaisesRegex(ValueError, "known routes"):
                validate_config(
                    replace(
                        config,
                        market_data_target_hold_seconds_by_route={"polymarket_typo": 60.0},
                    )
                )
            with self.assertRaisesRegex(ValueError, "market_data_executable_priority_seconds_by_route"):
                validate_config(
                    replace(
                        config,
                        market_data_executable_priority_seconds_by_route={"polymarket_typo": 60.0},
                    )
                )
            with self.assertRaisesRegex(ValueError, "market_evaluation_weight_by_route"):
                validate_config(
                    replace(
                        config,
                        market_evaluation_weight_by_route={"polymarket_myriad": 5},
                    )
                )
            with self.assertRaisesRegex(
                ValueError,
                "max_concurrent_market_evaluations_by_route",
            ):
                validate_config(
                    replace(
                        config,
                        max_concurrent_market_evaluations_by_route={
                            "polymarket_typo": 12
                        },
                    )
                )
            with self.assertRaisesRegex(
                ValueError,
                "values between 1 and max_concurrent_market_evaluations",
            ):
                validate_config(
                    replace(
                        config,
                        max_concurrent_market_evaluations_by_route={
                            "polymarket_myriad": 21
                        },
                    )
                )
            with self.assertRaisesRegex(ValueError, "market_data_exploration_fraction_by_route"):
                validate_config(
                    replace(
                        config,
                        market_data_exploration_fraction_by_route={"polymarket_myriad": 1.1},
                    )
                )
            with self.assertRaisesRegex(ValueError, "shadow_preflight_samples"):
                validate_config(replace(config, shadow_preflight_samples=6))
            with self.assertRaisesRegex(ValueError, "shadow_preflight_sample_interval_seconds"):
                validate_config(replace(config, shadow_preflight_sample_interval_seconds=1.1))
            with self.assertRaisesRegex(ValueError, "shadow_preflight_cooldown_seconds"):
                validate_config(replace(config, shadow_preflight_cooldown_seconds=-1))
            with self.assertRaisesRegex(ValueError, "shadow_preflight_evidence_ttl_seconds"):
                validate_config(replace(config, shadow_preflight_evidence_ttl_seconds=0))

    def test_load_config_reads_runtime_instance_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "execution_mode": "canary",
                        "isTest": False,
                        "scan_all": True,
                        "market_horizon_filter_enabled": True,
                        "database_url": "${DATABASE_URL}",
                        "runtime_instance_id": "quote_arb",
                        "live_trading_confirmed": True,
                        "spread_policy": {"fixed_chain_cost_usd": 0.25},
                        "enable_predict_fun": False,
                        "routes": {
                            "polymarket_myriad": True,
                            "polymarket_predict": False,
                            "predict_myriad": False,
                            "predict_sx": False,
                            "polymarket_sx": False,
                            "sx_myriad": False
                        },
                        "funded_routes": {"polymarket_myriad": True},
                        "position_size_usd": 20.0,
                        "max_order_size_usd": 20.0,
                        "max_daily_loss_usd": 10.0,
                        "max_open_positions": 1,
                        "polymarket": {
                            "private_key": "0x" + "1" * 64
                        },
                        "myriad_markets": {
                            "enabled": True,
                            "private_key": "0x" + "2" * 64,
                            "collateral_tokens": {"USDT": "0x1"}
                        }
                    }
                ),
                encoding="utf-8",
            )

            with patch.dict(os.environ, {"DATABASE_URL": "postgresql://db", "LIVE_TRADING_CONFIRM": ""}, clear=False):
                config = load_config(path)

            self.assertEqual(config.runtime_instance_id, "quote_arb")
            validate_config(config, require_verified_mappings=False)
            with self.assertRaisesRegex(ValueError, "between 1.5 and 2.0"):
                validate_config(replace(config, max_orderbook_age_seconds=2.01))

    def test_scan_all_canary_requires_market_horizon_filter(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "isTest": True,
                        "scan_all": True,
                        "myriad_markets": {
                            "enabled": True,
                            "collateral_tokens": {"USDT": "0x1"},
                        },
                    }
                ),
                encoding="utf-8",
            )
            config = load_config(path)

            with self.assertRaisesRegex(ValueError, "scan_all canary/live requires"):
                validate_config(
                    replace(
                        config,
                        execution_mode=ExecutionMode.CANARY,
                        market_horizon_filter_enabled=False,
                    )
                )

    def test_additional_canary_category_requires_configured_horizon(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "isTest": True,
                        "scan_all": True,
                        "market_horizon_filter_enabled": True,
                        "categories_to_scan": ["weather"],
                        "myriad_markets": {
                            "enabled": True,
                            "collateral_tokens": {"USDT": "0x1"},
                        },
                    }
                ),
                encoding="utf-8",
            )
            config = replace(load_config(path), execution_mode=ExecutionMode.CANARY)

            with self.assertRaisesRegex(
                ValueError,
                "max_market_horizon_hours_by_category for: weather",
            ):
                validate_config(config)

            with self.assertRaises(ValueError) as remaining_canary_errors:
                validate_config(
                    replace(
                        config,
                        max_market_horizon_hours_by_category={"Weather": 200.0},
                    )
                )
            self.assertNotIn(
                "max_market_horizon_hours_by_category for: weather",
                str(remaining_canary_errors.exception),
            )

    def test_load_config_reads_category_horizon_map(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "isTest": True,
                        "max_market_horizon_hours_by_category": {
                            "weather": 200,
                            "politics": 72.5,
                        },
                    }
                ),
                encoding="utf-8",
            )

            config = load_config(path)

            self.assertEqual(
                config.max_market_horizon_hours_by_category,
                {"weather": 200.0, "politics": 72.5},
            )
            with self.assertRaisesRegex(ValueError, "values must be positive"):
                validate_config(
                    replace(
                        config,
                        max_market_horizon_hours_by_category={"weather": 0.0},
                    )
                )

    def test_percentage_fields_require_decimal_fractions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "isTest": True,
                        "min_entry_spread_pct": 8.0,
                        "myriad_markets": {
                            "enabled": True,
                            "collateral_tokens": {"USDT": "0x1"},
                        },
                        "scan_all": True,
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "decimal fraction"):
                load_config(path)

    def test_predict_fun_can_be_hard_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "isTest": True,
                        "enable_predict_fun": False,
                        "predict_fun": {"enabled": True, "api_key": "key"},
                        "myriad_markets": {
                            "enabled": True,
                            "collateral_tokens": {"USDT": "0x1"},
                        },
                        "scan_all": True,
                    }
                ),
                encoding="utf-8",
            )

            config = load_config(path)

            validate_config(config)
            self.assertFalse(config.enable_predict_fun)

    def test_load_config_accepts_live_trading_confirm_from_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "execution_mode": "canary",
                        "isTest": False,
                        "scan_all": False,
                        "database_url": "${DATABASE_URL}",
                        "live_trading_confirmed": True,
                        "spread_policy": {"fixed_chain_cost_usd": 0.25},
                        "enable_predict_fun": False,
                        "routes": {
                            "polymarket_myriad": True,
                            "polymarket_predict": False,
                            "predict_myriad": False,
                            "polymarket_sx": False,
                            "sx_myriad": False,
                        },
                        "funded_routes": {"polymarket_myriad": True},
                        "position_size_usd": 20.0,
                        "max_order_size_usd": 20.0,
                        "max_daily_loss_usd": 10.0,
                        "max_open_positions": 1,
                        "polymarket": {
                            "private_key": "0x" + "1" * 64,
                        },
                        "myriad_markets": {
                            "enabled": True,
                            "private_key": "0x" + "2" * 64,
                            "collateral_tokens": {"USDT": "0x1"},
                        },
                        "markets": [
                            {
                                "symbol": "BTC-USD",
                                "target_label": ">$75,000",
                                "polymarket_token_id": "poly",
                                "polymarket_side": "YES",
                                "myriad_market_id": "myriad",
                                "myriad_side": "NO",
                                "mapping_status": "VERIFIED",
                                "verified_routes": ["polymarket_myriad"],
                                "rules_fingerprint": "fingerprint",
                                "resolution_source": "Coinbase BTC/USD close",
                                "outcome_semantics": "YES if close is strictly above 75000 USD",
                                "category": "finance",
                                "expires_at": "2026-06-30T12:00:00Z",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            with patch.dict(os.environ, {"DATABASE_URL": "postgresql://db"}, clear=False):
                with patch.dict(os.environ, {"LIVE_TRADING_CONFIRM": ""}, clear=False):
                    config = load_config(path)
                    self.assertTrue(config.live_trading_confirmed)
                    validate_config(config)

    def test_load_config_applies_database_host_and_port_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "isTest": True,
                        "scan_all": True,
                        "database_url": "${DATABASE_URL}",
                        "myriad_markets": {
                            "enabled": True,
                            "collateral_tokens": {"USDT": "0x1"},
                        },
                    }
                ),
                encoding="utf-8",
            )

            with patch.dict(
                os.environ,
                {
                    "DATABASE_URL": "postgresql+asyncpg://arb-user:pa:ss@postgres:6543/arbitrage?sslmode=disable",
                    "ARBITRAGE_DATABASE_HOST_OVERRIDE": "127.0.0.1",
                    "ARBITRAGE_DATABASE_PORT_OVERRIDE": "5432",
                },
                clear=False,
            ):
                config = load_config(path)

            self.assertEqual(
                config.database_url,
                "postgresql+asyncpg://arb-user:pa%3Ass@127.0.0.1:5432/arbitrage?sslmode=disable",
            )

    def test_load_operator_env_prefers_adjacent_env_production_for_production_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "config.production.json").write_text("{}", encoding="utf-8")
            (root / ".env.production").write_text("LIVE_TRADING_CONFIRM=YES\n", encoding="utf-8")
            (root / ".env").write_text("LIVE_TRADING_CONFIRM=NO\n", encoding="utf-8")

            with patch.dict(os.environ, {}, clear=True):
                load_operator_env(root / "config.production.json")

                self.assertEqual(os.getenv("LIVE_TRADING_CONFIRM"), "YES")

    def test_load_operator_env_uses_adjacent_env_for_non_production_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "config.json").write_text("{}", encoding="utf-8")
            (root / ".env").write_text("DATABASE_URL=postgresql://local-db\n", encoding="utf-8")
            (root / ".env.production").write_text("DATABASE_URL=postgresql://prod-db\n", encoding="utf-8")

            with patch.dict(os.environ, {}, clear=True):
                load_operator_env(root / "config.json")

                self.assertEqual(os.getenv("DATABASE_URL"), "postgresql://local-db")

    def test_load_operator_env_keeps_injected_environment_when_dotenv_is_unreadable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "config.production.json").write_text("{}", encoding="utf-8")
            (root / ".env.production").write_text("DATABASE_URL=from-file\n", encoding="utf-8")

            with (
                patch.dict(os.environ, {"DATABASE_URL": "from-compose"}, clear=True),
                patch("arbitrage_engine.config.load_dotenv", side_effect=PermissionError),
                self.assertLogs("arbitrage_engine.config", level="WARNING") as captured,
            ):
                load_operator_env(root / "config.production.json")
                self.assertEqual(os.getenv("DATABASE_URL"), "from-compose")

            self.assertTrue(
                any(
                    "operator_env_file_unreadable_using_process_environment" in message
                    for message in captured.output
                )
            )

    def test_load_config_accepts_legacy_sx_env_aliases(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "isTest": True,
                        "scan_all": True,
                        "sx_bet": {
                            "enabled": True,
                            "api_key": "${SX_BET_API_KEY}",
                            "private_key": "${SX_BET_PRIVATE_KEY}",
                            "base_token_address": "${SX_BET_BASE_TOKEN_ADDRESS}",
                        },
                        "myriad_markets": {
                            "enabled": True,
                            "collateral_tokens": {"USDT": "0x1"},
                        },
                    }
                ),
                encoding="utf-8",
            )

            with patch.dict(
                os.environ,
                {
                    "SX_API_KEY": "legacy-api-key",
                    "SX_PRIVATE_KEY": "0x" + ("3" * 64),
                    "SX_BASE_TOKEN_ADDRESS": "0x" + ("4" * 40),
                },
                clear=False,
            ):
                config = load_config(path)

            self.assertEqual(config.sx_bet.api_key, "legacy-api-key")
            self.assertEqual(config.sx_bet.private_key, "0x" + ("3" * 64))
            self.assertEqual(config.sx_bet.base_token_address, "0x" + ("4" * 40))

    def test_load_config_reads_sx_v3_cutover_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "isTest": True,
                        "scan_all": True,
                        "sx_bet": {
                            "enabled": True,
                            "api_version": "v3",
                            "environment": "toronto",
                            "time_in_force": "fok",
                            "allow_v3_mainnet": "false",
                            "api_base_url": "https://api.toronto.sx.bet",
                            "ws_url": "wss://realtime.toronto.sx.bet/connection/websocket",
                        },
                        "myriad_markets": {
                            "enabled": True,
                            "collateral_tokens": {"USDT": "0x1"},
                        },
                    }
                ),
                encoding="utf-8",
            )

            config = load_config(path)

            self.assertEqual(config.sx_bet.api_version, "v3")
            self.assertEqual(config.sx_bet.environment, "toronto")
            self.assertEqual(config.sx_bet.time_in_force, "FOK")
            self.assertFalse(config.sx_bet.allow_v3_mainnet)
            validate_config(config)

    def test_sx_v3_cutover_flag_rejects_non_boolean_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps({"sx_bet": {"allow_v3_mainnet": "not-a-boolean"}}),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "sx_bet.allow_v3_mainnet must be a boolean"):
                load_config(path)

    def test_sx_v3_mainnet_requires_explicit_operator_cutover(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "isTest": True,
                        "scan_all": True,
                        "sx_bet": {
                            "enabled": True,
                            "api_version": "v3",
                            "environment": "mainnet",
                            "api_base_url": "https://api.sx.bet",
                            "ws_url": "wss://realtime.sx.bet/connection/websocket",
                        },
                        "myriad_markets": {
                            "enabled": True,
                            "collateral_tokens": {"USDT": "0x1"},
                        },
                    }
                ),
                encoding="utf-8",
            )
            config = load_config(path)

            with self.assertRaisesRegex(ValueError, "allow_v3_mainnet=true"):
                validate_config(config)

            enabled = replace(config, sx_bet=replace(config.sx_bet, allow_v3_mainnet=True))
            validate_config(enabled)
            with self.assertRaisesRegex(ValueError, "official API host"):
                validate_config(
                    replace(
                        enabled,
                        sx_bet=replace(enabled.sx_bet, api_base_url="https://api.sx.bet.evil.example"),
                    )
                )
            with self.assertRaisesRegex(ValueError, "official realtime host"):
                validate_config(
                    replace(
                        enabled,
                        sx_bet=replace(enabled.sx_bet, ws_url="wss://realtime.sx.bet.evil.example/ws"),
                    )
                )

    def test_scan_all_allows_myriad_without_predict_api_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "isTest": True,
                        "scan_all": True,
                        "myriad_markets": {
                            "enabled": True,
                            "api_key": "myriad-key",
                            "collateral_tokens": {"USDT": "0x0000000000000000000000000000000000000001"},
                        },
                    }
                ),
                encoding="utf-8",
            )

            config = load_config(path)

            validate_config(config)
            self.assertFalse(bool(config.predict_fun.api_key))

    def test_wildcard_market_filter_enables_scan_all(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "isTest": True,
                        "predict_fun": {"api_key": "test-key"},
                        "markets": [{"symbol": "*"}],
                    }
                ),
                encoding="utf-8",
            )

            config = load_config(path)

            validate_config(config)
            self.assertTrue(config.scan_all)
            self.assertEqual(config.markets, [])
            validate_config(config, require_resolved_markets=not config.scan_all)
            with self.assertRaisesRegex(ValueError, "markets must contain at least one market"):
                validate_config(config, require_resolved_markets=True)

    def test_scan_all_allows_empty_market_filters(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "isTest": True,
                        "scan_all": True,
                        "predict_fun": {"api_key": "test-key"},
                        "markets": [{}],
                    }
                ),
                encoding="utf-8",
            )

            config = load_config(path)

            validate_config(config)
            self.assertTrue(config.scan_all)
            self.assertEqual(config.markets, [])

    def test_scan_all_defaults_to_single_sport_category(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "isTest": True,
                        "scan_all": True,
                        "myriad_markets": {
                            "enabled": True,
                            "collateral_tokens": {"USDT": "0x1"},
                        },
                    }
                ),
                encoding="utf-8",
            )

            config = load_config(path)

            self.assertEqual(config.categories_to_scan, ["sport"])

    def test_validate_config_requires_live_keys_for_production(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "isTest": False,
                        "shadow_mode": False,
                        "polymarket": {"private_key": None},
                        "predict_fun": {"private_key": None, "api_base_url": None, "api_key": "test-key"},
                        "markets": [
                            {
                                "symbol": "BTC-USD",
                                "target_label": ">$75,000",
                                "polymarket_token_id": "poly",
                                "polymarket_side": "YES",
                                "predict_fun_token_id": "predict",
                                "predict_fun_side": "NO",
                                "expires_at": "2026-06-30T12:00:00Z",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            config = load_config(path)

            with self.assertRaisesRegex(ValueError, "PREDICT_FUN_PRIVATE_KEY"):
                validate_config(config)

    def test_post_discovery_validation_requires_resolved_predict_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "isTest": True,
                        "predict_fun": {"api_key": "test-key"},
                        "markets": [
                            {
                                "symbol": "BTC-USD",
                                "target_label": ">$75,000",
                                "polymarket_token_id": "",
                                "polymarket_side": "YES",
                                "predict_fun_token_id": "",
                                "predict_fun_side": "NO",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            config = load_config(path)
            validate_config(config)

            with self.assertRaisesRegex(ValueError, "second_leg_token_id|predict_fun_token_id"):
                validate_config(config, require_resolved_markets=True)

    def test_entry_spread_defaults_to_five_percent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "isTest": True,
                        "predict_fun": {"api_key": "test-key"},
                        "markets": [
                            {
                                "symbol": "BTC-USD",
                                "target_label": ">$75,000",
                                "polymarket_token_id": "poly",
                                "polymarket_side": "YES",
                                "predict_fun_token_id": "predict",
                                "predict_fun_side": "NO",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            config = load_config(path)
            validate_config(config)
            self.assertEqual(config.min_net_spread, 0.05)

    def test_polymarket_defaults_to_pusd_collateral(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "isTest": True,
                        "scan_all": True,
                        "myriad_markets": {
                            "enabled": True,
                            "collateral_tokens": {"USDT": "0x1"},
                        },
                    }
                ),
                encoding="utf-8",
            )

            config = load_config(path)
            self.assertEqual(
                config.polymarket.collateral_token_address,
                "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB",
            )

    def test_canary_accepts_locked_five_position_limits_and_rejects_larger_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "execution_mode": "canary",
                        "isTest": False,
                        "scan_all": False,
                        "database_url": "${DATABASE_URL}",
                        "enable_predict_fun": False,
                        "routes": {
                            "polymarket_myriad": True,
                            "polymarket_predict": False,
                            "predict_myriad": False,
                        },
                        "funded_routes": {"polymarket_myriad": True},
                        "position_size_usd": 50.0,
                        "max_order_size_usd": 50.0,
                        "max_daily_loss_usd": 10.0,
                        "max_open_positions": 5,
                        "max_total_notional_usd": 252.0,
                        "max_venue_exposure_usd": 125.0,
                        "max_market_exposure_usd": 52.0,
                        "max_unresolved_exposure_usd": 5.0,
                        "max_orders_per_minute": 10,
                        "spread_policy": {"fixed_chain_cost_usd": 0.25},
                        "polymarket": {
                            "private_key": "0x" + "1" * 64,
                        },
                        "predict_fun": {
                            "enabled": False,
                        },
                        "myriad_markets": {
                            "enabled": True,
                            "private_key": "0x" + "2" * 64,
                            "collateral_tokens": {"USDT": "0x1"},
                        },
                        "markets": [
                            {
                                "symbol": "BTC-USD",
                                "target_label": ">$75,000",
                                "polymarket_token_id": "poly",
                                "polymarket_side": "YES",
                                "myriad_market_id": "myriad",
                                "myriad_side": "NO",
                                "mapping_status": "VERIFIED",
                                "verified_routes": ["polymarket_myriad"],
                                "rules_fingerprint": "fingerprint",
                                "resolution_source": "Coinbase BTC/USD close",
                                "outcome_semantics": "YES if close is strictly above 75000 USD",
                                "category": "finance",
                                "expires_at": "2026-06-30T12:00:00Z",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            with patch.dict(os.environ, {"DATABASE_URL": "postgresql://db", "LIVE_TRADING_CONFIRM": "YES"}):
                config = load_config(path)
                validate_config(config)
                with self.assertRaisesRegex(ValueError, "positive spread_policy fixed chain cost"):
                    validate_config(
                        replace(
                            config,
                            spread_policy=replace(
                                config.spread_policy,
                                fixed_chain_cost_usd=0.0,
                                fixed_chain_cost_usd_by_route={},
                            ),
                        )
                    )
                validate_config(config)
                with self.assertRaisesRegex(ValueError, r"\$50 total \(\$25 per leg\)"):
                    validate_config(replace(config, position_size_usd=50.01, max_order_size_usd=50.01))
                with self.assertRaisesRegex(ValueError, "must not exceed 5"):
                    validate_config(replace(config, max_open_positions=6))
                with self.assertRaisesRegex(ValueError, r"must not exceed \$10"):
                    validate_config(replace(config, max_daily_loss_usd=10.01))

    def test_second_leg_aliases_and_sx_route_parse(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "isTest": True,
                        "enable_sx_bet": True,
                        "sx_bet": {"enabled": True},
                        "routes": {
                            "polymarket_myriad": False,
                            "polymarket_predict": False,
                            "predict_myriad": False,
                            "polymarket_sx": True,
                            "sx_myriad": False,
                        },
                        "funded_routes": {"polymarket_sx": True},
                        "markets": [
                            {
                                "symbol": "MATCH",
                                "target_label": "Team A win",
                                "polymarket_token_id": "poly",
                                "polymarket_side": "YES",
                                "second_leg_token_id": "sx:market:NO",
                                "second_leg_side": "NO",
                                "second_venue_label": "SX Bet",
                                "second_leg_market_id": "0xmarket",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            config = load_config(path)

            self.assertTrue(config.routes.polymarket_sx)
            self.assertEqual(config.markets[0].venue_b_label, "SX Bet")
            self.assertEqual(config.markets[0].predict_fun_market_id, "0xmarket")
            self.assertEqual(config.markets[0].predict_fun_token_id, "sx:market:NO")

    def test_live_execution_allows_sx_routes_when_required_keys_and_mappings_exist(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "execution_mode": "canary",
                        "isTest": False,
                        "scan_all": False,
                        "database_url": "${DATABASE_URL}",
                        "enable_predict_fun": False,
                        "enable_sx_bet": True,
                        "sx_bet": {
                            "enabled": True,
                            "private_key": "0x" + "3" * 64,
                        },
                        "routes": {
                            "polymarket_myriad": False,
                            "polymarket_predict": False,
                            "predict_myriad": False,
                            "polymarket_sx": True,
                            "sx_myriad": False,
                        },
                        "funded_routes": {"polymarket_sx": True},
                        "position_size_usd": 20.0,
                        "max_order_size_usd": 20.0,
                        "max_daily_loss_usd": 10.0,
                        "max_open_positions": 1,
                        "spread_policy": {"fixed_chain_cost_usd": 0.25},
                        "polymarket": {
                            "private_key": "0x" + "1" * 64,
                        },
                        "markets": [
                            {
                                "symbol": "MATCH",
                                "target_label": "Team A win",
                                "polymarket_token_id": "poly",
                                "polymarket_side": "YES",
                                "second_leg_token_id": "sx:market:NO",
                                "second_leg_side": "NO",
                                "second_venue_label": "SX Bet",
                                "second_leg_market_id": "0xmarket",
                                "mapping_status": "VERIFIED",
                                "verified_routes": ["polymarket_sx"],
                                "rules_fingerprint": "fingerprint",
                                "resolution_source": "Official event result",
                                "outcome_semantics": "Outcome one=Team A; outcome two=Team B",
                                "category": "sports",
                                "expires_at": "2026-06-30T12:00:00Z",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            with patch.dict(os.environ, {"DATABASE_URL": "postgresql://db", "LIVE_TRADING_CONFIRM": "YES"}):
                validate_config(load_config(path))

                config = load_config(path)
                v3 = replace(
                    config,
                    sx_bet=replace(
                        config.sx_bet,
                        api_version="v3",
                        environment="toronto",
                        api_base_url="https://api.toronto.sx.bet",
                        ws_url="wss://realtime.toronto.sx.bet/connection/websocket",
                        api_key=None,
                        rpc_url="",
                    ),
                )
                with self.assertRaisesRegex(ValueError, "SX_BET_API_KEY"):
                    validate_config(v3)
                validate_config(replace(v3, sx_bet=replace(v3.sx_bet, api_key="new-v3-key")))

    def test_sx_fill_timeout_has_dedicated_config_and_validation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "execution_mode": "canary",
                        "isTest": False,
                        "scan_all": False,
                        "database_url": "${DATABASE_URL}",
                        "enable_predict_fun": False,
                        "enable_sx_bet": True,
                        "sx_bet_fill_timeout_ms": 4001,
                        "sx_bet": {
                            "enabled": True,
                            "private_key": "0x" + "3" * 64,
                        },
                        "routes": {
                            "polymarket_myriad": False,
                            "polymarket_predict": False,
                            "predict_myriad": False,
                            "polymarket_sx": True,
                            "sx_myriad": False,
                        },
                        "funded_routes": {"polymarket_sx": True},
                        "position_size_usd": 20.0,
                        "max_order_size_usd": 20.0,
                        "max_daily_loss_usd": 10.0,
                        "max_open_positions": 1,
                        "spread_policy": {"fixed_chain_cost_usd": 0.25},
                        "polymarket": {
                            "private_key": "0x" + "1" * 64,
                        },
                        "markets": [
                            {
                                "symbol": "MATCH",
                                "target_label": "Team A win",
                                "polymarket_token_id": "poly",
                                "polymarket_side": "YES",
                                "second_leg_token_id": "sx:market:NO",
                                "second_leg_side": "NO",
                                "second_venue_label": "SX Bet",
                                "second_leg_market_id": "0xmarket",
                                "mapping_status": "VERIFIED",
                                "verified_routes": ["polymarket_sx"],
                                "rules_fingerprint": "fingerprint",
                                "resolution_source": "Official event result",
                                "outcome_semantics": "Outcome one=Team A; outcome two=Team B",
                                "category": "sports",
                                "expires_at": "2026-06-30T12:00:00Z",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            with patch.dict(os.environ, {"DATABASE_URL": "postgresql://db", "LIVE_TRADING_CONFIRM": "YES"}):
                config = load_config(path)
                self.assertEqual(config.sx_bet_fill_timeout_ms, 4001)
                validate_config(config)
                with self.assertRaisesRegex(ValueError, "sx_bet_fill_timeout_ms must be at least 3600"):
                    validate_config(replace(config, sx_bet_fill_timeout_ms=3599))

    def test_sx_fill_timeout_defaults_to_predict_timeout_for_backward_compatibility(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "isTest": True,
                        "scan_all": True,
                        "predict_fun_fill_timeout_ms": 4567,
                        "myriad_markets": {
                            "enabled": True,
                            "collateral_tokens": {"USDT": "0x1"},
                        },
                    }
                ),
                encoding="utf-8",
            )

            config = load_config(path)

            self.assertEqual(config.predict_fun_fill_timeout_ms, 4567)
            self.assertEqual(config.sx_bet_fill_timeout_ms, 4567)

    def test_validation_allows_predict_and_sx_route_families_together_in_shadow(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "isTest": True,
                        "scan_all": True,
                        "enable_predict_fun": True,
                        "enable_sx_bet": True,
                        "predict_fun": {"enabled": True, "api_key": "predict-key"},
                        "sx_bet": {"enabled": True},
                        "myriad_markets": {
                            "enabled": True,
                            "collateral_tokens": {"USDT": "0x1"},
                        },
                        "routes": {
                            "polymarket_myriad": True,
                            "polymarket_predict": True,
                            "predict_myriad": True,
                            "polymarket_sx": True,
                            "sx_myriad": True,
                        },
                    }
                ),
                encoding="utf-8",
            )

            validate_config(load_config(path))

    def test_canary_polymarket_myriad_does_not_require_predict_fun_keys_when_predict_routes_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "execution_mode": "canary",
                        "isTest": False,
                        "scan_all": False,
                        "database_url": "${DATABASE_URL}",
                        "enable_predict_fun": True,
                        "predict_fun": {
                            "enabled": True,
                            "api_key": None
                        },
                        "routes": {
                            "polymarket_myriad": True,
                            "polymarket_predict": False,
                            "predict_myriad": False,
                            "polymarket_sx": False,
                            "sx_myriad": False
                        },
                        "funded_routes": {"polymarket_myriad": True},
                        "position_size_usd": 20.0,
                        "max_order_size_usd": 20.0,
                        "max_daily_loss_usd": 10.0,
                        "max_open_positions": 1,
                        "spread_policy": {"fixed_chain_cost_usd": 0.25},
                        "polymarket": {
                            "private_key": "0x" + "1" * 64
                        },
                        "myriad_markets": {
                            "enabled": True,
                            "private_key": "0x" + "2" * 64,
                            "collateral_tokens": {"USDT": "0x1"}
                        },
                        "markets": [
                            {
                                "symbol": "BTC-USD",
                                "target_label": ">$75,000",
                                "polymarket_token_id": "poly",
                                "polymarket_side": "YES",
                                "myriad_market_id": "myriad",
                                "myriad_side": "NO",
                                "mapping_status": "VERIFIED",
                                "verified_routes": ["polymarket_myriad"],
                                "rules_fingerprint": "fingerprint",
                                "resolution_source": "Coinbase BTC/USD close",
                                "outcome_semantics": "YES if close is strictly above 75000 USD",
                                "category": "finance",
                                "expires_at": "2026-06-30T12:00:00Z"
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            with patch.dict(os.environ, {"DATABASE_URL": "postgresql://db", "LIVE_TRADING_CONFIRM": "YES"}):
                validate_config(load_config(path))

    def test_canary_sx_route_requires_sx_private_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "execution_mode": "canary",
                        "isTest": False,
                        "scan_all": False,
                        "database_url": "${DATABASE_URL}",
                        "enable_predict_fun": False,
                        "enable_sx_bet": True,
                        "routes": {
                            "polymarket_myriad": False,
                            "polymarket_predict": False,
                            "predict_myriad": False,
                            "polymarket_sx": True,
                            "sx_myriad": False,
                        },
                        "position_size_usd": 20.0,
                        "max_order_size_usd": 20.0,
                        "max_daily_loss_usd": 10.0,
                        "max_open_positions": 1,
                        "polymarket": {
                            "private_key": "0x" + "1" * 64
                        },
                        "sx_bet": {
                            "enabled": True,
                            "private_key": None,
                            "rpc_url": "https://rpc-rollup.sx.technology",
                        },
                        "myriad_markets": {
                            "enabled": False,
                            "collateral_tokens": {"USDT": "0x1"}
                        },
                        "markets": [
                            {
                                "symbol": "Rams-49ers total",
                                "target_label": "Over 48.5",
                                "polymarket_token_id": "poly",
                                "polymarket_side": "YES",
                                "predict_fun_token_id": "0xmarket:YES",
                                "predict_fun_side": "NO",
                                "venue_b_label": "SX Bet",
                                "predict_fun_market_id": "0xmarket",
                                "mapping_status": "VERIFIED",
                                "verified_routes": ["polymarket_sx"],
                                "rules_fingerprint": "fingerprint",
                                "resolution_source": "SX/Polymarket aligned sports market",
                                "outcome_semantics": "YES if total points are over 48.5",
                                "category": "sports",
                                "expires_at": "2026-09-16T00:00:00Z"
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            with patch.dict(os.environ, {"DATABASE_URL": "postgresql://db", "LIVE_TRADING_CONFIRM": "YES"}):
                with self.assertRaisesRegex(ValueError, "SX_BET_PRIVATE_KEY is required"):
                    validate_config(load_config(path))

    def test_polymarket_api_creds_must_be_complete_when_present(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "execution_mode": "canary",
                        "isTest": False,
                        "scan_all": True,
                        "database_url": "${DATABASE_URL}",
                        "enable_predict_fun": False,
                        "routes": {
                            "polymarket_myriad": True,
                            "polymarket_predict": False,
                            "predict_myriad": False,
                        },
                        "polymarket": {
                            "private_key": "0x" + "1" * 64,
                            "api_key": "pm-key",
                            "api_secret": "pm-secret",
                        },
                        "myriad_markets": {
                            "enabled": True,
                            "private_key": "0x" + "2" * 64,
                            "collateral_tokens": {"USDT": "0x1"},
                        },
                    }
                ),
                encoding="utf-8",
            )

            with patch.dict(os.environ, {"DATABASE_URL": "postgresql://db", "LIVE_TRADING_CONFIRM": "YES"}):
                with self.assertRaisesRegex(ValueError, "api_key, api_secret, and api_passphrase together"):
                    validate_config(load_config(path))

    def test_production_requires_predict_fun_rest_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "isTest": False,
                        "shadow_mode": False,
                        "polymarket": {"private_key": "0x" + "1" * 64},
                        "predict_fun": {
                            "private_key": "0x" + "2" * 64,
                            "api_key": "test-key",
                            "api_base_url": None,
                            "network": "mainnet",
                        },
                        "markets": [
                            {
                                "symbol": "BTC-USD",
                                "target_label": ">$75,000",
                                "polymarket_token_id": "poly",
                                "polymarket_side": "YES",
                                "predict_fun_token_id": "predict",
                                "predict_fun_side": "NO",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "api_base_url"):
                validate_config(load_config(path))

    def test_missing_predict_key_requires_myriad_as_alternative(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "isTest": False,
                        "polymarket": {"private_key": "0xabc"},
                        "predict_fun": {
                            "private_key": "0xabc",
                            "api_base_url": "https://api.predict.fun/",
                            "network": "mainnet",
                        },
                        "markets": [
                            {
                                "symbol": "BTC-USD",
                                "target_label": ">$75,000",
                                "polymarket_token_id": "poly",
                                "polymarket_side": "YES",
                                "predict_fun_token_id": "predict",
                                "predict_fun_side": "NO",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "at least one hedge venue"):
                validate_config(load_config(path))

    def test_myriad_enabled_allows_public_api_without_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "isTest": True,
                        "myriad_markets": {
                            "enabled": True,
                            "private_key": "0xabc",
                            "collateral_tokens": {"USDT": "0x1"},
                        },
                        "markets": [
                            {
                                "symbol": "BTC-USD",
                                "target_label": ">$75,000",
                                "polymarket_token_id": "poly",
                                "polymarket_side": "YES",
                                "predict_fun_token_id": "predict",
                                "predict_fun_side": "NO",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            validate_config(load_config(path))


if __name__ == "__main__":
    unittest.main()
