import json
import tempfile
import unittest
from dataclasses import replace
from datetime import UTC
from pathlib import Path

from arbitrage_engine.config import _parse_datetime, load_config, validate_config


class ConfigTests(unittest.TestCase):
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
            with self.assertRaisesRegex(ValueError, "between 1.5 and 2.0"):
                validate_config(replace(config, max_orderbook_age_seconds=2.01))

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

            with self.assertRaisesRegex(ValueError, "predict_fun_token_id"):
                validate_config(config, require_resolved_markets=True)

    def test_entry_spread_defaults_to_eight_percent(self) -> None:
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
            self.assertEqual(config.min_net_spread, 0.08)

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
