import json
import tempfile
import unittest
from pathlib import Path

from arbitrage_engine.config import load_config, validate_config


class ConfigTests(unittest.TestCase):
    def test_validate_config_requires_live_keys_for_production(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "isTest": False,
                        "polymarket": {"private_key": None},
                        "predict_fun": {"private_key": None, "api_base_url": None},
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

    def test_min_spread_must_be_at_least_ten_percent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "isTest": True,
                        "min_net_spread": 0.05,
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

            with self.assertRaisesRegex(ValueError, "min_net_spread"):
                validate_config(load_config(path))

    def test_production_requires_predict_fun_rest_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "isTest": False,
                        "polymarket": {"private_key": "0xabc"},
                        "predict_fun": {"private_key": "0xabc", "network": "mainnet"},
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

    def test_production_requires_predict_fun_api_key_on_mainnet(self) -> None:
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

            with self.assertRaisesRegex(ValueError, "PREDICT_FUN_API_KEY"):
                validate_config(load_config(path))

    def test_myriad_enabled_requires_api_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "isTest": True,
                        "myriad_markets": {
                            "enabled": True,
                            "private_key": "0xabc",
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

            with self.assertRaisesRegex(ValueError, "MYRIAD_API_KEY"):
                validate_config(load_config(path))


if __name__ == "__main__":
    unittest.main()
