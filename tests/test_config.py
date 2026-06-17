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
                        "binance": {"api_key": None, "api_secret": None},
                        "markets": [
                            {
                                "symbol": "BTC-USD",
                                "target_label": ">$75,000",
                                "polymarket_token_id": "token",
                                "polymarket_side": "YES",
                                "condition_id": "condition",
                                "cefi_symbol": "BTC/USDT:USDT",
                                "cefi_hedge_side": "SHORT",
                                "expires_at": "2026-06-30T12:00:00Z",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            config = load_config(path)

            with self.assertRaisesRegex(ValueError, "BINANCE_API_KEY"):
                validate_config(config)

    def test_post_discovery_validation_requires_resolved_market_metadata(self) -> None:
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
                                "condition_id": None,
                                "cefi_symbol": "BTC/USDT:USDT",
                                "cefi_hedge_side": "SHORT",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            config = load_config(path)
            validate_config(config)

            with self.assertRaisesRegex(ValueError, "polymarket_token_id"):
                validate_config(config, require_resolved_markets=True)


if __name__ == "__main__":
    unittest.main()
