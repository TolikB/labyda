import json
import tempfile
import unittest
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from arbitrage_engine.models import BinarySide, MarketSpec, OpenPosition
from arbitrage_engine.positions import JsonPositionLedger


class PositionLedgerTests(unittest.TestCase):
    def test_json_ledger_round_trips_open_positions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "open_positions.json"
            ledger = JsonPositionLedger(path)
            market = MarketSpec(
                symbol="BTC-USD",
                target_label=">$75,000",
                polymarket_token_id="poly-token",
                polymarket_side=BinarySide.YES,
                predict_fun_token_id="predict-token",
                predict_fun_side=BinarySide.NO,
                expires_at=datetime(2026, 6, 30, 12, tzinfo=UTC),
                condition_id="condition",
                tick_size="0.01",
                neg_risk=False,
                predict_fun_market_id="predict-market",
                polymarket_url="https://polymarket.com/event/test",
                predict_fun_url="https://predict.fun/market/predict-market",
            )
            ledger.add(
                OpenPosition(
                    market=market,
                    polymarket_contracts=Decimal("100"),
                    polymarket_entry_price=Decimal("0.42"),
                    predict_fun_contracts=Decimal("100"),
                    predict_fun_entry_price=Decimal("0.50"),
                    opened_at=datetime(2026, 6, 17, 12, tzinfo=UTC),
                    polymarket_order_id="poly-entry-1",
                    predict_fun_order_id="predict-entry-1",
                )
            )

            reloaded = JsonPositionLedger(path).all()
            raw = json.loads(path.read_text(encoding="utf-8"))

            self.assertEqual(len(reloaded), 1)
            self.assertEqual(reloaded[0].market.predict_fun_token_id, "predict-token")
            self.assertEqual(reloaded[0].market.predict_fun_side, BinarySide.NO)
            self.assertEqual(reloaded[0].market.polymarket_url, "https://polymarket.com/event/test")
            self.assertEqual(raw[0]["polymarket_entry_price"], "0.42")
            self.assertFalse(path.with_name(f"{path.name}.tmp").exists())


if __name__ == "__main__":
    unittest.main()
