import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from arbitrage_engine.models import HedgeSide, MarketSpec, OpenPosition, PolymarketSide
from arbitrage_engine.positions import JsonPositionLedger


class PositionLedgerTests(unittest.TestCase):
    def test_json_ledger_round_trips_open_positions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "open_positions.json"
            ledger = JsonPositionLedger(path)
            market = MarketSpec(
                "BTC-USD",
                ">$75,000",
                "token",
                PolymarketSide.YES,
                "BTC/USDT:USDT",
                HedgeSide.SHORT,
                datetime(2026, 6, 30, 12, tzinfo=timezone.utc),
                "condition",
                "0.01",
                False,
            )
            ledger.add(
                OpenPosition(
                    market=market,
                    polymarket_contracts=200,
                    polymarket_entry_price=0.40,
                    cefi_quantity=0.0013,
                    cefi_entry_side=HedgeSide.SHORT,
                    opened_at=datetime(2026, 6, 17, 12, tzinfo=timezone.utc),
                    polymarket_order_id="poly-entry-1",
                    cefi_order_id="hedge-entry-1",
                )
            )

            reloaded = JsonPositionLedger(path).all()

            self.assertEqual(len(reloaded), 1)
            self.assertEqual(reloaded[0].market.condition_id, "condition")
            self.assertEqual(reloaded[0].market.tick_size, "0.01")
            self.assertFalse(reloaded[0].market.neg_risk)


if __name__ == "__main__":
    unittest.main()
