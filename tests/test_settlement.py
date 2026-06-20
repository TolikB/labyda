import unittest
from datetime import UTC, datetime, timedelta

from arbitrage_engine.connectors.base import BinaryMarketClient
from arbitrage_engine.models import (
    BinarySide,
    ExecutionReport,
    MarketSpec,
    OpenPosition,
    OrderBook,
)
from arbitrage_engine.positions import PositionLedger
from arbitrage_engine.risk import GlobalRiskController
from arbitrage_engine.settlement import SettlementService


class ManualSettlementClient(BinaryMarketClient):
    async def watch_order_book(self, token_id: str) -> OrderBook:
        raise NotImplementedError

    async def buy(
        self,
        token_id: str,
        side: BinarySide,
        contracts: float,
        max_price: float,
        *,
        condition_id: str | None = None,
        tick_size: str | None = None,
        neg_risk: bool | None = None,
    ) -> str:
        raise NotImplementedError

    async def sell(
        self,
        token_id: str,
        side: BinarySide,
        contracts: float,
        min_price: float,
        *,
        condition_id: str | None = None,
        tick_size: str | None = None,
        neg_risk: bool | None = None,
    ) -> str:
        raise NotImplementedError

    async def wait_filled(self, order_id: str, timeout_ms: int) -> ExecutionReport:
        raise NotImplementedError

    async def cancel_order(self, order_id: str) -> None:
        raise NotImplementedError

    async def get_cash_balance(self) -> float:
        return 0.0


class FakeTelegram:
    def __init__(self) -> None:
        self.messages: list[str] = []

    async def send_html(self, message: str) -> None:
        self.messages.append(message)


class SettlementTests(unittest.IsolatedAsyncioTestCase):
    async def test_unsupported_settlement_pauses_risk_and_requires_manual_review(self) -> None:
        market = MarketSpec(
            symbol="EXPIRED",
            target_label="expired market",
            polymarket_token_id="poly-token",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="predict-token",
            predict_fun_side=BinarySide.NO,
            polymarket_market_id="poly-market",
            predict_fun_market_id="predict-market",
            expires_at=datetime.now(UTC) - timedelta(minutes=1),
        )
        ledger = PositionLedger()
        ledger.add(
            OpenPosition(
                market=market,
                polymarket_contracts=1,
                polymarket_entry_price=0.4,
                predict_fun_contracts=1,
                predict_fun_entry_price=0.5,
                opened_at=datetime.now(UTC) - timedelta(hours=1),
                polymarket_order_id="poly-order",
                predict_fun_order_id="predict-order",
            )
        )
        risk = GlobalRiskController(10, 3)
        telegram = FakeTelegram()
        service = SettlementService(
            ledger,
            {
                "Polymarket": ManualSettlementClient(),
                "Predict.fun": ManualSettlementClient(),
            },
            risk,
            telegram,  # type: ignore[arg-type]
        )

        await service.run_once()

        self.assertTrue(risk.is_paused())
        self.assertEqual(ledger.all()[0].status, "manual_review")
        self.assertIn("SETTLEMENT MANUAL REVIEW REQUIRED", telegram.messages[0])


if __name__ == "__main__":
    unittest.main()
