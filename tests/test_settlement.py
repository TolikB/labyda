import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from arbitrage_engine.connectors.base import BinaryMarketClient
from arbitrage_engine.models import (
    BinarySide,
    ExecutionReport,
    MarketSpec,
    OpenPosition,
    OrderBook,
    RedemptionIntent,
    RedemptionIntentStatus,
    RedemptionReport,
    SettlementRequest,
    SettlementStatus,
    position_key,
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


class AutomaticSettlementClient(ManualSettlementClient):
    def __init__(self) -> None:
        self.submissions = 0

    def prepare_settlement_request(self, request: SettlementRequest) -> SettlementRequest:
        return replace(request, collateral_token=request.collateral_token or "0x" + "1" * 40)

    async def get_settlement_status(self, request: SettlementRequest) -> SettlementStatus:
        return SettlementStatus.RESOLVED

    async def redeem_position(self, request: SettlementRequest, redemption_id: str) -> RedemptionReport:
        self.submissions += 1
        return RedemptionReport(RedemptionIntentStatus.SUBMITTED, tx_hash=f"0x{redemption_id.replace('-', '')}")

    async def reconcile_redemption(
        self,
        request: SettlementRequest,
        report: RedemptionReport,
    ) -> RedemptionReport:
        return RedemptionReport(RedemptionIntentStatus.CONFIRMED, tx_hash=report.tx_hash)


class MemorySettlementRepository:
    def __init__(self) -> None:
        self.intents: dict[tuple[str, str, str], RedemptionIntent] = {}
        self.removed: list[str] = []

    async def get_redemption_intent(self, key: str, venue: str, condition_id: str) -> RedemptionIntent | None:
        return self.intents.get((key, venue, condition_id))

    async def create_redemption_intent(self, intent: RedemptionIntent) -> bool:
        key = (intent.position_key, intent.venue, intent.condition_id)
        if key in self.intents:
            return False
        self.intents[key] = intent
        return True

    async def update_redemption_intent(
        self,
        redemption_id: str,
        status: RedemptionIntentStatus,
        *,
        tx_hash: str | None = None,
        error: str | None = None,
    ) -> RedemptionIntent:
        key, current = next(item for item in self.intents.items() if item[1].redemption_id == redemption_id)
        updated = replace(current, status=status, tx_hash=tx_hash or current.tx_hash, last_error=error)
        self.intents[key] = updated
        return updated

    async def save_position(self, key: str, position: OpenPosition) -> None:
        return None

    async def remove_position(self, key: str) -> None:
        self.removed.append(key)

    async def audit(self, event_type: str, payload: dict[str, object]) -> None:
        return None


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
                polymarket_contracts=Decimal("1"),
                polymarket_entry_price=Decimal("0.4"),
                predict_fun_contracts=Decimal("1"),
                predict_fun_entry_price=Decimal("0.5"),
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

    async def test_redemption_intents_prevent_blind_retry_after_restart(self) -> None:
        condition_a = "0x" + "a" * 64
        condition_b = "0x" + "b" * 64
        market = MarketSpec(
            symbol="EXPIRED-AUTO",
            target_label="expired market",
            polymarket_token_id="poly-token",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="myriad-token",
            predict_fun_side=BinarySide.NO,
            venue_b_label="Myriad",
            condition_id=condition_a,
            polymarket_market_id=condition_a,
            myriad_market_id="myriad-market",
            myriad_condition_id=condition_b,
            myriad_collateral_token="USDT",
            expires_at=datetime.now(UTC) - timedelta(minutes=1),
        )
        position = OpenPosition(
            market=market,
            polymarket_contracts=Decimal("1.25"),
            polymarket_entry_price=Decimal("0.4"),
            predict_fun_contracts=Decimal("1.25"),
            predict_fun_entry_price=Decimal("0.5"),
            opened_at=datetime.now(UTC) - timedelta(hours=1),
            polymarket_order_id="poly-order",
            predict_fun_order_id="myriad-order",
        )
        ledger = PositionLedger()
        ledger.add(position)
        repository = MemorySettlementRepository()
        first = AutomaticSettlementClient()
        second = AutomaticSettlementClient()
        service = SettlementService(
            ledger,
            {"Polymarket": first, "Myriad": second},
            GlobalRiskController(10, 3),
            FakeTelegram(),  # type: ignore[arg-type]
            repository,  # type: ignore[arg-type]
        )

        await service.run_once()
        self.assertEqual((first.submissions, second.submissions), (1, 1))
        self.assertTrue(ledger.has(position_key(market)))

        await service.run_once()
        self.assertEqual((first.submissions, second.submissions), (1, 1))
        self.assertFalse(ledger.has(position_key(market)))
        self.assertEqual({intent.status for intent in repository.intents.values()}, {RedemptionIntentStatus.CONFIRMED})


if __name__ == "__main__":
    unittest.main()
