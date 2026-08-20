from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from arbitrage_engine.connectors.base import BinaryMarketClient
from arbitrage_engine.models import (
    BinarySide,
    ExecutionReport,
    FillRecord,
    MarketSpec,
    OpenPosition,
    OrderBook,
    OrderIntent,
    OrderIntentStatus,
    VenueOrder,
)
from arbitrage_engine.reconciliation import ReconciliationService, _expected_positions
from arbitrage_engine.risk import GlobalRiskController


class _FakeNotFound(RuntimeError):
    def __init__(self, message: str = "404 not found") -> None:
        super().__init__(message)
        self.status = 404


class _FakeClient(BinaryMarketClient):
    def __init__(
        self,
        *,
        error: Exception | None = None,
        open_orders: list[VenueOrder] | None = None,
        fills: list[FillRecord] | None = None,
        positions: dict[str, Decimal] | None = None,
    ) -> None:
        self._error = error
        self._open_orders = open_orders or []
        self._fills = fills or []
        self._positions = positions or {}
        self.restored_contexts: list[tuple[str, OrderIntent]] = []

    async def watch_order_book(self, token_id: str) -> OrderBook:
        del token_id
        raise AssertionError("unreachable")

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
        del token_id, side, contracts, max_price, condition_id, tick_size, neg_risk
        raise AssertionError("unreachable")

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
        del token_id, side, contracts, min_price, condition_id, tick_size, neg_risk
        raise AssertionError("unreachable")

    async def wait_filled(self, order_id: str, timeout_ms: int) -> ExecutionReport:
        del order_id, timeout_ms
        raise AssertionError("unreachable")

    async def cancel_order(self, order_id: str) -> None:
        del order_id
        raise AssertionError("unreachable")

    async def get_cash_balance(self) -> float:
        return 0.0

    async def get_order(self, order_id: str) -> ExecutionReport:
        del order_id
        if self._error is not None:
            raise self._error
        raise AssertionError("get_order should not be called without an explicit fixture")

    async def restore_order_context(self, order_id: str, intent: OrderIntent) -> None:
        self.restored_contexts.append((order_id, intent))

    async def list_open_orders(self) -> list[VenueOrder]:
        return list(self._open_orders)

    async def list_fills(self, since: datetime | None = None) -> list[FillRecord]:
        del since
        return list(self._fills)

    async def get_balances(self) -> dict[str, Decimal]:
        return {"cash": Decimal("0")}

    async def get_positions(self) -> dict[str, Decimal]:
        return dict(self._positions)

    def supports_full_reconciliation(self) -> bool:
        return True


class _FlakyStartupClient(_FakeClient):
    def __init__(self) -> None:
        super().__init__()
        self._open_orders_calls = 0

    async def list_open_orders(self) -> list[VenueOrder]:
        self._open_orders_calls += 1
        if self._open_orders_calls == 1:
            raise TimeoutError("transient startup timeout")
        return []


class _TransientContinuousClient(_FakeClient):
    def __init__(self, *, fail_after: int = 1) -> None:
        super().__init__()
        self._open_orders_calls = 0
        self._fail_after = fail_after

    async def list_open_orders(self) -> list[VenueOrder]:
        self._open_orders_calls += 1
        if self._open_orders_calls > self._fail_after:
            raise TimeoutError("transient continuous timeout")
        return []


class _FakeRepository:
    def __init__(self, unresolved: list[SimpleNamespace]) -> None:
        self._unresolved = unresolved
        self.updates: list[dict[str, object]] = []
        self.reconciliations: list[Any] = []
        self.audits: list[tuple[str, dict[str, object]]] = []

    async def unresolved_order_intents(self) -> list[SimpleNamespace]:
        return list(self._unresolved)

    async def client_order_id_for_venue_order(self, venue: str, venue_order_id: str) -> str | None:
        del venue, venue_order_id
        return None

    async def update_order_intent(
        self,
        client_order_id: str,
        status: OrderIntentStatus,
        *,
        venue_order_id: str | None = None,
        error: str | None = None,
    ) -> None:
        self.updates.append(
            {
                "client_order_id": client_order_id,
                "status": status,
                "venue_order_id": venue_order_id,
                "error": error,
            }
        )

    async def upsert_venue_order(self, order: object) -> None:
        del order

    async def insert_fill(self, fill: object) -> bool:
        del fill
        return False

    async def load_positions(self) -> list[OpenPosition]:
        return []

    async def record_balances(self, venue: str, balances: dict[str, Decimal]) -> None:
        del venue, balances

    async def audit(self, event_type: str, payload: dict[str, object], correlation_id: str | None = None) -> None:
        del correlation_id
        self.audits.append((event_type, payload))

    async def record_reconciliation(self, result: object) -> None:
        self.reconciliations.append(result)


@pytest.mark.asyncio
async def test_startup_reconcile_retires_synthetic_startup_artifacts() -> None:
    repository = _FakeRepository(
        [
            SimpleNamespace(
                client_order_id="restart-order",
                route="polymarket_myriad",
                market_key="restart:restart-order",
                venue="Myriad",
                token_id="restart-token",
                binary_side=BinarySide.YES.value,
                action="BUY",
                quantity=Decimal("1"),
                limit_price=Decimal("0.5"),
                venue_order_id="venue-restart-order",
                status=OrderIntentStatus.ACKNOWLEDGED.value,
                created_at=datetime(2026, 8, 20, 10),
                updated_at=datetime(2026, 8, 20, 10),
            ),
            SimpleNamespace(
                client_order_id="integration-order",
                route="polymarket_myriad",
                market_key="integration:integration-order",
                venue="Polymarket",
                token_id="integration-token",
                venue_order_id=None,
                status=OrderIntentStatus.MANUAL_REVIEW.value,
            ),
        ]
    )
    risk = GlobalRiskController(10, 3)
    service = ReconciliationService(
        repository,  # type: ignore[arg-type]
        {
            "Myriad": _FakeClient(error=_FakeNotFound()),
            "Polymarket": _FakeClient(error=None),
        },
        risk,
    )

    assert await service.startup_reconcile()
    assert service.ready
    assert not risk.is_paused()
    assert service.last_error is None
    assert len(repository.reconciliations) == 2
    assert repository.updates == [
        {
            "client_order_id": "restart-order",
            "status": OrderIntentStatus.CANCELLED,
            "venue_order_id": "venue-restart-order",
            "error": "retired synthetic startup artifact missing on venue",
        },
        {
            "client_order_id": "integration-order",
            "status": OrderIntentStatus.CANCELLED,
            "venue_order_id": None,
            "error": "retired synthetic startup artifact without venue order id",
        },
    ]


@pytest.mark.asyncio
async def test_startup_reconcile_keeps_real_missing_order_as_failure() -> None:
    repository = _FakeRepository(
        [
            SimpleNamespace(
                client_order_id="real-order",
                route="polymarket_myriad",
                market_key="real-market",
                venue="Myriad",
                token_id="real-token",
                binary_side=BinarySide.YES.value,
                action="BUY",
                quantity=Decimal("1"),
                limit_price=Decimal("0.5"),
                venue_order_id="venue-real-order",
                status=OrderIntentStatus.ACKNOWLEDGED.value,
                created_at=datetime(2026, 8, 20, 10),
                updated_at=datetime(2026, 8, 20, 10),
            )
        ]
    )
    risk = GlobalRiskController(10, 3)
    client = _FakeClient(error=_FakeNotFound("404 venue missing"))
    service = ReconciliationService(
        repository,  # type: ignore[arg-type]
        {"Myriad": client},
        risk,
    )

    assert not await service.startup_reconcile()
    assert not service.ready
    assert risk.is_paused()
    assert service.last_error is not None
    assert "404 venue missing" in service.last_error
    assert repository.updates == []
    assert client.restored_contexts[0][0] == "venue-real-order"
    assert client.restored_contexts[0][1].action == "BUY"


@pytest.mark.asyncio
async def test_startup_reconcile_ignores_untracked_external_orders_fills_and_positions() -> None:
    repository = _FakeRepository([])
    risk = GlobalRiskController(10, 3)
    service = ReconciliationService(
        repository,  # type: ignore[arg-type]
        {
            "Myriad": _FakeClient(
                open_orders=[
                    VenueOrder(
                        client_order_id="",
                        venue_order_id="venue-external-order",
                        venue="Myriad",
                        status=OrderIntentStatus.ACKNOWLEDGED,
                        quantity=Decimal("1"),
                        cumulative_filled=Decimal("0"),
                        average_price=Decimal("0.4"),
                        updated_at=datetime.now(),
                    )
                ],
                fills=[
                    FillRecord(
                        fill_id="fill-external",
                        client_order_id="",
                        venue_order_id="venue-external-order",
                        venue="Myriad",
                        quantity=Decimal("1"),
                        price=Decimal("0.4"),
                        fee=Decimal("0"),
                        occurred_at=datetime.now(),
                    )
                ],
                positions={"external-token": Decimal("12.5")},
            )
        },
        risk,
    )

    assert await service.startup_reconcile()
    assert service.ready
    assert not risk.is_paused()
    result = repository.reconciliations[0]
    assert result.drift_count == 0
    assert (
        "untracked_open_orders",
        {"venue": "Myriad", "count": 1, "sample_venue_order_ids": ["venue-external-order"]},
    ) in repository.audits
    assert (
        "untracked_fills",
        {"venue": "Myriad", "count": 1, "sample_fill_refs": ["fill-external"]},
    ) in repository.audits


@pytest.mark.asyncio
async def test_startup_reconcile_retries_transient_venue_failure() -> None:
    repository = _FakeRepository([])
    risk = GlobalRiskController(10, 3)
    service = ReconciliationService(
        repository,  # type: ignore[arg-type]
        {"Predict.fun": _FlakyStartupClient()},
        risk,
        startup_retry_attempts=2,
        startup_retry_delay_seconds=0.0,
    )

    assert await service.startup_reconcile()
    assert service.ready
    assert not risk.is_paused()
    assert service.last_error is None
    assert len(repository.reconciliations) == 2


@pytest.mark.asyncio
async def test_run_once_keeps_service_ready_on_single_transient_failure_after_success() -> None:
    repository = _FakeRepository([])
    risk = GlobalRiskController(10, 3)
    service = ReconciliationService(
        repository,  # type: ignore[arg-type]
        {"Predict.fun": _TransientContinuousClient(fail_after=1)},
        risk,
        transient_failure_pause_threshold=3,
    )

    first = await service.run_once(full=True)
    assert service.ready
    assert first[0].success
    assert not first[0].transient_failure

    second = await service.run_once(full=True)
    assert service.ready
    assert not second[0].success
    assert second[0].transient_failure
    assert not risk.is_paused()
    assert service.last_error is not None
    assert "transient continuous timeout" in service.last_error


@pytest.mark.asyncio
async def test_run_once_marks_service_not_ready_after_repeated_transient_failures() -> None:
    repository = _FakeRepository([])
    risk = GlobalRiskController(10, 3)
    service = ReconciliationService(
        repository,  # type: ignore[arg-type]
        {"Predict.fun": _TransientContinuousClient(fail_after=0)},
        risk,
        transient_failure_pause_threshold=2,
    )

    first = await service.run_once(full=True)
    assert not first[0].success
    assert first[0].transient_failure
    assert not service.ready

    second = await service.run_once(full=True)
    assert not second[0].success
    assert second[0].transient_failure
    assert not service.ready
    assert not risk.is_paused()


def test_expected_positions_follow_predict_myriad_route_shape() -> None:
    market = MarketSpec(
        symbol="BTC-USD",
        target_label=">$75,000",
        polymarket_token_id="predict-token",
        polymarket_side=BinarySide.NO,
        predict_fun_token_id="1335:YES",
        predict_fun_side=BinarySide.YES,
        venue_a_label="Predict.fun",
        venue_b_label="Myriad",
        predict_fun_market_id="predict-market",
        myriad_market_id="1335",
        myriad_side=BinarySide.NO,
    )
    position = OpenPosition(
        market=market,
        polymarket_contracts=Decimal("10"),
        polymarket_entry_price=Decimal("0.42"),
        predict_fun_contracts=Decimal("10"),
        predict_fun_entry_price=Decimal("0.50"),
        opened_at=datetime.now(),
        polymarket_order_id="predict-entry-1",
        predict_fun_order_id="myriad-entry-1",
        polymarket_closed_contracts=Decimal("2"),
        predict_fun_closed_contracts=Decimal("3"),
    )

    assert _expected_positions("Predict.fun", [position]) == {"predict-token": Decimal("8")}
    assert _expected_positions("Myriad", [position]) == {"1335:YES": Decimal("7")}
