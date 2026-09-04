from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from arbitrage_engine.connectors.base import (
    BinaryMarketClient,
    OrderResidualExposure,
    OrderResidualExposureBatch,
)
from arbitrage_engine.database import _position_after_residual_exit
from arbitrage_engine.models import (
    BinarySide,
    ExecutionReport,
    ExecutionStatus,
    ExternalAccountBaseline,
    FillRecord,
    MarketSpec,
    OpenPosition,
    OrderBook,
    OrderIntent,
    OrderIntentStatus,
    VenueOrder,
)
from arbitrage_engine.reconciliation import (
    ReconciliationService,
    _expected_positions,
    _is_transient_reconciliation_exception,
)
from arbitrage_engine.risk import GlobalRiskController


class _FakeNotFound(RuntimeError):
    def __init__(self, message: str = "404 not found") -> None:
        super().__init__(message)
        self.status = 404


class _FakeHttpError(RuntimeError):
    def __init__(self, status: int) -> None:
        super().__init__(f"HTTP {status}")
        self.status = status


class _FakeClient(BinaryMarketClient):
    def __init__(
        self,
        *,
        error: Exception | None = None,
        open_orders: list[VenueOrder] | None = None,
        fills: list[FillRecord] | None = None,
        fills_error: Exception | None = None,
        positions: dict[str, Decimal] | None = None,
        account_fingerprint: str | None = None,
    ) -> None:
        self._error = error
        self._open_orders = open_orders or []
        self._fills = fills or []
        self._fills_error = fills_error
        self._positions = positions or {}
        self._account_fingerprint = account_fingerprint
        self.restored_contexts: list[tuple[str, OrderIntent]] = []
        self.restored_fill_contexts: list[tuple[str, OrderIntent]] = []

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

    async def restore_fill_context(self, order_id: str, intent: OrderIntent) -> None:
        self.restored_fill_contexts.append((order_id, intent))

    async def list_open_orders(self) -> list[VenueOrder]:
        return list(self._open_orders)

    async def list_fills(self, since: datetime | None = None) -> list[FillRecord]:
        del since
        if self._fills_error is not None:
            raise self._fills_error
        return list(self._fills)

    async def get_balances(self) -> dict[str, Decimal]:
        return {"cash": Decimal("0")}

    async def get_positions(self) -> dict[str, Decimal]:
        return dict(self._positions)

    def supports_full_reconciliation(self) -> bool:
        return True

    def reconciliation_account_fingerprint(self) -> str | None:
        return self._account_fingerprint


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


class _TimestampFilteringClient(_FakeClient):
    def __init__(self) -> None:
        super().__init__()
        self.fills: list[FillRecord] = []
        self.fill_since_values: list[datetime | None] = []
        self.first_fill_query = asyncio.Event()

    async def list_fills(self, since: datetime | None = None) -> list[FillRecord]:
        self.fill_since_values.append(since)
        self.first_fill_query.set()
        if since is None:
            return list(self.fills)
        return [fill for fill in self.fills if fill.occurred_at >= since]


class _GapFillClient(_FakeClient):
    def __init__(self, first_client: _TimestampFilteringClient) -> None:
        super().__init__()
        self._first_client = first_client
        self.injected_at: datetime | None = None
        self._injected = False

    async def list_fills(self, since: datetime | None = None) -> list[FillRecord]:
        del since
        if not self._injected:
            await self._first_client.first_fill_query.wait()
            self.injected_at = datetime.now(UTC)
            self._first_client.fills.append(
                FillRecord(
                    fill_id="fill-created-between-venue-queries",
                    client_order_id="external",
                    venue_order_id="external-order",
                    venue="Polymarket",
                    quantity=Decimal("1"),
                    price=Decimal("0.4"),
                    fee=Decimal("0"),
                    occurred_at=self.injected_at,
                )
            )
            self._injected = True
            await asyncio.sleep(0.01)
        return []


class _FakeRepository:
    def __init__(
        self,
        unresolved: list[SimpleNamespace],
        *,
        venue_order_links: dict[tuple[str, str], str] | None = None,
        fill_contexts: list[SimpleNamespace] | None = None,
        external_baseline: ExternalAccountBaseline | None = None,
    ) -> None:
        self._unresolved = unresolved
        self._venue_order_links = venue_order_links or {}
        self._fill_contexts = fill_contexts or []
        self._external_baseline = external_baseline
        self.updates: list[dict[str, object]] = []
        self.venue_orders: list[VenueOrder] = []
        self.reconciliations: list[Any] = []
        self.audits: list[tuple[str, dict[str, object]]] = []
        self.residual_exposures: list[dict[str, object]] = []
        self.fill_insert_attempts: list[FillRecord] = []

    async def unresolved_order_intents(self) -> list[SimpleNamespace]:
        return list(self._unresolved)

    async def active_external_account_baseline(self, venue: str) -> ExternalAccountBaseline | None:
        del venue
        return self._external_baseline

    async def order_intents_for_fill_reconciliation(
        self,
        venue: str,
        since: datetime | None,
    ) -> list[SimpleNamespace]:
        del since
        return [row for row in self._fill_contexts if row.venue == venue]

    async def client_order_id_for_venue_order(self, venue: str, venue_order_id: str) -> str | None:
        return self._venue_order_links.get((venue, venue_order_id))

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

    async def upsert_venue_order(self, order: VenueOrder) -> None:
        self.venue_orders.append(order)

    async def insert_fill(self, fill: FillRecord) -> bool:
        self.fill_insert_attempts.append(fill)
        return False

    async def record_residual_exit_exposure(self, **kwargs: object) -> bool:
        self.residual_exposures.append(kwargs)
        return True

    async def load_positions(self) -> list[OpenPosition]:
        return []

    async def record_balances(self, venue: str, balances: dict[str, Decimal]) -> None:
        del venue, balances

    async def audit(self, event_type: str, payload: dict[str, object], correlation_id: str | None = None) -> None:
        del correlation_id
        self.audits.append((event_type, payload))

    async def record_reconciliation(self, result: object) -> None:
        self.reconciliations.append(result)


def _external_baseline(*, position: Decimal = Decimal("2.5")) -> ExternalAccountBaseline:
    return ExternalAccountBaseline(
        manifest_sha256="a" * 64,
        runtime_instance_id="quote_arb",
        venue="Polymarket",
        account_fingerprint="b" * 64,
        positions=(("personal-token", position),),
        fill_refs=("personal-fill",),
        captured_at=datetime.now(UTC),
        operator="test-operator",
    )


def _external_fill(fill_id: str) -> FillRecord:
    return FillRecord(
        fill_id=fill_id,
        client_order_id="",
        venue_order_id=f"order-{fill_id}",
        venue="Polymarket",
        quantity=Decimal("2.5"),
        price=Decimal("0.4"),
        fee=Decimal("0"),
        occurred_at=datetime.now(UTC),
    )


@pytest.mark.asyncio
async def test_exact_external_baseline_keeps_unchanged_personal_state_clean() -> None:
    repository = _FakeRepository([], external_baseline=_external_baseline())
    client = _FakeClient(
        positions={"personal-token": Decimal("2.5")},
        fills=[_external_fill("personal-fill")],
        account_fingerprint="b" * 64,
    )
    risk = GlobalRiskController(10, 3)
    service = ReconciliationService(
        repository,  # type: ignore[arg-type]
        {"Polymarket": client},
        risk,
        startup_retry_attempts=1,
    )

    assert await service.startup_reconcile()
    result = repository.reconciliations[-1]

    assert result.success
    assert result.drift_count == 0
    assert not risk.is_paused()
    assert result.account_fingerprint == "b" * 64
    assert result.external_baseline_manifest_sha256 == "a" * 64


@pytest.mark.asyncio
async def test_external_baseline_fails_closed_when_personal_position_changes() -> None:
    repository = _FakeRepository([], external_baseline=_external_baseline())
    client = _FakeClient(
        positions={"personal-token": Decimal("2.4")},
        fills=[_external_fill("personal-fill")],
        account_fingerprint="b" * 64,
    )
    risk = GlobalRiskController(10, 3)
    service = ReconciliationService(
        repository,  # type: ignore[arg-type]
        {"Polymarket": client},
        risk,
        startup_retry_attempts=1,
    )

    assert not await service.startup_reconcile()
    result = repository.reconciliations[-1]

    assert result.success
    assert result.drift_count == 1
    assert risk.is_paused()


@pytest.mark.asyncio
async def test_external_baseline_fails_closed_when_new_personal_position_appears() -> None:
    repository = _FakeRepository([], external_baseline=_external_baseline())
    client = _FakeClient(
        positions={
            "personal-token": Decimal("2.5"),
            "new-personal-token": Decimal("1"),
        },
        fills=[_external_fill("personal-fill")],
        account_fingerprint="b" * 64,
    )
    risk = GlobalRiskController(10, 3)
    service = ReconciliationService(
        repository,  # type: ignore[arg-type]
        {"Polymarket": client},
        risk,
        startup_retry_attempts=1,
    )

    assert not await service.startup_reconcile()
    result = repository.reconciliations[-1]

    assert result.success
    assert result.drift_count == 1
    assert risk.is_paused()


@pytest.mark.asyncio
async def test_external_baseline_fails_closed_on_new_untracked_fill() -> None:
    repository = _FakeRepository([], external_baseline=_external_baseline())
    client = _FakeClient(
        positions={"personal-token": Decimal("2.5")},
        fills=[_external_fill("personal-fill"), _external_fill("new-fill")],
        account_fingerprint="b" * 64,
    )
    risk = GlobalRiskController(10, 3)
    service = ReconciliationService(
        repository,  # type: ignore[arg-type]
        {"Polymarket": client},
        risk,
        startup_retry_attempts=1,
    )

    assert not await service.startup_reconcile()
    result = repository.reconciliations[-1]

    assert result.success
    assert result.drift_count == 1
    assert risk.is_paused()
    assert (
        "untracked_fills",
        {"venue": "Polymarket", "count": 1, "sample_fill_refs": ["new-fill"]},
    ) in repository.audits


@pytest.mark.asyncio
async def test_external_baseline_fails_closed_on_wrong_account_fingerprint() -> None:
    repository = _FakeRepository([], external_baseline=_external_baseline())
    client = _FakeClient(
        positions={"personal-token": Decimal("2.5")},
        account_fingerprint="c" * 64,
    )
    risk = GlobalRiskController(10, 3)
    service = ReconciliationService(
        repository,  # type: ignore[arg-type]
        {"Polymarket": client},
        risk,
        startup_retry_attempts=1,
    )

    assert not await service.startup_reconcile()
    result = repository.reconciliations[-1]

    assert not result.success
    assert "account identity changed" in str(result.error)
    assert risk.is_paused()


@pytest.mark.asyncio
async def test_revoked_external_baseline_no_longer_whitelists_personal_position() -> None:
    repository = _FakeRepository([], external_baseline=None)
    client = _FakeClient(positions={"personal-token": Decimal("2.5")})
    risk = GlobalRiskController(10, 3)
    service = ReconciliationService(
        repository,  # type: ignore[arg-type]
        {"Polymarket": client},
        risk,
        startup_retry_attempts=1,
    )

    assert not await service.startup_reconcile()
    result = repository.reconciliations[-1]

    assert result.success
    assert result.drift_count == 1
    assert risk.is_paused()


@pytest.mark.asyncio
async def test_reconciliation_cycle_watermark_cannot_skip_fill_between_venue_queries() -> None:
    repository = _FakeRepository([])
    first = _TimestampFilteringClient()
    second = _GapFillClient(first)
    risk = GlobalRiskController(10, 3)
    service = ReconciliationService(
        repository,  # type: ignore[arg-type]
        {"Polymarket": first, "Predict.fun": second},
        risk,
    )

    clean_results = await service.run_once(full=False)
    assert all(result.success and result.drift_count == 0 for result in clean_results)
    assert second.injected_at is not None

    next_results = await service.run_once(full=False)

    polymarket_result = next(result for result in next_results if result.venue == "Polymarket")
    assert polymarket_result.drift_count == 1
    assert first.fill_since_values[-1] is not None
    assert first.fill_since_values[-1] <= second.injected_at


@pytest.mark.asyncio
async def test_reconciliation_overlap_catches_late_visible_fill_with_older_timestamp() -> None:
    repository = _FakeRepository([])
    client = _TimestampFilteringClient()
    risk = GlobalRiskController(10, 3)
    service = ReconciliationService(
        repository,  # type: ignore[arg-type]
        {"Polymarket": client},
        risk,
    )
    occurred_at = datetime.now(UTC) - timedelta(minutes=1)

    clean_results = await service.run_once(full=False)
    assert clean_results[0].success
    assert clean_results[0].drift_count == 0

    client.fills.append(
        FillRecord(
            fill_id="late-visible-fill",
            client_order_id="external",
            venue_order_id="external-order",
            venue="Polymarket",
            quantity=Decimal("1"),
            price=Decimal("0.4"),
            fee=Decimal("0"),
            occurred_at=occurred_at,
        )
    )
    late_results = await service.run_once(full=False)

    assert late_results[0].drift_count == 1
    assert client.fill_since_values[-1] is not None
    assert client.fill_since_values[-1] <= occurred_at


@pytest.mark.asyncio
async def test_initial_baseline_reconciliation_looks_back_to_baseline_capture() -> None:
    baseline = ExternalAccountBaseline(
        **{
            **_external_baseline().__dict__,
            "captured_at": datetime.now(UTC) - timedelta(days=30),
        }
    )
    repository = _FakeRepository([], external_baseline=baseline)
    client = _TimestampFilteringClient()
    client._account_fingerprint = baseline.account_fingerprint
    client._positions = baseline.position_map()
    risk = GlobalRiskController(10, 3)
    service = ReconciliationService(
        repository,  # type: ignore[arg-type]
        {"Polymarket": client},
        risk,
        startup_retry_delay_seconds=0,
    )

    assert await service.startup_reconcile()
    assert client.fill_since_values[0] is not None
    assert client.fill_since_values[0] <= baseline.captured_at


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
async def test_terminal_order_action_is_restored_before_account_fill_listing() -> None:
    terminal = SimpleNamespace(
        client_order_id="terminal-sell",
        route="polymarket_sx",
        market_key="market-key",
        venue="SX Bet",
        token_id=f"{'0x' + ('2' * 64)}:YES",
        binary_side=BinarySide.YES.value,
        action="SELL",
        quantity=Decimal("10"),
        limit_price=Decimal("0.4"),
        venue_order_id="0x" + ("3" * 64),
        status=OrderIntentStatus.FILLED.value,
        created_at=datetime(2026, 8, 20, 10, tzinfo=UTC),
        updated_at=datetime(2026, 8, 20, 10, tzinfo=UTC),
    )
    repository = _FakeRepository([], fill_contexts=[terminal])
    client = _FakeClient()
    service = ReconciliationService(
        repository,  # type: ignore[arg-type]
        {"SX Bet": client},
        GlobalRiskController(10, 3),
    )

    assert await service.startup_reconcile()
    assert len(client.restored_fill_contexts) == 1
    assert client.restored_fill_contexts[0][0] == terminal.venue_order_id
    assert client.restored_fill_contexts[0][1].action == "SELL"


@pytest.mark.asyncio
async def test_account_fill_residual_is_persisted_and_pauses_reconciliation() -> None:
    class ResidualOnceClient(_FakeClient):
        def __init__(self, residual_error: OrderResidualExposure) -> None:
            super().__init__()
            self._residual_error = residual_error
            self._fill_calls = 0

        async def list_fills(self, since: datetime | None = None) -> list[FillRecord]:
            del since
            self._fill_calls += 1
            if self._fill_calls == 1:
                raise self._residual_error
            return []

    order_id = "0x" + ("4" * 64)
    terminal = SimpleNamespace(
        client_order_id="terminal-residual-sell",
        route="polymarket_sx",
        market_key="market-key",
        venue="SX Bet",
        token_id=f"{'0x' + ('2' * 64)}:YES",
        binary_side=BinarySide.YES.value,
        action="SELL",
        quantity=Decimal("10"),
        limit_price=Decimal("0.4"),
        venue_order_id=order_id,
        status=OrderIntentStatus.FILLED.value,
        created_at=datetime(2026, 8, 20, 10, tzinfo=UTC),
        updated_at=datetime(2026, 8, 20, 10, tzinfo=UTC),
    )
    residual = OrderResidualExposure(
        "historical CE fill created residual opposite exposure",
        report=ExecutionReport.from_amounts(
            order_id,
            Decimal("10"),
            Decimal("5"),
            ExecutionStatus.PARTIAL,
            Decimal("0.39"),
        ),
        residual_contracts=Decimal("5"),
        residual_side=BinarySide.NO,
    )
    repository = _FakeRepository([], fill_contexts=[terminal])
    risk = GlobalRiskController(10, 3)
    client = ResidualOnceClient(residual)
    service = ReconciliationService(
        repository,  # type: ignore[arg-type]
        {"SX Bet": client},
        risk,
        startup_retry_attempts=1,
    )

    callback_completed = False

    async def reconcile_after_pause() -> None:
        nonlocal callback_completed
        await service.run_once(full=True)
        callback_completed = True

    risk.register_pause_callback(reconcile_after_pause)

    assert not await asyncio.wait_for(service.startup_reconcile(), timeout=1.0)
    assert risk.is_paused()
    assert callback_completed
    assert repository.residual_exposures == [
        {
            "market_key": "market-key",
            "venue": "SX Bet",
            "requested_contracts": Decimal("10"),
            "report": residual.report,
            "residual_contracts": Decimal("5"),
        }
    ]
    assert repository.updates[-1]["status"] is OrderIntentStatus.MANUAL_REVIEW
    assert any(event_type == "residual_exit_exposure_detected" for event_type, _ in repository.audits)


@pytest.mark.asyncio
async def test_account_fill_residual_batch_persists_every_order() -> None:
    first_order_id = "0x" + ("5" * 64)
    second_order_id = "0x" + ("6" * 64)

    def terminal(order_id: str, client_order_id: str, market_key: str) -> SimpleNamespace:
        return SimpleNamespace(
            client_order_id=client_order_id,
            route="polymarket_sx",
            market_key=market_key,
            venue="SX Bet",
            token_id=f"{'0x' + ('2' * 64)}:YES",
            binary_side=BinarySide.YES.value,
            action="SELL",
            quantity=Decimal("10"),
            limit_price=Decimal("0.4"),
            venue_order_id=order_id,
            status=OrderIntentStatus.FILLED.value,
            created_at=datetime(2026, 8, 20, 10, tzinfo=UTC),
            updated_at=datetime(2026, 8, 20, 10, tzinfo=UTC),
        )

    def residual(order_id: str, amount: str) -> OrderResidualExposure:
        return OrderResidualExposure(
            "historical CE fill created residual opposite exposure",
            report=ExecutionReport.from_amounts(
                order_id,
                Decimal("10"),
                Decimal(amount),
                ExecutionStatus.PARTIAL,
                Decimal("0.39") if Decimal(amount) > 0 else Decimal(0),
            ),
            residual_contracts=Decimal("10") - Decimal(amount),
            residual_side=BinarySide.NO,
        )

    contexts = [
        terminal(first_order_id, "first-residual", "first-market"),
        terminal(second_order_id, "second-residual", "second-market"),
    ]
    batch = OrderResidualExposureBatch(
        [residual(first_order_id, "5"), residual(second_order_id, "4")],
        fills=[
            FillRecord(
                fill_id="first-residual-fill",
                client_order_id="",
                venue_order_id=first_order_id,
                venue="SX Bet",
                quantity=Decimal("5"),
                price=Decimal("0.39"),
                fee=Decimal("0.05"),
                occurred_at=datetime(2026, 8, 20, 10, tzinfo=UTC),
            )
        ],
    )
    repository = _FakeRepository(
        [],
        venue_order_links={("SX Bet", first_order_id): "first-residual"},
        fill_contexts=contexts,
    )
    risk = GlobalRiskController(10, 3)
    service = ReconciliationService(
        repository,  # type: ignore[arg-type]
        {"SX Bet": _FakeClient(fills_error=batch)},
        risk,
        startup_retry_attempts=1,
    )

    assert not await service.startup_reconcile()
    assert risk.is_paused()
    assert {item["market_key"] for item in repository.residual_exposures} == {
        "first-market",
        "second-market",
    }
    manual_reviews = [
        item for item in repository.updates if item["status"] is OrderIntentStatus.MANUAL_REVIEW
    ]
    assert {item["client_order_id"] for item in manual_reviews} == {
        "first-residual",
        "second-residual",
    }
    assert [fill.fill_id for fill in repository.fill_insert_attempts] == ["first-residual-fill"]


def test_residual_exit_position_accounting_is_absolute_and_idempotent() -> None:
    market = MarketSpec(
        symbol="Poly-SX",
        target_label="Home",
        polymarket_token_id="poly-token",
        polymarket_side=BinarySide.YES,
        predict_fun_token_id="sx-token",
        predict_fun_side=BinarySide.NO,
        venue_a_label="Polymarket",
        venue_b_label="SX Bet",
    )
    position = OpenPosition(
        market=market,
        polymarket_contracts=Decimal("10"),
        polymarket_entry_price=Decimal("0.45"),
        predict_fun_contracts=Decimal("10"),
        predict_fun_entry_price=Decimal("0.55"),
        opened_at=datetime.now(UTC),
        polymarket_order_id="poly-entry",
        predict_fun_order_id="sx-entry",
        predict_fun_closed_contracts=Decimal("2"),
        predict_fun_exit_proceeds_usd=Decimal("0.50"),
    )
    kwargs = {
        "venue": "SX Bet",
        "venue_order_id": "sx-residual-exit-1",
        "requested_contracts": Decimal("8"),
        "closed_contracts": Decimal("4"),
        "average_exit_price": Decimal("0.39"),
        "residual_contracts": Decimal("2"),
    }

    updated = _position_after_residual_exit(position, **kwargs)  # type: ignore[arg-type]
    repeated = _position_after_residual_exit(updated, **kwargs)  # type: ignore[arg-type]
    expanded = _position_after_residual_exit(
        repeated,
        venue="SX Bet",
        venue_order_id="sx-residual-exit-1",
        requested_contracts=Decimal("8"),
        closed_contracts=Decimal("5"),
        average_exit_price=Decimal("0.4"),
        residual_contracts=Decimal("3"),
    )
    stale_replay = _position_after_residual_exit(expanded, **kwargs)  # type: ignore[arg-type]
    second_order = _position_after_residual_exit(
        stale_replay,
        venue="SX Bet",
        venue_order_id="sx-residual-exit-2",
        requested_contracts=Decimal("4"),
        closed_contracts=Decimal(0),
        average_exit_price=Decimal(0),
        residual_contracts=Decimal("3"),
    )

    assert updated.status == "manual_review"
    assert updated.predict_fun_closed_contracts == Decimal("6")
    assert updated.predict_fun_exit_proceeds_usd == Decimal("2.06")
    assert updated.predict_fun_residual_exposure_contracts == Decimal("2")
    assert updated.predict_fun_residual_exit_order_ids == ("sx-residual-exit-1",)
    assert repeated == updated
    assert expanded.predict_fun_closed_contracts == Decimal("7")
    assert expanded.predict_fun_exit_proceeds_usd == Decimal("2.50")
    assert expanded.predict_fun_residual_exposure_contracts == Decimal("3")
    assert stale_replay == expanded
    assert second_order.predict_fun_residual_exposure_contracts == Decimal("6")
    assert second_order.predict_fun_residual_exit_order_ids == (
        "sx-residual-exit-1",
        "sx-residual-exit-2",
    )
    with pytest.raises(RuntimeError, match="mixed cumulative regression"):
        _position_after_residual_exit(
            expanded,
            venue="SX Bet",
            venue_order_id="sx-residual-exit-1",
            requested_contracts=Decimal("8"),
            closed_contracts=Decimal("6"),
            average_exit_price=Decimal("0.4"),
            residual_contracts=Decimal("2"),
        )


@pytest.mark.asyncio
async def test_startup_reconcile_fails_closed_on_untracked_orders_fills_and_positions() -> None:
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

    assert not await service.startup_reconcile()
    assert not service.ready
    assert risk.is_paused()
    result = repository.reconciliations[-1]
    assert result.drift_count == 3
    assert (
        "untracked_open_orders",
        {"venue": "Myriad", "count": 1, "sample_venue_order_ids": ["venue-external-order"]},
    ) in repository.audits
    assert (
        "untracked_fills",
        {"venue": "Myriad", "count": 1, "sample_fill_refs": ["fill-external"]},
    ) in repository.audits
    assert (
        "venue_positions_snapshot",
        {
            "venue": "Myriad",
            "positions": {"external-token": "12.5"},
            "mismatches": {
                "external-token": {
                    "expected": "0",
                    "actual": "12.5",
                }
            },
        },
    ) in repository.audits


@pytest.mark.asyncio
async def test_continuous_reconcile_defers_fresh_inflight_submission_race() -> None:
    venue_order_id = "venue-inflight-order"
    now = datetime.now(UTC)
    repository = _FakeRepository(
        [
            SimpleNamespace(
                client_order_id="client-inflight-order",
                route="polymarket_predict",
                market_key="live-market",
                venue="Predict.fun",
                token_id="predict-token",
                binary_side=BinarySide.YES.value,
                action="BUY",
                quantity=Decimal("10"),
                limit_price=Decimal("0.4"),
                venue_order_id=None,
                status=OrderIntentStatus.SUBMITTING.value,
                created_at=now,
                updated_at=now,
            )
        ],
        venue_order_links={("Predict.fun", venue_order_id): "client-inflight-order"},
    )
    client = _FakeClient(
        open_orders=[
            VenueOrder(
                client_order_id="client-inflight-order",
                venue_order_id=venue_order_id,
                venue="Predict.fun",
                status=OrderIntentStatus.ACKNOWLEDGED,
                quantity=Decimal("10"),
                cumulative_filled=Decimal("0"),
                average_price=Decimal("0.4"),
                updated_at=now,
            )
        ],
        fills=[
            FillRecord(
                fill_id="fill-inflight-order",
                client_order_id="client-inflight-order",
                venue_order_id=venue_order_id,
                venue="Predict.fun",
                quantity=Decimal("10"),
                price=Decimal("0.4"),
                fee=Decimal("0"),
                occurred_at=now,
            )
        ],
    )
    service = ReconciliationService(
        repository,  # type: ignore[arg-type]
        {"Predict.fun": client},
        GlobalRiskController(10, 3),
    )

    result = (await service.run_once(full=False))[0]

    assert result.success
    assert result.drift_count == 0
    assert repository.updates == []
    assert (
        "inflight_submission_reconciliation_deferred",
        {
            "venue": "Predict.fun",
            "count": 1,
            "sample_venue_order_ids": [venue_order_id],
        },
    ) in repository.audits


@pytest.mark.asyncio
async def test_continuous_reconcile_does_not_race_created_to_submitting_transition() -> None:
    created_visible = asyncio.Event()
    release_transition = asyncio.Event()
    repository = _FakeRepository([])
    now = datetime.now(UTC)
    row = SimpleNamespace(
        client_order_id="client-created-window",
        route="polymarket_predict",
        market_key="live-market",
        venue="Predict.fun",
        token_id="predict-token",
        binary_side=BinarySide.YES.value,
        action="BUY",
        quantity=Decimal("10"),
        limit_price=Decimal("0.4"),
        venue_order_id=None,
        status=OrderIntentStatus.PREPARED.value,
        created_at=now,
        updated_at=now,
    )

    async def expose_committed_created_then_advance() -> None:
        repository._unresolved.append(row)  # noqa: SLF001
        created_visible.set()
        await release_transition.wait()
        row.status = OrderIntentStatus.SUBMITTING.value
        row.updated_at = datetime.now(UTC)

    transition = asyncio.create_task(expose_committed_created_then_advance())
    await asyncio.wait_for(created_visible.wait(), timeout=1.0)
    risk = GlobalRiskController(10, 3)
    service = ReconciliationService(
        repository,  # type: ignore[arg-type]
        {"Predict.fun": _FakeClient()},
        risk,
    )
    try:
        result = (await service.run_once(full=False))[0]
    finally:
        release_transition.set()
        await transition

    assert result.success
    assert result.drift_count == 0
    assert repository.updates == []
    assert not risk.is_paused()
    assert row.status == OrderIntentStatus.SUBMITTING.value


@pytest.mark.asyncio
async def test_recent_prepared_intent_does_not_hide_unrelated_venue_order() -> None:
    now = datetime.now(UTC)
    repository = _FakeRepository(
        [
            SimpleNamespace(
                client_order_id="client-not-yet-submitted",
                route="polymarket_predict",
                market_key="live-market",
                venue="Predict.fun",
                token_id="predict-token",
                binary_side=BinarySide.YES.value,
                action="BUY",
                quantity=Decimal("10"),
                limit_price=Decimal("0.4"),
                venue_order_id=None,
                status=OrderIntentStatus.PREPARED.value,
                created_at=now,
                updated_at=now,
            )
        ]
    )
    unrelated_order_id = "unrelated-venue-order"
    client = _FakeClient(
        open_orders=[
            VenueOrder(
                client_order_id="external-client-id",
                venue_order_id=unrelated_order_id,
                venue="Predict.fun",
                status=OrderIntentStatus.ACKNOWLEDGED,
                quantity=Decimal("1"),
                cumulative_filled=Decimal("0"),
                average_price=Decimal("0.4"),
                updated_at=now,
            )
        ]
    )
    risk = GlobalRiskController(10, 3)
    service = ReconciliationService(
        repository,  # type: ignore[arg-type]
        {"Predict.fun": client},
        risk,
    )

    result = (await service.run_once(full=False))[0]

    assert result.drift_count == 1
    assert result.success
    assert not service.ready
    assert (
        "untracked_open_orders",
        {
            "venue": "Predict.fun",
            "count": 1,
            "sample_venue_order_ids": [unrelated_order_id],
        },
    ) in repository.audits


@pytest.mark.parametrize(
    "status",
    [OrderIntentStatus.PREPARED.value, OrderIntentStatus.SUBMITTING.value],
)
@pytest.mark.asyncio
async def test_startup_reconcile_never_graces_missing_venue_order_id(status: str) -> None:
    now = datetime.now(UTC)
    repository = _FakeRepository(
        [
            SimpleNamespace(
                client_order_id="client-crashed-submission",
                route="polymarket_predict",
                market_key="live-market",
                venue="Predict.fun",
                token_id="predict-token",
                binary_side=BinarySide.YES.value,
                action="BUY",
                quantity=Decimal("10"),
                limit_price=Decimal("0.4"),
                venue_order_id=None,
                status=status,
                created_at=now,
                updated_at=now,
            )
        ]
    )
    risk = GlobalRiskController(10, 3)
    service = ReconciliationService(
        repository,  # type: ignore[arg-type]
        {"Predict.fun": _FakeClient()},
        risk,
        startup_retry_attempts=1,
    )

    assert not await service.startup_reconcile()
    assert risk.is_paused()
    assert repository.updates == [
        {
            "client_order_id": "client-crashed-submission",
            "status": OrderIntentStatus.MANUAL_REVIEW,
            "venue_order_id": None,
            "error": "submission outcome unknown and venue order id is unavailable",
        }
    ]


@pytest.mark.asyncio
async def test_reconcile_prefers_durable_venue_order_mapping_over_remote_client_id() -> None:
    venue_order_id = "venue-order"
    repository = _FakeRepository(
        [],
        venue_order_links={("SX Bet", venue_order_id): "durable-client-id"},
    )
    client = _FakeClient(
        open_orders=[
            VenueOrder(
                client_order_id="signed-digest",
                venue_order_id=venue_order_id,
                venue="SX Bet",
                status=OrderIntentStatus.ACKNOWLEDGED,
                quantity=Decimal("1"),
                cumulative_filled=Decimal("0"),
                average_price=Decimal("0.4"),
                updated_at=datetime.now(),
            )
        ]
    )
    service = ReconciliationService(
        repository,  # type: ignore[arg-type]
        {"SX Bet": client},
        GlobalRiskController(10, 3),
    )

    result = (await service.run_once(full=False))[0]

    assert result.drift_count == 1
    assert repository.venue_orders[0].client_order_id == "durable-client-id"
    assert {update["client_order_id"] for update in repository.updates} == {"durable-client-id"}


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
async def test_run_once_fails_readiness_on_single_transient_failure_after_success() -> None:
    repository = _FakeRepository([])
    risk = GlobalRiskController(10, 3)
    service = ReconciliationService(
        repository,  # type: ignore[arg-type]
        {"Predict.fun": _TransientContinuousClient(fail_after=1)},
        risk,
        transient_failure_pause_threshold=3,
    )

    first = await service.run_once(full=True)
    first_cycle_ready = service.ready
    assert first_cycle_ready
    assert first[0].success
    assert not first[0].transient_failure

    second = await service.run_once(full=True)
    assert not service.ready
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


@pytest.mark.asyncio
async def test_continuous_reconciliation_pauses_on_first_transient_failure() -> None:
    repository = _FakeRepository([])
    risk = GlobalRiskController(10, 3)
    service = ReconciliationService(
        repository,  # type: ignore[arg-type]
        {"Predict.fun": _TransientContinuousClient(fail_after=0)},
        risk,
        orders_interval_seconds=0.01,
        transient_failure_pause_threshold=3,
    )

    await service.start()
    try:
        for _ in range(20):
            if risk.is_paused():
                break
            await asyncio.sleep(0.01)
    finally:
        await service.close()

    assert not service.ready
    assert risk.is_paused()
    assert risk.pause_reason == "continuous reconciliation transient failure"


@pytest.mark.asyncio
async def test_reconciliation_cycles_are_serialized() -> None:
    class BlockingClient(_FakeClient):
        def __init__(self) -> None:
            super().__init__()
            self.started = asyncio.Event()
            self.release = asyncio.Event()
            self.active_calls = 0
            self.max_active_calls = 0

        async def list_open_orders(self) -> list[VenueOrder]:
            self.active_calls += 1
            self.max_active_calls = max(self.max_active_calls, self.active_calls)
            self.started.set()
            try:
                await self.release.wait()
                return []
            finally:
                self.active_calls -= 1

    repository = _FakeRepository([])
    client = BlockingClient()
    service = ReconciliationService(
        repository,  # type: ignore[arg-type]
        {"Predict.fun": client},
        GlobalRiskController(10, 3),
    )

    first = asyncio.create_task(service.run_once(full=True))
    await asyncio.wait_for(client.started.wait(), timeout=1.0)
    second = asyncio.create_task(service.run_once(full=True))
    await asyncio.sleep(0)

    assert client.active_calls == 1
    assert client.max_active_calls == 1

    client.release.set()
    await asyncio.gather(first, second)

    assert client.max_active_calls == 1
    assert len(repository.reconciliations) == 2


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_http_throttle_and_transient_server_errors_are_reconciliation_transient(status: int) -> None:
    assert _is_transient_reconciliation_exception(_FakeHttpError(status))


@pytest.mark.parametrize("status", [400, 401, 403, 404])
def test_non_retryable_http_errors_are_not_reconciliation_transient(status: int) -> None:
    assert not _is_transient_reconciliation_exception(_FakeHttpError(status))


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
