import os
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import pytest

pytest.importorskip("sqlalchemy")

from arbitrage_engine.database import ProductionRepository
from arbitrage_engine.models import (
    BinarySide,
    FillRecord,
    OrderIntent,
    OrderIntentStatus,
    ReconciliationResult,
    RedemptionIntent,
    RedemptionIntentStatus,
)
from arbitrage_engine.risk import GlobalRiskController
from arbitrage_engine.utils.ids import uuid7


@pytest.fixture
async def repository() -> AsyncIterator[ProductionRepository]:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        if os.getenv("CI"):
            pytest.fail("DATABASE_URL must be configured in CI")
        pytest.skip("DATABASE_URL is required for PostgreSQL integration tests")
    repo = ProductionRepository(database_url)
    await repo.create_schema()
    yield repo
    await repo.close()


@pytest.mark.asyncio
async def test_order_intent_is_durable_before_status_transition(
    repository: ProductionRepository,
) -> None:
    client_order_id = str(uuid7())
    intent = OrderIntent(
        client_order_id=client_order_id,
        route="polymarket_myriad",
        market_key=f"integration:{client_order_id}",
        venue="Polymarket",
        token_id="integration-token",
        binary_side=BinarySide.YES,
        action="BUY",
        quantity=Decimal("1.000000000000000001"),
        limit_price=Decimal("0.123456789012345678"),
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )

    await repository.create_order_intent(intent)
    await repository.update_order_intent(client_order_id, OrderIntentStatus.UNKNOWN)

    unresolved = await repository.unresolved_order_intents()
    row = next(item for item in unresolved if item.client_order_id == client_order_id)
    assert row.status == OrderIntentStatus.UNKNOWN.value
    assert row.quantity == intent.quantity
    assert row.limit_price == intent.limit_price


@pytest.mark.asyncio
async def test_only_one_repository_can_hold_trader_lock(
    repository: ProductionRepository,
) -> None:
    contender = ProductionRepository(repository.engine.url.render_as_string(hide_password=False))
    try:
        assert await repository.acquire_trader_lock()
        assert not await contender.acquire_trader_lock()
        await repository.release_trader_lock()
        assert await contender.acquire_trader_lock()
    finally:
        await contender.close()


@pytest.mark.asyncio
async def test_global_risk_pause_survives_repository_restart(repository: ProductionRepository) -> None:
    clean_state = {
        "loss_day": datetime.now(UTC).date().isoformat(),
        "daily_loss_usd": Decimal(0),
        "consecutive_api_errors": 0,
        "paused": False,
        "pause_reason": None,
    }
    await repository.save_risk_state(clean_state)
    controller = GlobalRiskController(100, 3, state_store=repository)
    await controller.initialize()
    await controller.pause("integration restart pause")

    restarted = ProductionRepository(repository.engine.url.render_as_string(hide_password=False))
    try:
        restored = GlobalRiskController(100, 3, state_store=restarted)
        await restored.initialize()
        assert restored.is_paused()
        assert restored.pause_reason == "integration restart pause"
    finally:
        await restarted.save_risk_state(clean_state)
        await restarted.close()


@pytest.mark.asyncio
async def test_restart_recovery_and_duplicate_fill_are_idempotent(
    repository: ProductionRepository,
) -> None:
    client_order_id = str(uuid7())
    now = datetime.now(UTC)
    await repository.create_order_intent(
        OrderIntent(
            client_order_id=client_order_id,
            route="polymarket_myriad",
            market_key=f"restart:{client_order_id}",
            venue="Myriad",
            token_id="restart-token",
            binary_side=BinarySide.NO,
            action="BUY",
            quantity=Decimal("2.5"),
            limit_price=Decimal("0.4"),
            status=OrderIntentStatus.ACKNOWLEDGED,
            venue_order_id=f"venue-{client_order_id}",
            created_at=now,
            updated_at=now,
        )
    )
    fill = FillRecord(
        fill_id=f"fill-{client_order_id}",
        client_order_id=client_order_id,
        venue_order_id=f"venue-{client_order_id}",
        venue="Myriad",
        quantity=Decimal("1.25"),
        price=Decimal("0.4"),
        fee=Decimal("0"),
        occurred_at=now,
    )

    assert await repository.insert_fill(fill)
    assert not await repository.insert_fill(fill)

    restarted = ProductionRepository(repository.engine.url.render_as_string(hide_password=False))
    try:
        recovered = await restarted.unresolved_order_intents()
        row = next(item for item in recovered if item.client_order_id == client_order_id)
        assert row.status == OrderIntentStatus.ACKNOWLEDGED.value
        assert row.venue_order_id == fill.venue_order_id
    finally:
        await restarted.close()


@pytest.mark.asyncio
async def test_redemption_intent_is_unique_and_restart_safe(repository: ProductionRepository) -> None:
    suffix = str(uuid7())
    intent = RedemptionIntent(
        redemption_id=suffix,
        position_key=f"position:{suffix}",
        venue="Polymarket",
        market_id=f"market:{suffix}",
        condition_id=f"0x{uuid7().hex}{uuid7().hex}",
        collateral_token="0x" + "1" * 40,
        expected_contracts=Decimal("1.000000000000000001"),
    )
    assert await repository.create_redemption_intent(intent)
    assert not await repository.create_redemption_intent(replace(intent, redemption_id=str(uuid7())))
    await repository.update_redemption_intent(
        intent.redemption_id,
        RedemptionIntentStatus.SUBMITTED,
        tx_hash="0x" + "2" * 64,
    )

    restarted = ProductionRepository(repository.engine.url.render_as_string(hide_password=False))
    try:
        restored = await restarted.get_redemption_intent(intent.position_key, intent.venue, intent.condition_id)
        assert restored is not None
        assert restored.status is RedemptionIntentStatus.SUBMITTED
        assert restored.expected_contracts == intent.expected_contracts
    finally:
        await restarted.close()


@pytest.mark.asyncio
async def test_only_latest_reconciliation_result_blocks_risk_resume(
    repository: ProductionRepository,
) -> None:
    venue = f"test-{uuid7().hex[:16]}"
    now = datetime.now(UTC)
    await repository.record_reconciliation(
        ReconciliationResult(
            venue=venue,
            started_at=now,
            completed_at=now,
            orders_checked=1,
            fills_recorded=0,
            drift_count=1,
            success=True,
        )
    )
    assert any(item.startswith(f"{venue}:") for item in await repository.latest_reconciliation_failures())

    await repository.record_reconciliation(
        ReconciliationResult(
            venue=venue,
            started_at=now,
            completed_at=now,
            orders_checked=1,
            fills_recorded=1,
            drift_count=0,
            success=True,
        )
    )
    assert not any(item.startswith(f"{venue}:") for item in await repository.latest_reconciliation_failures())
